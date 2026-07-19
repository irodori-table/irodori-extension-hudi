//! Hudi copy-on-write table support.
//!
//! A raw `read_parquet('<path>/**/*.parquet')` over a Hudi table returns
//! superseded file slices next to current ones, silently duplicating and
//! resurrecting rows. This module reads the Hudi timeline (`.hoodie/`) to
//! resolve the latest file slice per file group, and refuses — with a clear
//! message — anything it cannot read correctly (merge-on-read tables,
//! non-JSON timeline formats, directories without Hudi metadata) instead of
//! returning wrong-but-plausible rows.

use std::collections::BTreeMap;

use serde_json::Value;

use crate::driver::sql_string;

/// One completed timeline instant (`*.commit` / `*.replacecommit`).
struct Instant {
    timestamp: String,
    is_replace: bool,
    content: Value,
}

/// Builds `view` over the latest copy-on-write file slices of the Hudi table
/// at `base_path`. Fails with actionable messages instead of guessing.
pub(crate) fn create_cow_view(
    conn: &duckdb::Connection,
    base_path: &str,
    view: &str,
) -> Result<(), String> {
    let base = base_path.trim_end_matches('/');

    let properties = read_properties(conn, base)?;
    match properties
        .get("hoodie.table.type")
        .map(String::as_str)
        .unwrap_or("COPY_ON_WRITE")
    {
        "COPY_ON_WRITE" => {}
        "MERGE_ON_READ" => {
            return Err(format!(
                "Hudi table at {base} is a Merge-on-Read table. Reading it requires merging \
                 log files with base files, which this connector does not support yet; \
                 scanning only the parquet base files would return stale rows. \
                 Query it through a Hudi-aware engine (Spark, Trino, Athena) instead."
            ))
        }
        other => {
            return Err(format!(
                "Hudi table at {base} has unsupported table type '{other}'."
            ))
        }
    }

    let instants = read_timeline(conn, base)?;
    if instants.is_empty() {
        return Err(format!(
            "Hudi table at {base} has no completed commits in its timeline, so it has no \
             readable data yet."
        ));
    }

    let files = latest_file_slices(&instants, base)?;
    if files.is_empty() {
        return Err(format!(
            "Hudi table at {base} has no live data files after applying the timeline \
             (all file groups were replaced or the commits carry no write stats)."
        ));
    }

    let file_list = files
        .iter()
        .map(|file| sql_string(file))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "create or replace view {view} as select * from read_parquet([{file_list}], \
         hive_partitioning=true, union_by_name=true)"
    );
    conn.execute_batch(&sql)
        .map_err(|err| format!("Hudi view creation failed: {err}"))
}

/// Reads `.hoodie/hoodie.properties` through DuckDB's `read_text`, which
/// works for local paths and (via httpfs) object storage alike.
fn read_properties(
    conn: &duckdb::Connection,
    base: &str,
) -> Result<BTreeMap<String, String>, String> {
    let pattern = format!("{base}/.hoodie/hoodie.properties");
    let files = read_text_files(conn, &pattern).map_err(|err| {
        format!(
            "No Hudi metadata found at {base}/.hoodie ({err}). If this location is a plain \
             Parquet directory rather than a Hudi table, set the connection option \
             rawParquet=true to scan it as-is."
        )
    })?;
    let Some((_, content)) = files.into_iter().next() else {
        return Err(format!(
            "No Hudi metadata found at {base}/.hoodie. If this location is a plain Parquet \
             directory rather than a Hudi table, set the connection option rawParquet=true \
             to scan it as-is."
        ));
    };
    Ok(parse_properties(&content))
}

fn parse_properties(content: &str) -> BTreeMap<String, String> {
    let mut properties = BTreeMap::new();
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with('!') {
            continue;
        }
        if let Some((key, value)) = line.split_once('=') {
            properties.insert(key.trim().to_string(), value.trim().to_string());
        }
    }
    properties
}

/// Collects completed commit instants from both timeline layouts:
/// `.hoodie/` (Hudi 0.x, table version <= 7) and `.hoodie/timeline/`
/// (Hudi 1.x, table version >= 8).
fn read_timeline(conn: &duckdb::Connection, base: &str) -> Result<Vec<Instant>, String> {
    let mut instants = Vec::new();
    for (glob, is_replace) in [
        (format!("{base}/.hoodie/*.commit"), false),
        (format!("{base}/.hoodie/*.replacecommit"), true),
        (format!("{base}/.hoodie/timeline/*.commit"), false),
        (format!("{base}/.hoodie/timeline/*.replacecommit"), true),
    ] {
        let files = match read_text_files(conn, &glob) {
            Ok(files) => files,
            Err(err) if is_no_files_error(&err) => Vec::new(),
            Err(err) => return Err(format!("failed to read Hudi timeline {glob}: {err}")),
        };
        for (filename, content) in files {
            let timestamp = instant_timestamp(&filename);
            if timestamp.is_empty() {
                continue;
            }
            let trimmed = content.trim();
            if trimmed.is_empty() {
                // Completed instants with no payload carry no write stats.
                continue;
            }
            let content: Value = serde_json::from_str(trimmed).map_err(|_| {
                format!(
                    "Hudi timeline file {filename} is not JSON commit metadata. This table \
                     was probably written by a Hudi version whose timeline format this \
                     connector does not understand; refusing to scan it because the result \
                     could silently include superseded rows."
                )
            })?;
            instants.push(Instant {
                timestamp,
                is_replace,
                content,
            });
        }
    }
    instants.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
    Ok(instants)
}

/// `20240101093000123.commit` -> `20240101093000123`;
/// Hudi 1.x completed files are named `<requested>_<completed>.commit`.
fn instant_timestamp(filename: &str) -> String {
    let name = filename.rsplit(['/', '\\']).next().unwrap_or(filename);
    let stem = name.split('.').next().unwrap_or("");
    stem.split('_').next().unwrap_or("").to_string()
}

/// Resolves the latest file slice per file group and drops replaced groups.
fn latest_file_slices(instants: &[Instant], base: &str) -> Result<Vec<String>, String> {
    // (partition, fileId) -> (instant timestamp, relative path)
    let mut slices: BTreeMap<(String, String), (String, String)> = BTreeMap::new();
    // (partition, fileId) -> timestamp of the replacecommit that dropped it
    let mut replaced: BTreeMap<(String, String), String> = BTreeMap::new();

    for instant in instants {
        if let Some(partitions) = instant
            .content
            .get("partitionToWriteStats")
            .and_then(Value::as_object)
        {
            for (partition, stats) in partitions {
                let Some(stats) = stats.as_array() else {
                    continue;
                };
                for stat in stats {
                    let Some(file_id) = stat.get("fileId").and_then(Value::as_str) else {
                        continue;
                    };
                    let Some(path) = stat.get("path").and_then(Value::as_str) else {
                        continue;
                    };
                    if file_id.is_empty() || path.is_empty() {
                        continue;
                    }
                    let key = (partition.clone(), file_id.to_string());
                    let candidate = (instant.timestamp.clone(), path.to_string());
                    match slices.get(&key) {
                        Some((existing, _)) if existing.as_str() > instant.timestamp.as_str() => {}
                        _ => {
                            slices.insert(key, candidate);
                        }
                    }
                }
            }
        }
        if instant.is_replace {
            if let Some(partitions) = instant
                .content
                .get("partitionToReplaceFileIds")
                .and_then(Value::as_object)
            {
                for (partition, ids) in partitions {
                    let Some(ids) = ids.as_array() else { continue };
                    for id in ids.iter().filter_map(Value::as_str) {
                        let key = (partition.clone(), id.to_string());
                        match replaced.get(&key) {
                            Some(existing) if existing.as_str() > instant.timestamp.as_str() => {}
                            _ => {
                                replaced.insert(key, instant.timestamp.clone());
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(slices
        .into_iter()
        .filter(|(key, (timestamp, _))| {
            replaced
                .get(key)
                .is_none_or(|replaced_at| replaced_at.as_str() < timestamp.as_str())
        })
        .map(|(_, (_, path))| format!("{base}/{}", path.trim_start_matches('/')))
        .collect())
}

fn is_no_files_error(message: &str) -> bool {
    let lowered = message.to_ascii_lowercase();
    lowered.contains("no files found") || lowered.contains("no such file")
}

fn read_text_files(
    conn: &duckdb::Connection,
    pattern: &str,
) -> Result<Vec<(String, String)>, String> {
    let sql = format!(
        "select filename, content from read_text({}) order by filename",
        sql_string(pattern)
    );
    let mut stmt = conn.prepare(&sql).map_err(|err| err.to_string())?;
    let rows = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(|err| err.to_string())?;
    let mut files = Vec::new();
    for row in rows {
        files.push(row.map_err(|err| err.to_string())?);
    }
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_properties_lines() {
        let properties = parse_properties(
            "#comment\nhoodie.table.name=demo\n hoodie.table.type = COPY_ON_WRITE \n\n!x\n",
        );
        assert_eq!(properties.get("hoodie.table.name").unwrap(), "demo");
        assert_eq!(
            properties.get("hoodie.table.type").unwrap(),
            "COPY_ON_WRITE"
        );
    }

    #[test]
    fn extracts_instant_timestamps_from_both_layouts() {
        assert_eq!(instant_timestamp("/t/.hoodie/001.commit"), "001");
        assert_eq!(
            instant_timestamp("/t/.hoodie/timeline/001_002.commit"),
            "001"
        );
    }
}

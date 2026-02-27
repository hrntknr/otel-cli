/// Convert old CLI flags to SQL query strings.

pub fn trace_flags_to_sql(
    service: Option<&str>,
    trace_id: Option<&str>,
    attributes: &[(String, String)],
    limit: Option<usize>,
    start_time_ns: Option<u64>,
    end_time_ns: Option<u64>,
) -> String {
    let mut conditions = Vec::new();

    if let Some(svc) = service {
        conditions.push(format!("service_name = '{}'", escape_sql_string(svc)));
    }
    if let Some(tid) = trace_id {
        conditions.push(format!("trace_id = '{}'", escape_sql_string(tid)));
    }
    for (key, value) in attributes {
        conditions.push(format!(
            "attributes['{}'] = '{}'",
            escape_sql_string(key),
            escape_sql_string(value)
        ));
    }
    if let Some(start) = start_time_ns {
        conditions.push(format!("start_time >= {}", start));
    }
    if let Some(end) = end_time_ns {
        conditions.push(format!("start_time <= {}", end));
    }

    build_sql("traces", &conditions, limit)
}

pub fn log_flags_to_sql(
    service: Option<&str>,
    severity: Option<&str>,
    attributes: &[(String, String)],
    limit: Option<usize>,
    start_time_ns: Option<u64>,
    end_time_ns: Option<u64>,
) -> String {
    let mut conditions = Vec::new();

    if let Some(svc) = service {
        conditions.push(format!("service_name = '{}'", escape_sql_string(svc)));
    }
    if let Some(sev) = severity {
        conditions.push(format!("severity >= '{}'", escape_sql_string(sev)));
    }
    for (key, value) in attributes {
        conditions.push(format!(
            "attributes['{}'] = '{}'",
            escape_sql_string(key),
            escape_sql_string(value)
        ));
    }
    if let Some(start) = start_time_ns {
        conditions.push(format!("timestamp >= {}", start));
    }
    if let Some(end) = end_time_ns {
        conditions.push(format!("timestamp <= {}", end));
    }

    build_sql("logs", &conditions, limit)
}

pub fn metric_flags_to_sql(
    service: Option<&str>,
    name: Option<&str>,
    limit: Option<usize>,
    start_time_ns: Option<u64>,
    end_time_ns: Option<u64>,
) -> String {
    let mut conditions = Vec::new();

    if let Some(svc) = service {
        conditions.push(format!("service_name = '{}'", escape_sql_string(svc)));
    }
    if let Some(n) = name {
        conditions.push(format!("metric_name = '{}'", escape_sql_string(n)));
    }
    if let Some(start) = start_time_ns {
        conditions.push(format!("timestamp >= {}", start));
    }
    if let Some(end) = end_time_ns {
        conditions.push(format!("timestamp <= {}", end));
    }

    build_sql("metrics", &conditions, limit)
}

fn build_sql(table: &str, conditions: &[String], limit: Option<usize>) -> String {
    let mut sql = format!("SELECT * FROM {}", table);
    if !conditions.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conditions.join(" AND "));
    }
    if let Some(limit) = limit {
        sql.push_str(&format!(" LIMIT {}", limit));
    }
    sql
}

fn escape_sql_string(s: &str) -> String {
    s.replace('\'', "''")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trace_service_only() {
        let sql = trace_flags_to_sql(Some("myapp"), None, &[], None, None, None);
        assert_eq!(sql, "SELECT * FROM traces WHERE service_name = 'myapp'");
    }

    #[test]
    fn trace_service_with_limit() {
        let sql = trace_flags_to_sql(Some("myapp"), None, &[], Some(100), None, None);
        assert_eq!(
            sql,
            "SELECT * FROM traces WHERE service_name = 'myapp' LIMIT 100"
        );
    }

    #[test]
    fn trace_with_trace_id() {
        let sql = trace_flags_to_sql(None, Some("abc123"), &[], None, None, None);
        assert_eq!(sql, "SELECT * FROM traces WHERE trace_id = 'abc123'");
    }

    #[test]
    fn trace_with_attributes() {
        let sql = trace_flags_to_sql(
            None,
            None,
            &[
                ("env".to_string(), "prod".to_string()),
                ("region".to_string(), "us".to_string()),
            ],
            None,
            None,
            None,
        );
        assert_eq!(
            sql,
            "SELECT * FROM traces WHERE attributes['env'] = 'prod' AND attributes['region'] = 'us'"
        );
    }

    #[test]
    fn trace_with_time_range() {
        let sql = trace_flags_to_sql(None, None, &[], None, Some(1000), Some(2000));
        assert_eq!(
            sql,
            "SELECT * FROM traces WHERE start_time >= 1000 AND start_time <= 2000"
        );
    }

    #[test]
    fn trace_no_flags() {
        let sql = trace_flags_to_sql(None, None, &[], None, None, None);
        assert_eq!(sql, "SELECT * FROM traces");
    }

    #[test]
    fn log_severity_only() {
        let sql = log_flags_to_sql(None, Some("ERROR"), &[], None, None, None);
        assert_eq!(sql, "SELECT * FROM logs WHERE severity >= 'ERROR'");
    }

    #[test]
    fn log_service_and_severity() {
        let sql = log_flags_to_sql(Some("myapp"), Some("WARN"), &[], None, None, None);
        assert_eq!(
            sql,
            "SELECT * FROM logs WHERE service_name = 'myapp' AND severity >= 'WARN'"
        );
    }

    #[test]
    fn log_with_attributes() {
        let sql = log_flags_to_sql(
            None,
            None,
            &[("env".to_string(), "prod".to_string())],
            Some(50),
            None,
            None,
        );
        assert_eq!(
            sql,
            "SELECT * FROM logs WHERE attributes['env'] = 'prod' LIMIT 50"
        );
    }

    #[test]
    fn log_no_flags() {
        let sql = log_flags_to_sql(None, None, &[], None, None, None);
        assert_eq!(sql, "SELECT * FROM logs");
    }

    #[test]
    fn metric_service_only() {
        let sql = metric_flags_to_sql(Some("myapp"), None, None, None, None);
        assert_eq!(sql, "SELECT * FROM metrics WHERE service_name = 'myapp'");
    }

    #[test]
    fn metric_name_only() {
        let sql = metric_flags_to_sql(None, Some("http.duration"), None, None, None);
        assert_eq!(
            sql,
            "SELECT * FROM metrics WHERE metric_name = 'http.duration'"
        );
    }

    #[test]
    fn metric_with_limit_and_time() {
        let sql = metric_flags_to_sql(None, None, Some(100), Some(1000), Some(2000));
        assert_eq!(
            sql,
            "SELECT * FROM metrics WHERE timestamp >= 1000 AND timestamp <= 2000 LIMIT 100"
        );
    }

    #[test]
    fn metric_no_flags() {
        let sql = metric_flags_to_sql(None, None, None, None, None);
        assert_eq!(sql, "SELECT * FROM metrics");
    }

    #[test]
    fn trace_combined_all_flags() {
        let sql = trace_flags_to_sql(
            Some("myapp"),
            Some("abc"),
            &[("env".to_string(), "prod".to_string())],
            Some(10),
            Some(1000),
            Some(2000),
        );
        assert_eq!(
            sql,
            "SELECT * FROM traces WHERE service_name = 'myapp' AND trace_id = 'abc' AND attributes['env'] = 'prod' AND start_time >= 1000 AND start_time <= 2000 LIMIT 10"
        );
    }

    #[test]
    fn escape_single_quotes() {
        let sql = trace_flags_to_sql(Some("my'app"), None, &[], None, None, None);
        assert_eq!(sql, "SELECT * FROM traces WHERE service_name = 'my''app'");
    }
}

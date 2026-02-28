use sqlparser::ast::{
    BinaryOperator, Expr, OrderByExpr, SelectItem, SetExpr, Statement, Subscript, UnaryOperator,
    Value,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::query::TargetTable;

#[derive(Debug, Clone, PartialEq)]
pub enum WhereExpr {
    Comparison {
        column: ColumnRef,
        op: CompOp,
        value: SqlValue,
    },
    Like {
        column: ColumnRef,
        pattern: String,
        negated: bool,
    },
    RegexMatch {
        column: ColumnRef,
        pattern: String,
        negated: bool,
    },
    InList {
        column: ColumnRef,
        values: Vec<SqlValue>,
        negated: bool,
    },
    IsNull {
        column: ColumnRef,
        negated: bool,
    },
    And(Box<WhereExpr>, Box<WhereExpr>),
    Or(Box<WhereExpr>, Box<WhereExpr>),
    Not(Box<WhereExpr>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnRef {
    Named(String),
    BracketAccess(String, String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum CompOp {
    Eq,
    NotEq,
    Lt,
    Gt,
    LtEq,
    GtEq,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    String(String),
    Number(f64),
    Boolean(bool),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Projection {
    All,
    Columns(Vec<ColumnRef>),
}

#[derive(Debug, Clone)]
pub struct SqlQuery {
    pub table: TargetTable,
    pub where_expr: Option<WhereExpr>,
    pub limit: Option<usize>,
    pub order_by: Vec<OrderByItem>,
    pub projection: Projection,
}

#[derive(Debug, Clone)]
pub struct OrderByItem {
    pub column: String,
    pub desc: bool,
}

pub fn parse(sql: &str) -> Result<SqlQuery, String> {
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql).map_err(|e| e.to_string())?;

    if statements.len() != 1 {
        return Err("expected exactly one SQL statement".to_string());
    }

    let statement = &statements[0];
    let Statement::Query(query) = statement else {
        return Err("expected a SELECT statement".to_string());
    };

    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err("expected a SELECT expression".to_string());
    };

    // Extract SELECT projection
    let projection = if select.projection.len() == 1
        && matches!(select.projection[0], SelectItem::Wildcard(_))
    {
        Projection::All
    } else {
        let mut cols = Vec::new();
        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    cols.push(extract_column_ref(expr)?);
                }
                SelectItem::Wildcard(_) => {
                    return Err("wildcard (*) must be the only select item".to_string());
                }
                _ => {
                    return Err(format!("unsupported select item: {:?}", item));
                }
            }
        }
        Projection::Columns(cols)
    };

    // Extract FROM clause → TargetTable
    if select.from.len() != 1 {
        return Err("expected exactly one table in FROM clause".to_string());
    }
    let table_name = table_factor_name(&select.from[0].relation)?;
    let table = match table_name.to_lowercase().as_str() {
        "traces" => TargetTable::Traces,
        "logs" => TargetTable::Logs,
        "metrics" => TargetTable::Metrics,
        _ => return Err(format!("unknown table: {}", table_name)),
    };

    // Extract WHERE clause
    let where_expr = match &select.selection {
        Some(expr) => Some(convert_expr(expr)?),
        None => None,
    };

    // Extract LIMIT
    let limit = match &query.limit {
        Some(expr) => Some(extract_limit(expr)?),
        None => None,
    };

    // Extract ORDER BY
    let order_by = match &query.order_by {
        Some(ob) => convert_order_by(ob)?,
        None => vec![],
    };

    Ok(SqlQuery {
        table,
        where_expr,
        limit,
        order_by,
        projection,
    })
}

fn table_factor_name(tf: &sqlparser::ast::TableFactor) -> Result<String, String> {
    match tf {
        sqlparser::ast::TableFactor::Table { name, .. } => Ok(name.to_string()),
        _ => Err("unsupported table factor".to_string()),
    }
}

fn extract_limit(expr: &Expr) -> Result<usize, String> {
    match expr {
        Expr::Value(v) => match v {
            Value::Number(n, _) => n
                .parse::<usize>()
                .map_err(|_| format!("invalid LIMIT value: {}", n)),
            _ => Err("LIMIT must be a number".to_string()),
        },
        _ => Err("LIMIT must be a simple value".to_string()),
    }
}

fn convert_order_by(order_by: &sqlparser::ast::OrderBy) -> Result<Vec<OrderByItem>, String> {
    let mut items = Vec::new();
    for ob in &order_by.exprs {
        let OrderByExpr { expr, asc, .. } = ob;
        let column = match expr {
            Expr::Identifier(ident) => ident.value.clone(),
            _ => return Err("ORDER BY must reference a column name".to_string()),
        };
        let desc = asc.map(|a| !a).unwrap_or(false);
        items.push(OrderByItem { column, desc });
    }
    Ok(items)
}

fn convert_expr(expr: &Expr) -> Result<WhereExpr, String> {
    match expr {
        Expr::BinaryOp { left, op, right } => convert_binary_op(left, op, right),
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: inner,
        } => Ok(WhereExpr::Not(Box::new(convert_expr(inner)?))),
        Expr::Nested(inner) => convert_expr(inner),
        Expr::Like {
            negated,
            expr: col_expr,
            pattern,
            ..
        } => {
            let column = extract_column_ref(col_expr)?;
            let pat = extract_string_value(pattern)?;
            Ok(WhereExpr::Like {
                column,
                pattern: pat,
                negated: *negated,
            })
        }
        Expr::InList {
            expr: col_expr,
            list,
            negated,
        } => {
            let column = extract_column_ref(col_expr)?;
            let values = list
                .iter()
                .map(extract_sql_value)
                .collect::<Result<_, _>>()?;
            Ok(WhereExpr::InList {
                column,
                values,
                negated: *negated,
            })
        }
        Expr::IsNull(inner) => {
            let column = extract_column_ref(inner)?;
            Ok(WhereExpr::IsNull {
                column,
                negated: false,
            })
        }
        Expr::IsNotNull(inner) => {
            let column = extract_column_ref(inner)?;
            Ok(WhereExpr::IsNull {
                column,
                negated: true,
            })
        }
        _ => Err(format!("unsupported expression: {:?}", expr)),
    }
}

fn convert_binary_op(left: &Expr, op: &BinaryOperator, right: &Expr) -> Result<WhereExpr, String> {
    match op {
        BinaryOperator::And => {
            let l = convert_expr(left)?;
            let r = convert_expr(right)?;
            Ok(WhereExpr::And(Box::new(l), Box::new(r)))
        }
        BinaryOperator::Or => {
            let l = convert_expr(left)?;
            let r = convert_expr(right)?;
            Ok(WhereExpr::Or(Box::new(l), Box::new(r)))
        }
        BinaryOperator::Eq
        | BinaryOperator::NotEq
        | BinaryOperator::Lt
        | BinaryOperator::Gt
        | BinaryOperator::LtEq
        | BinaryOperator::GtEq => {
            let column = extract_column_ref(left)?;
            let value = extract_sql_value(right)?;
            let comp_op = match op {
                BinaryOperator::Eq => CompOp::Eq,
                BinaryOperator::NotEq => CompOp::NotEq,
                BinaryOperator::Lt => CompOp::Lt,
                BinaryOperator::Gt => CompOp::Gt,
                BinaryOperator::LtEq => CompOp::LtEq,
                BinaryOperator::GtEq => CompOp::GtEq,
                _ => unreachable!(),
            };
            Ok(WhereExpr::Comparison {
                column,
                op: comp_op,
                value,
            })
        }
        // ~ operator for regex match
        BinaryOperator::PGRegexMatch => {
            let column = extract_column_ref(left)?;
            let pattern = extract_string_value(right)?;
            Ok(WhereExpr::RegexMatch {
                column,
                pattern,
                negated: false,
            })
        }
        BinaryOperator::PGRegexNotMatch => {
            let column = extract_column_ref(left)?;
            let pattern = extract_string_value(right)?;
            Ok(WhereExpr::RegexMatch {
                column,
                pattern,
                negated: true,
            })
        }
        _ => Err(format!("unsupported operator: {:?}", op)),
    }
}

fn extract_column_ref(expr: &Expr) -> Result<ColumnRef, String> {
    match expr {
        Expr::Identifier(ident) => Ok(ColumnRef::Named(ident.value.clone())),
        // Handle attributes['key'] / resource['key'] — sqlparser parses as Subscript
        Expr::Subscript { expr, subscript } => {
            let base = match expr.as_ref() {
                Expr::Identifier(ident) => ident.value.clone(),
                _ => return Err(format!("unsupported subscript base: {:?}", expr)),
            };
            let key = match subscript.as_ref() {
                Subscript::Index { index } => match index {
                    Expr::Value(v) => match v {
                        Value::SingleQuotedString(s) => s.clone(),
                        Value::DoubleQuotedString(s) => s.clone(),
                        _ => {
                            return Err(format!("subscript key must be a string: {:?}", v));
                        }
                    },
                    // GenericDialect parses double-quoted strings as identifiers
                    Expr::Identifier(ident) => ident.value.clone(),
                    _ => {
                        return Err(format!(
                            "unsupported subscript index expression: {:?}",
                            index
                        ));
                    }
                },
                _ => {
                    return Err(format!("unsupported subscript expression: {:?}", subscript));
                }
            };
            Ok(ColumnRef::BracketAccess(base, key))
        }
        // Handle compound identifiers like attributes.key
        Expr::CompoundIdentifier(parts) => {
            if parts.len() == 2 {
                Ok(ColumnRef::BracketAccess(
                    parts[0].value.clone(),
                    parts[1].value.clone(),
                ))
            } else {
                Err(format!("unsupported compound identifier: {:?}", parts))
            }
        }
        _ => Err(format!("expected column reference, got: {:?}", expr)),
    }
}

fn extract_sql_value(expr: &Expr) -> Result<SqlValue, String> {
    match expr {
        Expr::Value(v) => match v {
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
                Ok(SqlValue::String(s.clone()))
            }
            Value::Number(n, _) => {
                let f = n
                    .parse::<f64>()
                    .map_err(|_| format!("invalid number: {}", n))?;
                Ok(SqlValue::Number(f))
            }
            Value::Boolean(b) => Ok(SqlValue::Boolean(*b)),
            _ => Err(format!("unsupported value: {:?}", v)),
        },
        // GenericDialect treats double-quoted strings as identifiers;
        // treat them as string literals for user convenience.
        Expr::Identifier(ident) => Ok(SqlValue::String(ident.value.clone())),
        _ => Err(format!("expected a literal value, got: {:?}", expr)),
    }
}

fn extract_string_value(expr: &Expr) -> Result<String, String> {
    match extract_sql_value(expr)? {
        SqlValue::String(s) => Ok(s),
        other => Err(format!("expected string value, got: {:?}", other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_select_all_from_traces() {
        let q = parse("SELECT * FROM traces").unwrap();
        assert!(matches!(q.table, TargetTable::Traces));
        assert_eq!(q.projection, Projection::All);
        assert!(q.where_expr.is_none());
        assert!(q.limit.is_none());
        assert!(q.order_by.is_empty());
    }

    #[test]
    fn parse_select_specific_columns() {
        let q = parse("SELECT timestamp, resource FROM logs").unwrap();
        assert!(matches!(q.table, TargetTable::Logs));
        assert_eq!(
            q.projection,
            Projection::Columns(vec![
                ColumnRef::Named("timestamp".to_string()),
                ColumnRef::Named("resource".to_string()),
            ])
        );
    }

    #[test]
    fn parse_select_bracket_access_column() {
        let q = parse("SELECT resource['service.name'], attributes['http.method'] FROM traces")
            .unwrap();
        assert_eq!(
            q.projection,
            Projection::Columns(vec![
                ColumnRef::BracketAccess("resource".to_string(), "service.name".to_string()),
                ColumnRef::BracketAccess("attributes".to_string(), "http.method".to_string()),
            ])
        );
    }

    #[test]
    fn parse_select_from_logs() {
        let q = parse("SELECT * FROM logs").unwrap();
        assert!(matches!(q.table, TargetTable::Logs));
    }

    #[test]
    fn parse_select_from_metrics() {
        let q = parse("SELECT * FROM metrics").unwrap();
        assert!(matches!(q.table, TargetTable::Metrics));
    }

    #[test]
    fn parse_where_eq() {
        let q = parse("SELECT * FROM traces WHERE service_name = 'myapp'").unwrap();
        let expr = q.where_expr.unwrap();
        assert_eq!(
            expr,
            WhereExpr::Comparison {
                column: ColumnRef::Named("service_name".to_string()),
                op: CompOp::Eq,
                value: SqlValue::String("myapp".to_string()),
            }
        );
    }

    #[test]
    fn parse_where_not_eq() {
        let q = parse("SELECT * FROM traces WHERE service_name != 'myapp'").unwrap();
        let expr = q.where_expr.unwrap();
        assert_eq!(
            expr,
            WhereExpr::Comparison {
                column: ColumnRef::Named("service_name".to_string()),
                op: CompOp::NotEq,
                value: SqlValue::String("myapp".to_string()),
            }
        );
    }

    #[test]
    fn parse_where_lt_gt() {
        let q = parse("SELECT * FROM traces WHERE duration_ns > 1000000").unwrap();
        let expr = q.where_expr.unwrap();
        assert_eq!(
            expr,
            WhereExpr::Comparison {
                column: ColumnRef::Named("duration_ns".to_string()),
                op: CompOp::Gt,
                value: SqlValue::Number(1000000.0),
            }
        );
    }

    #[test]
    fn parse_where_and() {
        let q = parse("SELECT * FROM traces WHERE service_name = 'myapp' AND duration_ns > 1000")
            .unwrap();
        let expr = q.where_expr.unwrap();
        match expr {
            WhereExpr::And(l, r) => {
                assert!(matches!(*l, WhereExpr::Comparison { .. }));
                assert!(matches!(*r, WhereExpr::Comparison { .. }));
            }
            _ => panic!("expected And"),
        }
    }

    #[test]
    fn parse_where_or() {
        let q = parse("SELECT * FROM logs WHERE severity = 'ERROR' OR severity = 'FATAL'").unwrap();
        let expr = q.where_expr.unwrap();
        assert!(matches!(expr, WhereExpr::Or(_, _)));
    }

    #[test]
    fn parse_where_like() {
        let q = parse("SELECT * FROM traces WHERE span_name LIKE '%http%'").unwrap();
        let expr = q.where_expr.unwrap();
        assert_eq!(
            expr,
            WhereExpr::Like {
                column: ColumnRef::Named("span_name".to_string()),
                pattern: "%http%".to_string(),
                negated: false,
            }
        );
    }

    #[test]
    fn parse_where_not_like() {
        let q = parse("SELECT * FROM traces WHERE span_name NOT LIKE '%health%'").unwrap();
        let expr = q.where_expr.unwrap();
        assert_eq!(
            expr,
            WhereExpr::Like {
                column: ColumnRef::Named("span_name".to_string()),
                pattern: "%health%".to_string(),
                negated: true,
            }
        );
    }

    #[test]
    fn parse_where_in_list() {
        let q = parse("SELECT * FROM traces WHERE service_name IN ('svc-a', 'svc-b')").unwrap();
        let expr = q.where_expr.unwrap();
        assert_eq!(
            expr,
            WhereExpr::InList {
                column: ColumnRef::Named("service_name".to_string()),
                values: vec![
                    SqlValue::String("svc-a".to_string()),
                    SqlValue::String("svc-b".to_string()),
                ],
                negated: false,
            }
        );
    }

    #[test]
    fn parse_where_is_null() {
        let q = parse("SELECT * FROM traces WHERE parent_span_id IS NULL").unwrap();
        let expr = q.where_expr.unwrap();
        assert_eq!(
            expr,
            WhereExpr::IsNull {
                column: ColumnRef::Named("parent_span_id".to_string()),
                negated: false,
            }
        );
    }

    #[test]
    fn parse_where_is_not_null() {
        let q = parse("SELECT * FROM traces WHERE parent_span_id IS NOT NULL").unwrap();
        let expr = q.where_expr.unwrap();
        assert_eq!(
            expr,
            WhereExpr::IsNull {
                column: ColumnRef::Named("parent_span_id".to_string()),
                negated: true,
            }
        );
    }

    #[test]
    fn parse_bracket_access() {
        let q = parse("SELECT * FROM traces WHERE attributes['http.method'] = 'GET'").unwrap();
        let expr = q.where_expr.unwrap();
        assert_eq!(
            expr,
            WhereExpr::Comparison {
                column: ColumnRef::BracketAccess(
                    "attributes".to_string(),
                    "http.method".to_string()
                ),
                op: CompOp::Eq,
                value: SqlValue::String("GET".to_string()),
            }
        );
    }

    #[test]
    fn parse_resource_bracket_access() {
        let q = parse("SELECT * FROM logs WHERE resource['service.name'] = 'myapp'").unwrap();
        let expr = q.where_expr.unwrap();
        assert_eq!(
            expr,
            WhereExpr::Comparison {
                column: ColumnRef::BracketAccess(
                    "resource".to_string(),
                    "service.name".to_string()
                ),
                op: CompOp::Eq,
                value: SqlValue::String("myapp".to_string()),
            }
        );
    }

    #[test]
    fn parse_limit() {
        let q = parse("SELECT * FROM traces LIMIT 100").unwrap();
        assert_eq!(q.limit, Some(100));
    }

    #[test]
    fn parse_order_by_asc() {
        let q = parse("SELECT * FROM traces ORDER BY start_time ASC").unwrap();
        assert_eq!(q.order_by.len(), 1);
        assert_eq!(q.order_by[0].column, "start_time");
        assert!(!q.order_by[0].desc);
    }

    #[test]
    fn parse_order_by_desc() {
        let q = parse("SELECT * FROM traces ORDER BY duration_ns DESC").unwrap();
        assert_eq!(q.order_by.len(), 1);
        assert_eq!(q.order_by[0].column, "duration_ns");
        assert!(q.order_by[0].desc);
    }

    #[test]
    fn parse_regex_match() {
        let q = parse("SELECT * FROM traces WHERE span_name ~ '^http.*'").unwrap();
        let expr = q.where_expr.unwrap();
        assert_eq!(
            expr,
            WhereExpr::RegexMatch {
                column: ColumnRef::Named("span_name".to_string()),
                pattern: "^http.*".to_string(),
                negated: false,
            }
        );
    }

    #[test]
    fn parse_regex_not_match() {
        let q = parse("SELECT * FROM traces WHERE span_name !~ '^health.*'").unwrap();
        let expr = q.where_expr.unwrap();
        assert_eq!(
            expr,
            WhereExpr::RegexMatch {
                column: ColumnRef::Named("span_name".to_string()),
                pattern: "^health.*".to_string(),
                negated: true,
            }
        );
    }

    #[test]
    fn parse_complex_where() {
        let q = parse(
            "SELECT * FROM traces WHERE (service_name = 'myapp' OR service_name = 'otherapp') AND duration_ns > 1000 LIMIT 50",
        )
        .unwrap();
        assert!(q.where_expr.is_some());
        assert_eq!(q.limit, Some(50));
    }

    #[test]
    fn parse_unknown_table_error() {
        let result = parse("SELECT * FROM unknown_table");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("unknown table"));
    }

    #[test]
    fn parse_not_select_error() {
        let result = parse("INSERT INTO traces VALUES (1)");
        assert!(result.is_err());
    }

    #[test]
    fn parse_where_not() {
        let q = parse("SELECT * FROM traces WHERE NOT service_name = 'myapp'").unwrap();
        let expr = q.where_expr.unwrap();
        match expr {
            WhereExpr::Not(inner) => {
                assert!(matches!(*inner, WhereExpr::Comparison { .. }));
            }
            _ => panic!("expected Not"),
        }
    }

    #[test]
    fn parse_double_quoted_string_as_value() {
        let q = parse("SELECT * FROM traces WHERE service_name = \"myapp\"").unwrap();
        let expr = q.where_expr.unwrap();
        assert_eq!(
            expr,
            WhereExpr::Comparison {
                column: ColumnRef::Named("service_name".to_string()),
                op: CompOp::Eq,
                value: SqlValue::String("myapp".to_string()),
            }
        );
    }

    #[test]
    fn parse_where_number_comparison() {
        let q = parse("SELECT * FROM traces WHERE kind = 2").unwrap();
        let expr = q.where_expr.unwrap();
        assert_eq!(
            expr,
            WhereExpr::Comparison {
                column: ColumnRef::Named("kind".to_string()),
                op: CompOp::Eq,
                value: SqlValue::Number(2.0),
            }
        );
    }

    #[test]
    fn parse_double_quoted_bracket_access() {
        let q =
            parse(r#"SELECT * FROM logs WHERE resource["service.name"] = "user-service""#).unwrap();
        let expr = q.where_expr.unwrap();
        assert_eq!(
            expr,
            WhereExpr::Comparison {
                column: ColumnRef::BracketAccess(
                    "resource".to_string(),
                    "service.name".to_string()
                ),
                op: CompOp::Eq,
                value: SqlValue::String("user-service".to_string()),
            }
        );
    }
}

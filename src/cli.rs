use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(
    name = "otel-cli",
    version,
    about = "OTLP Server/Viewer CLI",
    long_about = "An in-memory OpenTelemetry (OTLP) server with querying and interactive TUI.\n\n\
        Receive traces, logs, and metrics via standard OTLP protocols (gRPC/HTTP),\n\
        store them in-memory, and inspect them interactively.",
    after_long_help = "\
Getting started:
  $ otel-cli server                    Start server with interactive TUI
  $ otel-cli server --no-tui           Start headless server

Query data:
  $ otel-cli trace                     List recent traces
  $ otel-cli log --severity ERROR      Filter logs by severity
  $ otel-cli metrics -f                Follow metrics in real-time

Agent skill:
  $ otel-cli skill-install              Install skill for current project
  $ otel-cli skill-install --global    Install skill globally"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Start OTLP server with TUI viewer
    #[command(after_long_help = "\
Examples:
  $ otel-cli server                              Interactive TUI mode
  $ otel-cli server --no-tui                     Headless mode
  $ otel-cli server --grpc-addr 0.0.0.0:5317     Custom gRPC port
  $ otel-cli server --max-items 5000             Larger store capacity")]
    Server {
        /// gRPC listen address (OTLP collector)
        #[arg(long, default_value = "0.0.0.0:4317")]
        grpc_addr: String,
        /// HTTP listen address (OTLP collector)
        #[arg(long, default_value = "0.0.0.0:4318")]
        http_addr: String,
        /// Query API listen address
        #[arg(long, default_value = "0.0.0.0:4319")]
        query_addr: String,
        /// Maximum items to keep in store
        #[arg(long, default_value = "1000")]
        max_items: usize,
        /// Run without TUI (headless mode)
        #[arg(long)]
        no_tui: bool,
    },
    /// Query logs from server
    #[command(after_long_help = "\
Examples:
  $ otel-cli log                                 List recent logs
  $ otel-cli log --severity ERROR                Filter by severity
  $ otel-cli log --service myapp -f              Follow logs for a service
  $ otel-cli log --format json --since 10m       JSON output, last 10 minutes")]
    Log {
        /// Server address
        #[arg(long, default_value = "http://localhost:4319")]
        server: String,
        /// Filter by service name
        #[arg(long)]
        service: Option<String>,
        /// Filter by severity (shows logs at this level and above)
        #[arg(long)]
        severity: Option<String>,
        /// Filter by attributes (key=value)
        #[arg(long, value_parser = parse_key_val)]
        attribute: Vec<(String, String)>,
        /// Maximum number of results
        #[arg(long, default_value = "100")]
        limit: i32,
        /// Output format
        #[arg(long, default_value = "text")]
        format: OutputFormat,
        /// Follow new logs in real-time
        #[arg(short = 'f', long)]
        follow: bool,
        /// Show logs since (e.g. 30s, 5m, 1h, 2d, or RFC3339)
        #[arg(long)]
        since: Option<String>,
        /// Show logs until (e.g. 30s, 5m, 1h, 2d, or RFC3339)
        #[arg(long)]
        until: Option<String>,
    },
    /// Query traces from server
    #[command(after_long_help = "\
Examples:
  $ otel-cli trace                               List recent traces
  $ otel-cli trace --trace-id abc123             Look up a specific trace
  $ otel-cli trace --service myapp -f            Follow traces for a service
  $ otel-cli trace -f --full                     Follow with full trace groups")]
    Trace {
        /// Server address
        #[arg(long, default_value = "http://localhost:4319")]
        server: String,
        /// Filter by service name
        #[arg(long)]
        service: Option<String>,
        /// Filter by trace ID
        #[arg(long)]
        trace_id: Option<String>,
        /// Filter by attributes (key=value)
        #[arg(long, value_parser = parse_key_val)]
        attribute: Vec<(String, String)>,
        /// Maximum number of results
        #[arg(long, default_value = "100")]
        limit: i32,
        /// Output format
        #[arg(long, default_value = "text")]
        format: OutputFormat,
        /// Follow new traces in real-time
        #[arg(short = 'f', long)]
        follow: bool,
        /// Show full trace groups instead of only new spans in follow mode
        #[arg(long)]
        full: bool,
        /// Show traces since (e.g. 30s, 5m, 1h, 2d, or RFC3339)
        #[arg(long)]
        since: Option<String>,
        /// Show traces until (e.g. 30s, 5m, 1h, 2d, or RFC3339)
        #[arg(long)]
        until: Option<String>,
    },
    /// Clear stored data on server
    #[command(after_long_help = "\
Examples:
  $ otel-cli clear --traces --logs --metrics     Clear all data
  $ otel-cli clear --traces                      Clear only traces
  $ otel-cli clear --logs                        Clear only logs")]
    Clear {
        /// Server address
        #[arg(long, default_value = "http://localhost:4319")]
        server: String,
        /// Clear traces
        #[arg(long)]
        traces: bool,
        /// Clear logs
        #[arg(long)]
        logs: bool,
        /// Clear metrics
        #[arg(long)]
        metrics: bool,
    },
    /// Attach to a running server and display TUI
    #[command(after_long_help = "\
Examples:
  $ otel-cli view                                Attach to default server
  $ otel-cli view --server http://remote:4319    Attach to remote server
  $ otel-cli view --max-items 500                Custom local store capacity")]
    View {
        /// Query API server address
        #[arg(long, default_value = "http://localhost:4319")]
        server: String,
        /// Maximum items to keep in local store
        #[arg(long, default_value = "1000")]
        max_items: usize,
    },
    /// Query metrics from server
    #[command(after_long_help = "\
Examples:
  $ otel-cli metrics                             List recent metrics
  $ otel-cli metrics --name http_requests_total  Filter by metric name
  $ otel-cli metrics --service myapp -f          Follow metrics for a service
  $ otel-cli metrics --format json               JSON output")]
    Metrics {
        /// Server address
        #[arg(long, default_value = "http://localhost:4319")]
        server: String,
        /// Filter by service name
        #[arg(long)]
        service: Option<String>,
        /// Filter by metric name
        #[arg(long)]
        name: Option<String>,
        /// Maximum number of results
        #[arg(long, default_value = "100")]
        limit: i32,
        /// Output format
        #[arg(long, default_value = "text")]
        format: OutputFormat,
        /// Follow new metrics in real-time
        #[arg(short = 'f', long)]
        follow: bool,
        /// Show metrics since (e.g. 30s, 5m, 1h, 2d, or RFC3339)
        #[arg(long)]
        since: Option<String>,
        /// Show metrics until (e.g. 30s, 5m, 1h, 2d, or RFC3339)
        #[arg(long)]
        until: Option<String>,
    },
    /// Install agent skill for AI-assisted operation
    #[command(after_long_help = "\
Examples:
  $ otel-cli skill-install                       Install for current project
  $ otel-cli skill-install --global              Install globally (~/.claude/skills/)
  $ otel-cli skill-install --force               Overwrite existing installation")]
    SkillInstall {
        /// Install to ~/.claude/skills/ (available in all projects)
        #[arg(long)]
        global: bool,
        /// Force overwrite existing installation
        #[arg(long)]
        force: bool,
    },
}

#[derive(clap::ValueEnum, Clone, Debug)]
pub enum OutputFormat {
    Text,
    Json,
    Toon,
}

fn parse_key_val(s: &str) -> Result<(String, String), String> {
    let pos = s
        .find('=')
        .ok_or_else(|| format!("invalid KEY=VALUE: no `=` found in `{s}`"))?;
    Ok((s[..pos].to_string(), s[pos + 1..].to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn server_subcommand_parses_with_defaults() {
        let cli = Cli::parse_from(["otel-cli", "server"]);
        match cli.command {
            Commands::Server {
                grpc_addr,
                http_addr,
                query_addr,
                max_items,
                no_tui,
            } => {
                assert_eq!(grpc_addr, "0.0.0.0:4317");
                assert_eq!(http_addr, "0.0.0.0:4318");
                assert_eq!(query_addr, "0.0.0.0:4319");
                assert_eq!(max_items, 1000);
                assert!(!no_tui);
            }
            _ => panic!("Expected Server command"),
        }
    }

    #[test]
    fn server_subcommand_parses_with_custom_args() {
        let cli = Cli::parse_from([
            "otel-cli",
            "server",
            "--grpc-addr",
            "127.0.0.1:5317",
            "--http-addr",
            "127.0.0.1:5318",
            "--query-addr",
            "127.0.0.1:5319",
            "--max-items",
            "5000",
            "--no-tui",
        ]);
        match cli.command {
            Commands::Server {
                grpc_addr,
                http_addr,
                query_addr,
                max_items,
                no_tui,
            } => {
                assert_eq!(grpc_addr, "127.0.0.1:5317");
                assert_eq!(http_addr, "127.0.0.1:5318");
                assert_eq!(query_addr, "127.0.0.1:5319");
                assert_eq!(max_items, 5000);
                assert!(no_tui);
            }
            _ => panic!("Expected Server command"),
        }
    }

    #[test]
    fn log_subcommand_parses_with_filters() {
        let cli = Cli::parse_from([
            "otel-cli",
            "log",
            "--service",
            "my-service",
            "--severity",
            "ERROR",
            "--attribute",
            "env=production",
            "--limit",
            "50",
            "--format",
            "json",
        ]);
        match cli.command {
            Commands::Log {
                server,
                service,
                severity,
                attribute,
                limit,
                format,
                follow,
                since,
                until,
            } => {
                assert_eq!(server, "http://localhost:4319");
                assert_eq!(service, Some("my-service".to_string()));
                assert_eq!(severity, Some("ERROR".to_string()));
                assert_eq!(
                    attribute,
                    vec![("env".to_string(), "production".to_string())]
                );
                assert!(!follow);
                assert!(since.is_none());
                assert!(until.is_none());
                assert_eq!(limit, 50);
                assert!(matches!(format, OutputFormat::Json));
            }
            _ => panic!("Expected Log command"),
        }
    }

    #[test]
    fn trace_subcommand_parses_with_trace_id() {
        let cli = Cli::parse_from([
            "otel-cli",
            "trace",
            "--trace-id",
            "abc123def456",
            "--service",
            "frontend",
        ]);
        match cli.command {
            Commands::Trace {
                server,
                service,
                trace_id,
                attribute,
                limit,
                format,
                follow,
                full,
                since,
                until,
            } => {
                assert_eq!(server, "http://localhost:4319");
                assert_eq!(service, Some("frontend".to_string()));
                assert_eq!(trace_id, Some("abc123def456".to_string()));
                assert!(attribute.is_empty());
                assert_eq!(limit, 100);
                assert!(matches!(format, OutputFormat::Text));
                assert!(!follow);
                assert!(!full);
                assert!(since.is_none());
                assert!(until.is_none());
            }
            _ => panic!("Expected Trace command"),
        }
    }

    #[test]
    fn metrics_subcommand_parses_with_name() {
        let cli = Cli::parse_from([
            "otel-cli",
            "metrics",
            "--name",
            "http.request.duration",
            "--service",
            "api-gateway",
            "--limit",
            "200",
        ]);
        match cli.command {
            Commands::Metrics {
                server,
                service,
                name,
                limit,
                format,
                follow,
                since,
                until,
            } => {
                assert_eq!(server, "http://localhost:4319");
                assert_eq!(service, Some("api-gateway".to_string()));
                assert_eq!(name, Some("http.request.duration".to_string()));
                assert_eq!(limit, 200);
                assert!(matches!(format, OutputFormat::Text));
                assert!(!follow);
                assert!(since.is_none());
                assert!(until.is_none());
            }
            _ => panic!("Expected Metrics command"),
        }
    }

    #[test]
    fn attribute_key_value_parsing_works() {
        let cli = Cli::parse_from([
            "otel-cli",
            "log",
            "--attribute",
            "env=production",
            "--attribute",
            "region=us-east-1",
        ]);
        match cli.command {
            Commands::Log { attribute, .. } => {
                assert_eq!(attribute.len(), 2);
                assert_eq!(attribute[0], ("env".to_string(), "production".to_string()));
                assert_eq!(
                    attribute[1],
                    ("region".to_string(), "us-east-1".to_string())
                );
            }
            _ => panic!("Expected Log command"),
        }
    }

    #[test]
    fn view_subcommand_parses_with_defaults() {
        let cli = Cli::parse_from(["otel-cli", "view"]);
        match cli.command {
            Commands::View { server, max_items } => {
                assert_eq!(server, "http://localhost:4319");
                assert_eq!(max_items, 1000);
            }
            _ => panic!("Expected View command"),
        }
    }

    #[test]
    fn view_subcommand_parses_with_custom_args() {
        let cli = Cli::parse_from([
            "otel-cli",
            "view",
            "--server",
            "http://remote:5319",
            "--max-items",
            "500",
        ]);
        match cli.command {
            Commands::View { server, max_items } => {
                assert_eq!(server, "http://remote:5319");
                assert_eq!(max_items, 500);
            }
            _ => panic!("Expected View command"),
        }
    }

    #[test]
    fn attribute_parsing_rejects_invalid_format() {
        let result = Cli::try_parse_from(["otel-cli", "log", "--attribute", "no-equals-sign"]);
        assert!(result.is_err());
    }

    #[test]
    fn attribute_value_can_contain_equals() {
        let cli = Cli::parse_from(["otel-cli", "trace", "--attribute", "query=a=b"]);
        match cli.command {
            Commands::Trace { attribute, .. } => {
                assert_eq!(attribute.len(), 1);
                assert_eq!(attribute[0], ("query".to_string(), "a=b".to_string()));
            }
            _ => panic!("Expected Trace command"),
        }
    }

    #[test]
    fn skill_install_subcommand_parses_defaults() {
        let cli = Cli::parse_from(["otel-cli", "skill-install"]);
        match cli.command {
            Commands::SkillInstall { global, force } => {
                assert!(!global);
                assert!(!force);
            }
            _ => panic!("Expected SkillInstall command"),
        }
    }

    #[test]
    fn skill_install_subcommand_parses_with_flags() {
        let cli = Cli::parse_from(["otel-cli", "skill-install", "--global", "--force"]);
        match cli.command {
            Commands::SkillInstall { global, force } => {
                assert!(global);
                assert!(force);
            }
            _ => panic!("Expected SkillInstall command"),
        }
    }
}

use clap::Parser;
use otel_cli::cli::{Cli, Commands};
use otel_cli::{client, server, store};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Server {
            grpc_addr,
            http_addr,
            query_addr,
            max_items,
            no_tui,
        } => {
            let (store, event_rx) = store::new_shared(max_items);
            let shutdown = CancellationToken::new();

            let grpc_addr: std::net::SocketAddr = grpc_addr.parse()?;
            let http_addr: std::net::SocketAddr = http_addr.parse()?;
            let query_addr: std::net::SocketAddr = query_addr.parse()?;

            let (grpc_listener, http_listener, query_listener) =
                server::bind_listeners(grpc_addr, http_addr, query_addr).await?;

            let grpc_handle = tokio::spawn(server::run_grpc_server(
                grpc_listener,
                store.clone(),
                shutdown.clone(),
            ));
            let http_handle = tokio::spawn(server::run_http_server(
                http_listener,
                store.clone(),
                shutdown.clone(),
            ));
            let query_handle = tokio::spawn(server::run_query_server(
                query_listener,
                store.clone(),
                shutdown.clone(),
            ));

            if no_tui {
                eprintln!("gRPC server listening on {}", grpc_addr);
                eprintln!("HTTP server listening on {}", http_addr);
                eprintln!("Query server listening on {}", query_addr);
                tokio::signal::ctrl_c().await.ok();
                eprintln!("\nShutting down...");
                shutdown.cancel();
                let _ = grpc_handle.await;
                let _ = http_handle.await;
                let _ = query_handle.await;
            } else {
                eprintln!(
                    "Starting OTLP server (gRPC: {}, HTTP: {}, Query: {})",
                    grpc_addr, http_addr, query_addr
                );
                otel_cli::tui::run(store.clone(), event_rx).await?;
                shutdown.cancel();
                let _ = grpc_handle.await;
                let _ = http_handle.await;
                let _ = query_handle.await;
            }

            Ok(())
        }
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
            if follow {
                client::log::follow_logs(
                    &server, service, severity, attribute, limit, &format, since, until,
                )
                .await?;
            } else {
                client::log::query_logs(
                    &server, service, severity, attribute, limit, &format, since, until,
                )
                .await?;
            }
            Ok(())
        }
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
            if follow {
                client::trace::follow_traces(
                    &server, service, trace_id, attribute, limit, &format, since, until, !full,
                )
                .await?;
            } else {
                client::trace::query_traces(
                    &server, service, trace_id, attribute, limit, &format, since, until,
                )
                .await?;
            }
            Ok(())
        }
        Commands::Clear {
            server,
            traces,
            logs,
            metrics,
        } => {
            client::clear::clear(&server, traces, logs, metrics).await?;
            Ok(())
        }
        Commands::View { server, max_items } => {
            client::view::run_view(&server, max_items).await?;
            Ok(())
        }
        Commands::Sql {
            server,
            query,
            format,
            follow,
        } => {
            if follow {
                client::sql::follow_sql(&server, &query, &format).await?;
            } else {
                client::sql::query_sql(&server, &query, &format).await?;
            }
            Ok(())
        }
        Commands::SkillInstall { global, force } => {
            otel_cli::install::run(global, force)?;
            Ok(())
        }
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
            if follow {
                client::metrics::follow_metrics(
                    &server, service, name, limit, &format, since, until,
                )
                .await?;
            } else {
                client::metrics::query_metrics(
                    &server, service, name, limit, &format, since, until,
                )
                .await?;
            }
            Ok(())
        }
    }
}

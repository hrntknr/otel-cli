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
            max_items,
            no_tui,
        } => {
            let (store, event_rx) = store::new_shared(max_items);
            let shutdown = CancellationToken::new();

            let grpc_addr: std::net::SocketAddr = grpc_addr.parse()?;
            let http_addr: std::net::SocketAddr = http_addr.parse()?;

            let grpc_handle = tokio::spawn(server::run_grpc_server(
                grpc_addr,
                store.clone(),
                shutdown.clone(),
            ));
            let http_handle = tokio::spawn(server::run_http_server(
                http_addr,
                store.clone(),
                shutdown.clone(),
            ));

            if no_tui {
                eprintln!("gRPC server listening on {}", grpc_addr);
                eprintln!("HTTP server listening on {}", http_addr);
                tokio::signal::ctrl_c().await.ok();
                eprintln!("\nShutting down...");
                shutdown.cancel();
                let _ = grpc_handle.await;
                let _ = http_handle.await;
            } else {
                eprintln!(
                    "Starting OTLP server (gRPC: {}, HTTP: {})",
                    grpc_addr, http_addr
                );
                otel_cli::tui::run(store.clone(), event_rx).await?;
                shutdown.cancel();
                let _ = grpc_handle.await;
                let _ = http_handle.await;
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
        } => {
            client::log::query_logs(&server, service, severity, attribute, limit, &format).await?;
            Ok(())
        }
        Commands::Trace {
            server,
            service,
            trace_id,
            attribute,
            limit,
            format,
        } => {
            client::trace::query_traces(&server, service, trace_id, attribute, limit, &format)
                .await?;
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
        Commands::Metrics {
            server,
            service,
            name,
            limit,
            format,
        } => {
            client::metrics::query_metrics(&server, service, name, limit, &format).await?;
            Ok(())
        }
    }
}

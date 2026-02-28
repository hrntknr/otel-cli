fn main() -> Result<(), Box<dyn std::error::Error>> {
    let descriptor_path =
        std::path::PathBuf::from(std::env::var("OUT_DIR")?).join("proto_descriptor.bin");

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .file_descriptor_set_path(&descriptor_path)
        .compile_protos(
            &[
                "proto/opentelemetry-proto/opentelemetry/proto/collector/trace/v1/trace_service.proto",
                "proto/opentelemetry-proto/opentelemetry/proto/collector/logs/v1/logs_service.proto",
                "proto/opentelemetry-proto/opentelemetry/proto/collector/metrics/v1/metrics_service.proto",
                "proto/query.proto",
            ],
            &["proto/opentelemetry-proto", "proto"],
        )?;

    let descriptor_set = std::fs::read(&descriptor_path)?;
    pbjson_build::Builder::new()
        .register_descriptors(&descriptor_set)?
        .build(&[".opentelemetry"])?;

    Ok(())
}

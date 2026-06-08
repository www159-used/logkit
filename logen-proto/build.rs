fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &["proto/logen/v1/logen.proto", "proto/agent/v1/event.proto"],
            &["proto"],
        )?;
    Ok(())
}

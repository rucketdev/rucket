//! Build script for rucket-consensus - compiles protobuf definitions.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/raft.proto"], &["proto"])?;
    Ok(())
}

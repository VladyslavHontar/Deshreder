fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Reuse the repo's Jito-compatible shredstream proto (same one Colibri serves
    // and Lumen's subscriber consumes).
    tonic_build::compile_protos("../protos/shredstream.proto")?;
    Ok(())
}

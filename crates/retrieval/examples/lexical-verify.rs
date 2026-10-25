//! Trusted offline verification. Errors contain no source paths or contents.
fn main() {
    if run().is_err() {
        eprintln!(
            "offline runtime verification failed; retain artifacts and verify explicit inputs"
        );
        std::process::exit(2);
    }
}
#[cfg(target_os = "linux")]
fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if args.len() != 4
        || args[1] != "--all-source-processes-stopped"
        || (args[2] != "--evidence" && args[2] != "--retained")
    {
        return Err("invalid explicit offline arguments".into());
    }
    notion_knowledge_retrieval::rollback_runtime::operator::verify(
        std::path::Path::new(&args[0]),
        std::path::Path::new(&args[3]),
        args[2] == "--retained",
        true,
    )?;
    Ok(())
}
#[cfg(not(target_os = "linux"))]
fn run() -> Result<(), Box<dyn std::error::Error>> {
    Err("Linux required".into())
}

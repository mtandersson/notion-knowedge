//! Explicit offline runtime description/probe; never a serving-state operator.
#[cfg(target_os = "linux")]
fn main() {
    if run().is_err() {
        eprintln!("offline lexical runtime probe failed");
        std::process::exit(2);
    }
}
#[cfg(not(target_os = "linux"))]
fn main() {
    std::process::exit(2);
}

#[cfg(target_os = "linux")]
fn run() -> Result<(), Box<dyn std::error::Error>> {
    use notion_knowledge_retrieval::{
        chunks::lexical_binding::runtime::RuntimeDescriptor,
        lexical_dictionary::Dictionary,
        rollback_runtime::{probe, source},
    };
    use std::{ffi::OsStr, io::Write, path::Path};
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    let action = args.first().ok_or("missing action")?;
    let mode_index = match action.to_str() {
        Some("describe") => 1,
        Some(
            "probe-create" | "probe-reopen" | "inspect-source" | "candidate-create"
            | "candidate-reopen" | "refence-owned",
        ) => 2,
        _ => return Err("unsupported action".into()),
    };
    let mode = args.get(mode_index).ok_or("missing lexical mode")?;
    let dictionary = if mode == "legacy" && args.len() == mode_index + 1 {
        None
    } else if mode == "expansion" && args.len() == mode_index + 3 {
        let asset = Path::new(&args[mode_index + 1]);
        let sha = args[mode_index + 2]
            .to_str()
            .ok_or("invalid asset digest")?;
        Some(Dictionary::load(asset, sha)?)
    } else {
        return Err("invalid lexical mode or arguments".into());
    };
    let bytes = if action == OsStr::new("describe") {
        RuntimeDescriptor::current(dictionary.as_ref())?.canonical_bytes()?
    } else if action == OsStr::new("candidate-create") || action == OsStr::new("candidate-reopen") {
        notion_knowledge_retrieval::chunks::runtime_builder::execute(
            Path::new(&args[1]),
            dictionary.as_ref(),
            action == OsStr::new("candidate-create"),
        )?
    } else if action == OsStr::new("refence-owned") {
        notion_knowledge_retrieval::chunks::runtime_refence::execute(
            Path::new(&args[1]),
            dictionary.as_ref(),
        )?
    } else if action == OsStr::new("inspect-source") {
        source::inspect_request(Path::new(&args[1]), dictionary.as_ref())?
    } else {
        let root = Path::new(&args[1]);
        if !root.is_absolute() {
            return Err("probe root must be absolute".into());
        }
        let report = if action == OsStr::new("probe-create") {
            probe::create(root, dictionary.as_ref())?
        } else {
            probe::reopen(root, dictionary.as_ref())?
        };
        serde_json::to_vec(&report)?
    };
    std::io::stdout().lock().write_all(&bytes)?;
    Ok(())
}

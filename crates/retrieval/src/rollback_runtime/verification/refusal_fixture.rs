//! Test-only real process: malformed output, excess output, or a runtime that
//! can read the source but couples policy generation to physical generation.
use std::{
    io::Write,
    process::{Command, ExitCode},
};
fn main() -> ExitCode {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    let mode = env!("NK_VERIFIER_BEHAVIOR");
    if args.first().is_some_and(|a| a == "inspect-source") {
        match mode {
            "malformed" => {
                print!("{{");
                return ExitCode::SUCCESS;
            }
            "oversized" => {
                let _ = std::io::stdout().write_all(&vec![b'x'; 70000]);
                return ExitCode::SUCCESS;
            }
            "coupled" => {
                let status = Command::new(env!("NK_VERIFIER_RUNTIME"))
                    .args(args)
                    .status()
                    .unwrap();
                return if status.success() {
                    ExitCode::SUCCESS
                } else {
                    ExitCode::FAILURE
                };
            }
            _ => return ExitCode::FAILURE,
        }
    }
    if mode == "coupled" && args.first().is_some_and(|a| a == "probe-create") {
        // The v1 probe deliberately keeps this policy while fencing storage to
        // another generation. An implementation requiring equality cannot run it.
        let policy_generation = "source-policy";
        let physical_generation = "probe-current";
        if physical_generation != policy_generation {
            std::fs::write(env!("NK_VERIFIER_MARKER"), "separated generation refused").unwrap();
            return ExitCode::FAILURE;
        }
    }
    ExitCode::FAILURE
}

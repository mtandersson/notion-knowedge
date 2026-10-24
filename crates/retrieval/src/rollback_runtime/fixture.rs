//! Small real executable used by transport tests in every feature combination.
use std::io::Read;
fn main() {
    match std::env::args().nth(1).as_deref() {
        Some("clean") => {
            assert_eq!(std::env::vars_os().count(), 0);
            let mut input = String::new();
            std::io::stdin().read_to_string(&mut input).unwrap();
            assert!(input.is_empty());
            print!("clean");
        }
        Some("descendant") => {
            let child = std::process::Command::new("/proc/self/exe").arg("timeout").spawn().unwrap();
            std::fs::write(std::env::args().nth(2).unwrap(), child.id().to_string()).unwrap();
            // The child deliberately retains the inherited output pipes after
            // this group leader exits; the verifier must kill the whole group.
        }
        Some("nonzero") => std::process::exit(7),
        Some("output") => eprintln!("{}", "x".repeat(8192)),
        Some("timeout") => std::thread::sleep(std::time::Duration::from_secs(30)),
        _ => std::process::exit(2),
    }
}

use std::{env, path::Path, process::Command};
fn main() {
    let rustc = env::var_os("RUSTC").expect("Cargo supplies RUSTC");
    let output = Command::new(rustc)
        .args(["--version", "--verbose"])
        .output()
        .expect("compiler identity");
    assert!(output.status.success(), "compiler identity unavailable");
    let identity = String::from_utf8(output.stdout)
        .expect("compiler identity UTF-8")
        .replace('\n', ";");
    println!("cargo:rustc-env=NK_LEXICAL_COMPILER={identity}");
    let root = Path::new(&env::var("CARGO_MANIFEST_DIR").unwrap()).join("../..");
    let revision = Command::new("git")
        .current_dir(&root)
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .unwrap_or_else(|| "source-archive".into());
    println!(
        "cargo:rustc-env=NK_LEXICAL_SOURCE_REVISION={}",
        revision.trim()
    );
    // Refresh build identity whenever source, lockfile or HEAD changes. Source
    // hashes additionally identify uncommitted contents in development builds.
    println!("cargo:rerun-if-changed=src/lexical_dictionary.rs");
    println!("cargo:rerun-if-changed=src/lexical_dictionary");
    println!("cargo:rerun-if-changed=../../Cargo.lock");
    for args in [
        vec!["rev-parse", "--git-path", "HEAD"],
        vec!["rev-parse", "--symbolic-full-name", "HEAD"],
    ] {
        if let Ok(o) = Command::new("git").current_dir(&root).args(&args).output()
            && o.status.success()
        {
            let path = String::from_utf8_lossy(&o.stdout).trim().to_owned();
            if args[1] == "--git-path" {
                println!("cargo:rerun-if-changed={}", root.join(path).display());
            } else if let Ok(o) = Command::new("git")
                .current_dir(&root)
                .args(["rev-parse", "--git-path", &path])
                .output()
            {
                println!(
                    "cargo:rerun-if-changed={}",
                    root.join(String::from_utf8_lossy(&o.stdout).trim())
                        .display()
                );
            }
        }
    }
}

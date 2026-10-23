//! Explicit offline preparation; deliberately absent from the shipping server.
use notion_knowledge_retrieval::lexical_dictionary::prepare_archive;
use std::{env, path::Path, process::ExitCode};
fn main() -> ExitCode {
    let arguments: Vec<_> = env::args_os().skip(1).collect();
    if arguments.len() != 2 {
        eprintln!("usage: lexical-asset PINNED_SOURCE.tar.xz NEW_BUNDLE");
        return ExitCode::FAILURE;
    }
    match prepare_archive(Path::new(&arguments[0]), Path::new(&arguments[1])) {
        Ok(info) => match serde_json::to_string(&info) {
            Ok(info) => {
                println!("{info}");
                ExitCode::SUCCESS
            }
            Err(_) => {
                eprintln!("lexical asset metadata unavailable");
                ExitCode::FAILURE
            }
        },
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

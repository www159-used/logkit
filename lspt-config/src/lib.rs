//! Shared configuration (TOML), embedded reference defaults, and [`LsptError`].

mod config;
mod embed;
mod error;

pub use config::{
    load_merged, ClientSection, DaemonSection, GrpcSection, LogServerSection, LsptConfig,
    ProtocolSection,
};
pub use error::LsptError;

use std::path::PathBuf;

/// Strip `--defaults-file <path>` from argv; remaining tokens are subcommands / arguments.
pub fn parse_cli_args(args: Vec<String>) -> Result<(Option<PathBuf>, Vec<String>), LsptError> {
    let mut defaults = None;
    let mut rest = Vec::new();
    let mut i = 0;
    while i < args.len() {
        if args[i] == "--defaults-file" {
            let path = args
                .get(i + 1)
                .ok_or_else(|| {
                    LsptError::Cli("--defaults-file must be followed by a path".into())
                })?
                .clone();
            defaults = Some(PathBuf::from(path));
            i += 2;
        } else {
            rest.push(args[i].clone());
            i += 1;
        }
    }
    Ok((defaults, rest))
}

#![forbid(unsafe_code)]

use clap::Parser;

#[derive(Debug, Parser)]
#[command(
    name = "herdr-flow",
    version,
    about = "Deterministic workflow coordinator for Herdr-managed AI agents"
)]
struct Cli {}

fn main() {
    let _cli = Cli::parse();
}

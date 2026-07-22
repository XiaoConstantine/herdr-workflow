#![forbid(unsafe_code)]

use std::{
    io::{IsTerminal, Write},
    path::PathBuf,
    process::ExitCode,
    str::FromStr,
};

use clap::{Parser, Subcommand, ValueEnum};
use herdr_flow_core::{
    canonical_json, MessageId, PublicationFeedbackTarget, PublicationGateCommand, RunId,
    Sha256Digest, StageInstanceId,
};
use herdr_flow_store::SqliteStore;

#[derive(Debug, Parser)]
#[command(
    name = "herdr-flow",
    version,
    about = "Deterministic workflow coordinator for Herdr-managed AI agents"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Inspect or submit an action through the dedicated human publication gate.
    Gate {
        #[command(subcommand)]
        command: GateCommand,
    },
}

#[derive(Debug, Subcommand)]
enum GateCommand {
    Show(GateTarget),
    Approve {
        #[command(flatten)]
        target: GateTarget,
        #[arg(long)]
        message_id: MessageId,
        #[arg(long)]
        manifest_digest: Sha256Digest,
        #[arg(long)]
        authorization_digest: Sha256Digest,
    },
    RequestChanges {
        #[command(flatten)]
        target: GateTarget,
        #[arg(long)]
        message_id: MessageId,
        #[arg(long)]
        manifest_digest: Sha256Digest,
        #[arg(long)]
        feedback_digest: Sha256Digest,
        #[arg(long, value_enum)]
        feedback_target: FeedbackTarget,
    },
    Cancel {
        #[command(flatten)]
        target: GateTarget,
        #[arg(long)]
        message_id: MessageId,
        #[arg(long)]
        manifest_digest: Sha256Digest,
        #[arg(long)]
        cancellation_digest: Sha256Digest,
    },
}

#[derive(Clone, Debug, clap::Args)]
struct GateTarget {
    #[arg(long)]
    database: PathBuf,
    #[arg(long)]
    run_id: RunId,
    #[arg(long)]
    gate_stage_id: StageInstanceId,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum FeedbackTarget {
    PublicationMetadata,
    IntegrationResult,
    RiskWaiver,
}

impl From<FeedbackTarget> for PublicationFeedbackTarget {
    fn from(value: FeedbackTarget) -> Self {
        match value {
            FeedbackTarget::PublicationMetadata => Self::PublicationMetadata,
            FeedbackTarget::IntegrationResult => Self::IntegrationResult,
            FeedbackTarget::RiskWaiver => Self::RiskWaiver,
        }
    }
}

fn main() -> ExitCode {
    match run(Cli::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("herdr-flow: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run(cli: Cli) -> Result<(), String> {
    let Command::Gate { command } = cli.command;
    match command {
        GateCommand::Show(target) => {
            let store = SqliteStore::open(&target.database).map_err(|error| error.to_string())?;
            let gate = store
                .load_publication_gate(&target.run_id, &target.gate_stage_id)
                .map_err(|error| error.to_string())?;
            let manifest = gate
                .manifest
                .as_ref()
                .ok_or_else(|| "publication manifest is not ready".to_owned())?;
            display_manifest(manifest).map(|_| ())
        }
        GateCommand::Approve {
            target,
            message_id,
            manifest_digest,
            authorization_digest,
        } => queue(
            &target,
            message_id,
            PublicationGateCommand::HumanApprove {
                expected_control_revision: confirm_human_action(&target, manifest_digest)?,
                manifest_digest,
                authorization_digest,
            },
        ),
        GateCommand::RequestChanges {
            target,
            message_id,
            manifest_digest,
            feedback_digest,
            feedback_target,
        } => queue(
            &target,
            message_id,
            PublicationGateCommand::HumanRequestChanges {
                expected_control_revision: confirm_human_action(&target, manifest_digest)?,
                manifest_digest,
                target: feedback_target.into(),
                feedback_digest,
            },
        ),
        GateCommand::Cancel {
            target,
            message_id,
            manifest_digest,
            cancellation_digest,
        } => queue(
            &target,
            message_id,
            PublicationGateCommand::HumanCancel {
                expected_control_revision: confirm_human_action(&target, manifest_digest)?,
                manifest_digest,
                cancellation_digest,
            },
        ),
    }
}

fn display_manifest(
    manifest: &herdr_flow_core::PublicationManifest,
) -> Result<Sha256Digest, String> {
    let value = serde_json::to_value(manifest).map_err(|error| error.to_string())?;
    let bytes = canonical_json::to_vec(&value).map_err(|error| error.to_string())?;
    let digest = Sha256Digest::of_bytes(&bytes);
    println!("manifest_digest={digest}");
    std::io::stdout()
        .write_all(&bytes)
        .and_then(|()| std::io::stdout().write_all(b"\n"))
        .map_err(|error| error.to_string())?;
    Ok(digest)
}

fn confirm_human_action(
    target: &GateTarget,
    expected_manifest_digest: Sha256Digest,
) -> Result<u64, String> {
    if !std::io::stdin().is_terminal() || !std::io::stdout().is_terminal() {
        return Err("human gate decisions require an interactive terminal".to_owned());
    }
    let store = SqliteStore::open(&target.database).map_err(|error| error.to_string())?;
    let gate = store
        .load_publication_gate(&target.run_id, &target.gate_stage_id)
        .map_err(|error| error.to_string())?;
    let manifest = gate
        .manifest
        .as_ref()
        .ok_or_else(|| "publication manifest is not ready".to_owned())?;
    let displayed_digest = display_manifest(manifest)?;
    if displayed_digest != expected_manifest_digest {
        return Err("requested manifest digest is not the authoritative manifest".to_owned());
    }
    print!("Type the full manifest digest to confirm this human action: ");
    std::io::stdout()
        .flush()
        .map_err(|error| error.to_string())?;
    let mut confirmation = String::new();
    std::io::stdin()
        .read_line(&mut confirmation)
        .map_err(|error| error.to_string())?;
    if confirmation.trim() != displayed_digest.to_prefixed_string() {
        return Err("human confirmation did not match the displayed manifest digest".to_owned());
    }
    Ok(gate.control_revision)
}

fn queue(
    target: &GateTarget,
    message_id: MessageId,
    command: PublicationGateCommand,
) -> Result<(), String> {
    let mut store = SqliteStore::open(&target.database).map_err(|error| error.to_string())?;
    let action = store
        .queue_human_publication_action(
            &message_id,
            &target.run_id,
            &target.gate_stage_id,
            &command,
        )
        .map_err(|error| error.to_string())?;
    println!("queued {} {}", action.message_id, action.command_digest);
    Ok(())
}

// Keep clap's generated parser tied to the same strict digest parser exposed by
// the domain type, rather than accepting an alternate CLI-only representation.
fn _digest_parser_is_canonical(value: &str) -> Result<Sha256Digest, String> {
    Sha256Digest::from_str(value).map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn noninteractive_process_cannot_submit_a_human_decision() {
        if std::io::stdin().is_terminal() || std::io::stdout().is_terminal() {
            return;
        }
        let target = GateTarget {
            database: "/path/that/must/not/be/opened.sqlite3".into(),
            run_id: RunId::parse("flow_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
            gate_stage_id: StageInstanceId::parse("stage_01ARZ3NDEKTSV4RRFFQ69G5FAW").unwrap(),
        };
        assert_eq!(
            confirm_human_action(&target, Sha256Digest::of_bytes(b"manifest")),
            Err("human gate decisions require an interactive terminal".to_owned())
        );
    }
}

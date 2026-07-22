use std::{
    ffi::OsString,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
};

use herdr_flow_core::{
    canonical_json, ParticipantPrincipalId, RunId, Sha256Digest, StageInstanceId,
};
use rusqlite::{params, OptionalExtension};
use serde::{Deserialize, Serialize};

use crate::{SqliteStore, StoreError};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AgentProduct {
    Codex,
    Pi,
}

impl AgentProduct {
    fn executable(self) -> &'static str {
        match self {
            Self::Codex => "codex",
            Self::Pi => "pi",
        }
    }

    fn integration_label(self) -> &'static str {
        match self {
            Self::Codex => "codex",
            Self::Pi => "pi",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HerdrAgentAdapter {
    product: AgentProduct,
    adapter_digest: Sha256Digest,
    herdr_executable: PathBuf,
    agent_executable: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FrozenWorktree {
    canonical_path: PathBuf,
    device: u64,
    inode: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IntegrationPreflightReceipt {
    product: AgentProduct,
    adapter_digest: Sha256Digest,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HerdrAgentObservation {
    product: AgentProduct,
    terminal_id: String,
    pane_id: String,
    agent_session_id: String,
    workspace_id: String,
    cwd: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AgentBindingTarget<'a> {
    pub run_id: &'a RunId,
    pub stage_instance_id: &'a StageInstanceId,
    pub role_slot: &'a str,
    pub workspace_id: &'a str,
    pub worktree: &'a FrozenWorktree,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AgentSessionBinding {
    principal: ParticipantPrincipalId,
    product: AgentProduct,
    terminal_id: String,
    pane_id: String,
    agent_session_id: String,
    workspace_id: String,
    cwd: PathBuf,
    cwd_device: u64,
    cwd_inode: u64,
    adapter_digest: Sha256Digest,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdapterContractError {
    RelativeExecutable,
    WrongAgentExecutable,
    EmptyOpaqueIdentity,
    ProductMismatch,
    AdapterMismatch,
    SamePrincipal,
    SameProduct,
    RuntimeIdentityAlias,
    IntegrationMissingOrOutdated,
    ObservationMismatch,
    InvalidObservation,
    WorktreeIdentityMismatch,
}

impl FrozenWorktree {
    pub fn seal(path: &Path) -> Result<Self, AdapterContractError> {
        let canonical_path = std::fs::canonicalize(path)
            .map_err(|_| AdapterContractError::WorktreeIdentityMismatch)?;
        if canonical_path != path {
            return Err(AdapterContractError::WorktreeIdentityMismatch);
        }
        let metadata = std::fs::metadata(&canonical_path)
            .map_err(|_| AdapterContractError::WorktreeIdentityMismatch)?;
        if !metadata.is_dir() {
            return Err(AdapterContractError::WorktreeIdentityMismatch);
        }
        Ok(Self {
            canonical_path,
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }

    fn verify_path(&self, path: &Path) -> Result<(), AdapterContractError> {
        let candidate = Self::seal(path)?;
        if candidate != *self {
            return Err(AdapterContractError::WorktreeIdentityMismatch);
        }
        Ok(())
    }
}

impl HerdrAgentObservation {
    pub fn from_herdr_json(json: &str) -> Result<Self, AdapterContractError> {
        let value: serde_json::Value =
            serde_json::from_str(json).map_err(|_| AdapterContractError::InvalidObservation)?;
        let agent = value
            .pointer("/result/agent")
            .ok_or(AdapterContractError::InvalidObservation)?;
        let field = |name: &str| {
            agent
                .get(name)
                .and_then(serde_json::Value::as_str)
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
                .ok_or(AdapterContractError::InvalidObservation)
        };
        let product = match field("agent")?.as_str() {
            "codex" => AgentProduct::Codex,
            "pi" => AgentProduct::Pi,
            _ => return Err(AdapterContractError::InvalidObservation),
        };
        let cwd = PathBuf::from(field("cwd")?);
        if !cwd.is_absolute() {
            return Err(AdapterContractError::InvalidObservation);
        }
        Ok(Self {
            product,
            terminal_id: field("terminal_id")?,
            pane_id: field("pane_id")?,
            agent_session_id: field("agent_session_id")?,
            workspace_id: field("workspace_id")?,
            cwd,
        })
    }
}

impl HerdrAgentAdapter {
    pub fn codex(
        herdr_executable: PathBuf,
        agent_executable: PathBuf,
        adapter_digest: Sha256Digest,
    ) -> Result<Self, AdapterContractError> {
        Self::new(
            AgentProduct::Codex,
            herdr_executable,
            agent_executable,
            adapter_digest,
        )
    }

    pub fn pi(
        herdr_executable: PathBuf,
        agent_executable: PathBuf,
        adapter_digest: Sha256Digest,
    ) -> Result<Self, AdapterContractError> {
        Self::new(
            AgentProduct::Pi,
            herdr_executable,
            agent_executable,
            adapter_digest,
        )
    }

    fn new(
        product: AgentProduct,
        herdr_executable: PathBuf,
        agent_executable: PathBuf,
        adapter_digest: Sha256Digest,
    ) -> Result<Self, AdapterContractError> {
        if !herdr_executable.is_absolute() || !agent_executable.is_absolute() {
            return Err(AdapterContractError::RelativeExecutable);
        }
        if agent_executable.file_name().and_then(|name| name.to_str()) != Some(product.executable())
        {
            return Err(AdapterContractError::WrongAgentExecutable);
        }
        Ok(Self {
            product,
            adapter_digest,
            herdr_executable,
            agent_executable,
        })
    }

    pub fn preflight_argv(&self) -> Vec<OsString> {
        vec![
            self.herdr_executable.clone().into_os_string(),
            "integration".into(),
            "status".into(),
        ]
    }

    pub fn validate_preflight_output(
        &self,
        stdout: &str,
    ) -> Result<IntegrationPreflightReceipt, AdapterContractError> {
        let prefix = format!("{}: ", self.product.integration_label());
        let status = stdout
            .lines()
            .find_map(|line| line.strip_prefix(&prefix))
            .ok_or(AdapterContractError::IntegrationMissingOrOutdated)?;
        if !status.starts_with("installed") || status.contains("outdated") {
            return Err(AdapterContractError::IntegrationMissingOrOutdated);
        }
        Ok(IntegrationPreflightReceipt {
            product: self.product,
            adapter_digest: self.adapter_digest,
        })
    }

    pub fn start_argv(
        &self,
        name: &str,
        checkout: &FrozenWorktree,
        workspace_id: &str,
        report_socket: &Path,
        role_token_path: &Path,
        preflight: &IntegrationPreflightReceipt,
    ) -> Result<Vec<OsString>, AdapterContractError> {
        if name.is_empty() || workspace_id.is_empty() {
            return Err(AdapterContractError::EmptyOpaqueIdentity);
        }
        if preflight.product != self.product || preflight.adapter_digest != self.adapter_digest {
            return Err(AdapterContractError::IntegrationMissingOrOutdated);
        }
        Ok(vec![
            self.herdr_executable.clone().into_os_string(),
            "agent".into(),
            "start".into(),
            name.into(),
            "--cwd".into(),
            checkout.canonical_path.as_os_str().to_owned(),
            "--workspace".into(),
            workspace_id.into(),
            "--no-focus".into(),
            "--env".into(),
            format!("HERDR_FLOW_REPORT_SOCKET={}", report_socket.display()).into(),
            "--env".into(),
            format!("HERDR_FLOW_ROLE_TOKEN_PATH={}", role_token_path.display()).into(),
            "--".into(),
            self.agent_executable.clone().into_os_string(),
        ])
    }

    pub fn delivery_argv(
        &self,
        binding: &AgentSessionBinding,
        observation: &HerdrAgentObservation,
        exact_prompt: &str,
    ) -> Result<Vec<OsString>, AdapterContractError> {
        self.validate_binding(binding, observation)?;
        Ok(vec![
            self.herdr_executable.clone().into_os_string(),
            "agent".into(),
            "send".into(),
            binding.terminal_id.clone().into(),
            exact_prompt.into(),
        ])
    }

    pub fn bind_observation(
        &self,
        principal: ParticipantPrincipalId,
        observation: &HerdrAgentObservation,
        preflight: &IntegrationPreflightReceipt,
        expected_workspace_id: &str,
        expected_worktree: &FrozenWorktree,
    ) -> Result<AgentSessionBinding, AdapterContractError> {
        if preflight.product != self.product
            || preflight.adapter_digest != self.adapter_digest
            || observation.product != self.product
            || observation.workspace_id != expected_workspace_id
            || observation.cwd != expected_worktree.canonical_path
        {
            return Err(AdapterContractError::ObservationMismatch);
        }
        expected_worktree.verify_path(&observation.cwd)?;
        let binding = AgentSessionBinding {
            principal,
            product: observation.product,
            terminal_id: observation.terminal_id.clone(),
            pane_id: observation.pane_id.clone(),
            agent_session_id: observation.agent_session_id.clone(),
            workspace_id: observation.workspace_id.clone(),
            cwd: observation.cwd.clone(),
            cwd_device: expected_worktree.device,
            cwd_inode: expected_worktree.inode,
            adapter_digest: self.adapter_digest,
        };
        self.validate_binding(&binding, observation)?;
        Ok(binding)
    }

    pub fn validate_binding(
        &self,
        binding: &AgentSessionBinding,
        observation: &HerdrAgentObservation,
    ) -> Result<(), AdapterContractError> {
        if binding.product != self.product {
            return Err(AdapterContractError::ProductMismatch);
        }
        if binding.adapter_digest != self.adapter_digest {
            return Err(AdapterContractError::AdapterMismatch);
        }
        if binding.terminal_id.is_empty()
            || binding.pane_id.is_empty()
            || binding.agent_session_id.is_empty()
        {
            return Err(AdapterContractError::EmptyOpaqueIdentity);
        }
        let frozen = FrozenWorktree {
            canonical_path: binding.cwd.clone(),
            device: binding.cwd_device,
            inode: binding.cwd_inode,
        };
        frozen.verify_path(&observation.cwd)?;
        if observation.product != binding.product
            || observation.terminal_id != binding.terminal_id
            || observation.pane_id != binding.pane_id
            || observation.agent_session_id != binding.agent_session_id
            || observation.workspace_id != binding.workspace_id
            || observation.cwd != binding.cwd
        {
            return Err(AdapterContractError::ObservationMismatch);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct DurableAgentBinding {
    run_id: RunId,
    stage_instance_id: StageInstanceId,
    role_slot: String,
    principal: ParticipantPrincipalId,
    product: AgentProduct,
    terminal_id: String,
    pane_id: String,
    agent_session_id: String,
    workspace_id: String,
    cwd: PathBuf,
    cwd_device: String,
    cwd_inode: String,
    adapter_digest: Sha256Digest,
}

impl DurableAgentBinding {
    #[cfg(test)]
    fn new(
        run_id: &RunId,
        stage_instance_id: &StageInstanceId,
        role_slot: &str,
        binding: &AgentSessionBinding,
    ) -> Self {
        Self {
            run_id: run_id.clone(),
            stage_instance_id: stage_instance_id.clone(),
            role_slot: role_slot.to_owned(),
            principal: binding.principal.clone(),
            product: binding.product,
            terminal_id: binding.terminal_id.clone(),
            pane_id: binding.pane_id.clone(),
            agent_session_id: binding.agent_session_id.clone(),
            workspace_id: binding.workspace_id.clone(),
            cwd: binding.cwd.clone(),
            cwd_device: binding.cwd_device.to_string(),
            cwd_inode: binding.cwd_inode.to_string(),
            adapter_digest: binding.adapter_digest,
        }
    }
}

impl SqliteStore {
    #[cfg(test)]
    pub fn persist_agent_session_binding(
        &mut self,
        run_id: &RunId,
        stage_instance_id: &StageInstanceId,
        role_slot: &str,
        binding: &AgentSessionBinding,
    ) -> Result<(), StoreError> {
        if role_slot.is_empty() {
            return Err(StoreError::AdapterContract(
                AdapterContractError::EmptyOpaqueIdentity,
            ));
        }
        let durable = DurableAgentBinding::new(run_id, stage_instance_id, role_slot, binding);
        let value = serde_json::to_value(&durable).map_err(StoreError::Serialization)?;
        let json = canonical_json::to_vec(&value).map_err(StoreError::Canonicalization)?;
        let digest = Sha256Digest::of_bytes(&json);
        let inserted = self
            .connection
            .execute(
                "INSERT OR IGNORE INTO agent_session_bindings(
                    run_id, stage_instance_id, role_slot, record_digest, record_json
                 ) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    run_id.as_str(),
                    stage_instance_id.as_str(),
                    role_slot,
                    digest.to_prefixed_string(),
                    json,
                ],
            )
            .map_err(StoreError::Sqlite)?;
        if inserted == 0 {
            let existing = load_durable_binding(self, run_id, stage_instance_id, role_slot)?;
            if existing != durable {
                return Err(StoreError::AgentBindingConflict);
            }
        }
        Ok(())
    }

    pub fn load_and_revalidate_agent_session_binding(
        &self,
        target: AgentBindingTarget<'_>,
        adapter: &HerdrAgentAdapter,
        observation: &HerdrAgentObservation,
    ) -> Result<AgentSessionBinding, StoreError> {
        let durable = load_durable_binding(
            self,
            target.run_id,
            target.stage_instance_id,
            target.role_slot,
        )?;
        let cwd_device = parse_identity_number(&durable.cwd_device)?;
        let cwd_inode = parse_identity_number(&durable.cwd_inode)?;
        if durable.run_id != *target.run_id
            || durable.stage_instance_id != *target.stage_instance_id
            || durable.role_slot != target.role_slot
            || durable.workspace_id != target.workspace_id
            || durable.cwd != target.worktree.canonical_path
            || cwd_device != target.worktree.device
            || cwd_inode != target.worktree.inode
        {
            return Err(StoreError::AdapterContract(
                AdapterContractError::WorktreeIdentityMismatch,
            ));
        }
        target
            .worktree
            .verify_path(&observation.cwd)
            .map_err(StoreError::AdapterContract)?;
        let binding = AgentSessionBinding {
            principal: durable.principal,
            product: durable.product,
            terminal_id: durable.terminal_id,
            pane_id: durable.pane_id,
            agent_session_id: durable.agent_session_id,
            workspace_id: durable.workspace_id,
            cwd: durable.cwd,
            cwd_device,
            cwd_inode,
            adapter_digest: durable.adapter_digest,
        };
        adapter
            .validate_binding(&binding, observation)
            .map_err(StoreError::AdapterContract)?;
        Ok(binding)
    }
}

fn load_durable_binding(
    store: &SqliteStore,
    run_id: &RunId,
    stage_instance_id: &StageInstanceId,
    role_slot: &str,
) -> Result<DurableAgentBinding, StoreError> {
    let row: Option<(String, Vec<u8>)> = store
        .connection
        .query_row(
            "SELECT record_digest, record_json FROM agent_session_bindings
             WHERE run_id = ?1 AND stage_instance_id = ?2 AND role_slot = ?3",
            params![run_id.as_str(), stage_instance_id.as_str(), role_slot],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(StoreError::Sqlite)?;
    let (digest, json) = row.ok_or(StoreError::AgentBindingNotFound)?;
    let digest: Sha256Digest = digest.parse().map_err(StoreError::Digest)?;
    let durable: DurableAgentBinding =
        serde_json::from_slice(&json).map_err(StoreError::Serialization)?;
    if durable.run_id != *run_id
        || durable.stage_instance_id != *stage_instance_id
        || durable.role_slot != role_slot
    {
        return Err(StoreError::CorruptData(
            "agent session binding assignment coordinates do not match its row key",
        ));
    }
    parse_identity_number(&durable.cwd_device)?;
    parse_identity_number(&durable.cwd_inode)?;
    let value = serde_json::to_value(&durable).map_err(StoreError::Serialization)?;
    let canonical = canonical_json::to_vec(&value).map_err(StoreError::Canonicalization)?;
    if canonical != json || Sha256Digest::of_bytes(&canonical) != digest {
        return Err(StoreError::CorruptData(
            "agent session binding failed integrity verification",
        ));
    }
    Ok(durable)
}

fn parse_identity_number(value: &str) -> Result<u64, StoreError> {
    let parsed = value.parse::<u64>().map_err(|_| {
        StoreError::CorruptData("agent session binding has an invalid filesystem identity")
    })?;
    if parsed.to_string() != value {
        return Err(StoreError::CorruptData(
            "agent session binding has a noncanonical filesystem identity",
        ));
    }
    Ok(parsed)
}

pub fn validate_m1_role_pair(
    implementer: &AgentSessionBinding,
    reviewer: &AgentSessionBinding,
) -> Result<(), AdapterContractError> {
    if implementer.principal == reviewer.principal {
        return Err(AdapterContractError::SamePrincipal);
    }
    if implementer.product == reviewer.product {
        return Err(AdapterContractError::SameProduct);
    }
    if implementer.terminal_id == reviewer.terminal_id
        || implementer.pane_id == reviewer.pane_id
        || implementer.agent_session_id == reviewer.agent_session_id
    {
        return Err(AdapterContractError::RuntimeIdentityAlias);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use herdr_flow_core::ParticipantPrincipalId;

    use super::*;

    fn digest(value: &[u8]) -> Sha256Digest {
        Sha256Digest::of_bytes(value)
    }

    fn binding(product: AgentProduct, suffix: &str) -> AgentSessionBinding {
        AgentSessionBinding {
            principal: ParticipantPrincipalId::parse(format!("principal_{suffix}")).unwrap(),
            product,
            terminal_id: format!("term-{suffix}"),
            pane_id: format!("pane-{suffix}"),
            agent_session_id: format!("session-{suffix}"),
            workspace_id: format!("workspace-{suffix}"),
            cwd: PathBuf::from(format!("/tmp/{suffix}")),
            cwd_device: 1,
            cwd_inode: 1,
            adapter_digest: digest(match product {
                AgentProduct::Codex => b"codex",
                AgentProduct::Pi => b"pi",
            }),
        }
    }

    #[test]
    fn concrete_adapters_use_opaque_targets_and_separate_secret_paths() {
        let adapter = HerdrAgentAdapter::codex(
            "/usr/local/bin/herdr".into(),
            "/usr/local/bin/codex".into(),
            digest(b"codex"),
        )
        .unwrap();
        assert_eq!(
            adapter.validate_preflight_output("codex: not installed (/tmp/hook)"),
            Err(AdapterContractError::IntegrationMissingOrOutdated)
        );
        let preflight = adapter
            .validate_preflight_output("pi: not installed\ncodex: installed (/tmp/hook)")
            .unwrap();
        let checkout_directory = tempfile::tempdir().unwrap();
        let checkout_path = std::fs::canonicalize(checkout_directory.path()).unwrap();
        let checkout = FrozenWorktree::seal(&checkout_path).unwrap();
        let start = adapter
            .start_argv(
                "implementer",
                &checkout,
                "workspace-id",
                Path::new("/tmp/report.sock"),
                Path::new("/tmp/role.token"),
                &preflight,
            )
            .unwrap();
        let rendered: Vec<_> = start.iter().map(|value| value.to_string_lossy()).collect();
        assert!(rendered.contains(&"--no-focus".into()));
        assert!(rendered
            .iter()
            .any(|value| value.starts_with("HERDR_FLOW_ROLE_TOKEN_PATH=")));
        assert!(!rendered.iter().any(|value| value.as_ref() == "role-secret"));
        assert_eq!(
            HerdrAgentObservation::from_herdr_json(
                r#"{"result":{"agent":{"agent":"unknown","terminal_id":"term","pane_id":"pane","agent_session_id":"session","workspace_id":"workspace-id","cwd":"/tmp/exact-worktree"}}}"#,
            ),
            Err(AdapterContractError::InvalidObservation)
        );
        let observation_json = serde_json::json!({
            "result": {"agent": {
                "agent": "codex",
                "terminal_id": "term_opaque",
                "pane_id": "pane_opaque",
                "agent_session_id": "session_opaque",
                "workspace_id": "workspace-id",
                "cwd": checkout.canonical_path,
            }}
        })
        .to_string();
        let observation = HerdrAgentObservation::from_herdr_json(&observation_json).unwrap();
        assert_eq!(
            adapter.bind_observation(
                ParticipantPrincipalId::parse("principal_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
                &observation,
                &preflight,
                "wrong-workspace",
                &checkout,
            ),
            Err(AdapterContractError::ObservationMismatch)
        );
        let binding = adapter
            .bind_observation(
                ParticipantPrincipalId::parse("principal_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
                &observation,
                &preflight,
                "workspace-id",
                &checkout,
            )
            .unwrap();
        assert_eq!(
            adapter
                .delivery_argv(&binding, &observation, "exact prompt")
                .unwrap()[3],
            "term_opaque"
        );
    }

    #[test]
    fn both_codex_pi_role_orders_are_admissible_but_same_product_is_not() {
        let codex = binding(AgentProduct::Codex, "01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let pi = binding(AgentProduct::Pi, "01ARZ3NDEKTSV4RRFFQ69G5FAW");
        validate_m1_role_pair(&codex, &pi).unwrap();
        validate_m1_role_pair(&pi, &codex).unwrap();
        let other_codex = binding(AgentProduct::Codex, "01ARZ3NDEKTSV4RRFFQ69G5FAX");
        assert_eq!(
            validate_m1_role_pair(&codex, &other_codex),
            Err(AdapterContractError::SameProduct)
        );
        let mut aliased_pi = pi.clone();
        aliased_pi.terminal_id.clone_from(&codex.terminal_id);
        assert_eq!(
            validate_m1_role_pair(&codex, &aliased_pi),
            Err(AdapterContractError::RuntimeIdentityAlias)
        );
    }

    #[test]
    fn binding_survives_restart_and_requires_a_fresh_exact_observation() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("bindings.sqlite3");
        let checkout_directory = tempfile::tempdir().unwrap();
        let checkout_path = std::fs::canonicalize(checkout_directory.path()).unwrap();
        let checkout = FrozenWorktree::seal(&checkout_path).unwrap();
        let adapter = HerdrAgentAdapter::codex(
            "/usr/local/bin/herdr".into(),
            "/usr/local/bin/codex".into(),
            digest(b"codex"),
        )
        .unwrap();
        let preflight = adapter
            .validate_preflight_output("codex: installed (/tmp/hook)")
            .unwrap();
        let observation_json = serde_json::json!({
            "result": {"agent": {
                "agent": "codex", "terminal_id": "term", "pane_id": "pane",
                "agent_session_id": "session", "workspace_id": "workspace",
                "cwd": checkout.canonical_path,
            }}
        })
        .to_string();
        let observation = HerdrAgentObservation::from_herdr_json(&observation_json).unwrap();
        let principal =
            ParticipantPrincipalId::parse("principal_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap();
        let binding = adapter
            .bind_observation(principal, &observation, &preflight, "workspace", &checkout)
            .unwrap();
        let run_id = RunId::parse("flow_01ARZ3NDEKTSV4RRFFQ69G5FAW").unwrap();
        let stage_id = StageInstanceId::parse("stage_01ARZ3NDEKTSV4RRFFQ69G5FAX").unwrap();
        let mut store = SqliteStore::open(&database).unwrap();
        store.create_run(&run_id, &digest(b"pipeline")).unwrap();
        store
            .register_stage(
                &run_id,
                &herdr_flow_core::StageState::new(
                    stage_id.clone(),
                    digest(b"component"),
                    digest(b"predicate"),
                ),
            )
            .unwrap();
        store
            .persist_agent_session_binding(&run_id, &stage_id, "IMPLEMENTER", &binding)
            .unwrap();
        drop(store);

        let reopened = SqliteStore::open(&database).unwrap();
        assert_eq!(
            reopened
                .load_and_revalidate_agent_session_binding(
                    AgentBindingTarget {
                        run_id: &run_id,
                        stage_instance_id: &stage_id,
                        role_slot: "IMPLEMENTER",
                        workspace_id: "workspace",
                        worktree: &checkout,
                    },
                    &adapter,
                    &observation,
                )
                .unwrap(),
            binding
        );
    }

    #[test]
    fn configured_product_name_matches_the_real_agent_executable_contract() {
        assert_eq!(AgentProduct::Codex.executable(), "codex");
        assert_eq!(AgentProduct::Pi.executable(), "pi");
    }
}

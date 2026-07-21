use alloc::{format, string::String, vec::Vec};
use core::fmt;

use serde::{
    de::{self, MapAccess, SeqAccess, Visitor},
    Deserialize, Deserializer, Serialize,
};
use serde_json::{Map, Number, Value};

use crate::{
    canonical_json, ArtifactId, MessageId, ParticipantPrincipalId, RoleBindingId, RunId,
    Sha256Digest, StageInstanceId, BASE_PROTOCOL,
};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MessageKind {
    AgentReport,
    HumanCommand,
    InternalCommand,
    CommittedEvent,
}

/// Binding resolved by the trusted transport after authenticating the role
/// credential and revalidating the exact participant process/session identity.
/// The envelope claims the opaque role binding; the remaining fields are the
/// coordinator-owned facts that binding resolves to.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthenticatedAgentContext {
    pub run_id: RunId,
    pub stage_instance_id: StageInstanceId,
    pub role_binding_id: RoleBindingId,
    pub participant_principal_id: ParticipantPrincipalId,
    pub agent_session_id: String,
    pub stage_protocol: String,
    pub component_digest: Sha256Digest,
    pub input_manifest_digest: Sha256Digest,
    pub allowed_report_types: Vec<String>,
}

#[derive(Clone, Copy, Debug)]
pub enum SubmissionAuthority<'a> {
    AgentCredential(&'a AuthenticatedAgentContext),
    ProtectedHumanChannel,
    InternalCoordinator,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactReference {
    pub artifact_id: ArtifactId,
    pub artifact_type: String,
    pub sha256: Sha256Digest,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Envelope {
    pub protocol: String,
    pub pipeline_definition_id: String,
    pub pipeline_definition_version: u32,
    pub pipeline_definition_digest: Sha256Digest,
    pub run_id: RunId,
    pub node_path: String,
    pub stage_instance_id: StageInstanceId,
    pub parent_stage_instance_id: Option<StageInstanceId>,
    pub stage_protocol: String,
    pub component_version: String,
    pub component_digest: Sha256Digest,
    pub role_binding_id: Option<RoleBindingId>,
    pub attempt: u32,
    pub iteration: u32,
    pub message_kind: MessageKind,
    pub message_id: MessageId,
    pub causation_id: Option<MessageId>,
    pub report_type: String,
    pub subject_kind: String,
    pub subject_id: String,
    pub expected_scheduler_revision: Option<u64>,
    pub expected_stage_revision: Option<u64>,
    pub expected_slot_version: Option<u64>,
    pub input_manifest_digest: Sha256Digest,
    pub payload_digest: Sha256Digest,
    pub payload: Value,
    pub artifacts: Vec<ArtifactReference>,
}

#[derive(Debug)]
pub enum EnvelopeParseError {
    Json(serde_json::Error),
    InvalidIJson(canonical_json::CanonicalJsonError),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EnvelopeValidationError {
    UnsupportedProtocol,
    InvalidPipelineDefinitionVersion,
    InvalidAttempt,
    EmptyRequiredField(&'static str),
    AuthorityMismatch,
    AgentBindingMismatch,
    AgentReportTypeForbidden,
    RoleBindingForbidden,
    CommittedEventCannotBeSubmitted,
    NonCanonicalPayload(canonical_json::CanonicalJsonError),
    PayloadNotNormalized,
    PayloadDigestMismatch,
}

impl Envelope {
    /// Parses a wire envelope without collapsing duplicate JSON object names.
    ///
    /// `Envelope` intentionally does not implement `Deserialize`; all untrusted
    /// JSON must enter through this strict parser before typed validation.
    pub fn from_json_slice(input: &[u8]) -> Result<Self, EnvelopeParseError> {
        let strict =
            serde_json::from_slice::<StrictValue>(input).map_err(EnvelopeParseError::Json)?;
        let mut wire =
            serde_json::from_value::<WireEnvelope>(strict.0).map_err(EnvelopeParseError::Json)?;
        wire.payload =
            canonical_json::normalize(&wire.payload).map_err(EnvelopeParseError::InvalidIJson)?;
        Ok(wire.into())
    }

    pub fn validate_submission(
        &self,
        authority: SubmissionAuthority<'_>,
    ) -> Result<(), EnvelopeValidationError> {
        self.validate_common_fields()?;

        match (authority, self.message_kind) {
            (_, MessageKind::CommittedEvent) => {
                return Err(EnvelopeValidationError::CommittedEventCannotBeSubmitted);
            }
            (SubmissionAuthority::AgentCredential(context), MessageKind::AgentReport) => {
                self.validate_agent_binding(context)?;
            }
            (SubmissionAuthority::ProtectedHumanChannel, MessageKind::HumanCommand)
            | (SubmissionAuthority::InternalCoordinator, MessageKind::InternalCommand) => {
                if self.role_binding_id.is_some() {
                    return Err(EnvelopeValidationError::RoleBindingForbidden);
                }
            }
            _ => return Err(EnvelopeValidationError::AuthorityMismatch),
        }

        if !canonical_json::is_normalized(&self.payload) {
            return Err(EnvelopeValidationError::PayloadNotNormalized);
        }
        let normalized_payload = canonical_json::normalize(&self.payload)
            .map_err(EnvelopeValidationError::NonCanonicalPayload)?;
        let canonical_payload = canonical_json::to_vec(&normalized_payload)
            .map_err(EnvelopeValidationError::NonCanonicalPayload)?;
        if Sha256Digest::of_bytes(&canonical_payload) != self.payload_digest {
            return Err(EnvelopeValidationError::PayloadDigestMismatch);
        }

        Ok(())
    }

    fn validate_common_fields(&self) -> Result<(), EnvelopeValidationError> {
        if self.protocol != BASE_PROTOCOL {
            return Err(EnvelopeValidationError::UnsupportedProtocol);
        }
        if self.pipeline_definition_version == 0 {
            return Err(EnvelopeValidationError::InvalidPipelineDefinitionVersion);
        }
        if self.attempt == 0 {
            return Err(EnvelopeValidationError::InvalidAttempt);
        }
        for (name, value) in [
            (
                "pipeline_definition_id",
                self.pipeline_definition_id.as_str(),
            ),
            ("node_path", self.node_path.as_str()),
            ("stage_protocol", self.stage_protocol.as_str()),
            ("component_version", self.component_version.as_str()),
            ("report_type", self.report_type.as_str()),
            ("subject_kind", self.subject_kind.as_str()),
            ("subject_id", self.subject_id.as_str()),
        ] {
            if value.is_empty() {
                return Err(EnvelopeValidationError::EmptyRequiredField(name));
            }
        }
        Ok(())
    }

    fn validate_agent_binding(
        &self,
        context: &AuthenticatedAgentContext,
    ) -> Result<(), EnvelopeValidationError> {
        let binding_matches = self.role_binding_id.as_ref() == Some(&context.role_binding_id)
            && self.run_id == context.run_id
            && self.stage_instance_id == context.stage_instance_id
            && self.stage_protocol == context.stage_protocol
            && self.component_digest == context.component_digest
            && self.input_manifest_digest == context.input_manifest_digest;
        if !binding_matches {
            return Err(EnvelopeValidationError::AgentBindingMismatch);
        }
        if !context
            .allowed_report_types
            .iter()
            .any(|allowed| allowed == &self.report_type)
        {
            return Err(EnvelopeValidationError::AgentReportTypeForbidden);
        }
        Ok(())
    }
}

impl fmt::Display for EnvelopeParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Json(error) => error.fmt(formatter),
            Self::InvalidIJson(error) => error.fmt(formatter),
        }
    }
}

impl fmt::Display for EnvelopeValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedProtocol => formatter.write_str("unsupported base protocol"),
            Self::InvalidPipelineDefinitionVersion => {
                formatter.write_str("pipeline definition version must be positive")
            }
            Self::InvalidAttempt => formatter.write_str("attempt must be positive"),
            Self::EmptyRequiredField(field) => write!(formatter, "{field} cannot be empty"),
            Self::AuthorityMismatch => {
                formatter.write_str("credential cannot submit this message kind")
            }
            Self::AgentBindingMismatch => {
                formatter.write_str("agent credential is not bound to the claimed report context")
            }
            Self::AgentReportTypeForbidden => {
                formatter.write_str("agent credential cannot submit this report type")
            }
            Self::RoleBindingForbidden => {
                formatter.write_str("human and internal commands cannot use agent role bindings")
            }
            Self::CommittedEventCannotBeSubmitted => {
                formatter.write_str("committed events can only be emitted by the coordinator")
            }
            Self::NonCanonicalPayload(error) => error.fmt(formatter),
            Self::PayloadNotNormalized => {
                formatter.write_str("payload numbers must use normalized IEEE-754 values")
            }
            Self::PayloadDigestMismatch => formatter.write_str("payload digest does not match"),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireEnvelope {
    protocol: String,
    pipeline_definition_id: String,
    pipeline_definition_version: u32,
    pipeline_definition_digest: Sha256Digest,
    run_id: RunId,
    node_path: String,
    stage_instance_id: StageInstanceId,
    parent_stage_instance_id: Option<StageInstanceId>,
    stage_protocol: String,
    component_version: String,
    component_digest: Sha256Digest,
    role_binding_id: Option<RoleBindingId>,
    attempt: u32,
    iteration: u32,
    message_kind: MessageKind,
    message_id: MessageId,
    causation_id: Option<MessageId>,
    report_type: String,
    subject_kind: String,
    subject_id: String,
    expected_scheduler_revision: Option<u64>,
    expected_stage_revision: Option<u64>,
    expected_slot_version: Option<u64>,
    input_manifest_digest: Sha256Digest,
    payload_digest: Sha256Digest,
    payload: Value,
    artifacts: Vec<ArtifactReference>,
}

impl From<WireEnvelope> for Envelope {
    fn from(wire: WireEnvelope) -> Self {
        Self {
            protocol: wire.protocol,
            pipeline_definition_id: wire.pipeline_definition_id,
            pipeline_definition_version: wire.pipeline_definition_version,
            pipeline_definition_digest: wire.pipeline_definition_digest,
            run_id: wire.run_id,
            node_path: wire.node_path,
            stage_instance_id: wire.stage_instance_id,
            parent_stage_instance_id: wire.parent_stage_instance_id,
            stage_protocol: wire.stage_protocol,
            component_version: wire.component_version,
            component_digest: wire.component_digest,
            role_binding_id: wire.role_binding_id,
            attempt: wire.attempt,
            iteration: wire.iteration,
            message_kind: wire.message_kind,
            message_id: wire.message_id,
            causation_id: wire.causation_id,
            report_type: wire.report_type,
            subject_kind: wire.subject_kind,
            subject_id: wire.subject_id,
            expected_scheduler_revision: wire.expected_scheduler_revision,
            expected_stage_revision: wire.expected_stage_revision,
            expected_slot_version: wire.expected_slot_version,
            input_manifest_digest: wire.input_manifest_digest,
            payload_digest: wire.payload_digest,
            payload: wire.payload,
            artifacts: wire.artifacts,
        }
    }
}

struct StrictString(String);

impl<'de> Deserialize<'de> for StrictString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        canonical_json::validate_string(&value).map_err(de::Error::custom)?;
        Ok(Self(value))
    }
}

struct StrictValue(Value);

impl<'de> Deserialize<'de> for StrictValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(StrictValueVisitor)
    }
}

struct StrictValueVisitor;

impl<'de> Visitor<'de> for StrictValueVisitor {
    type Value = StrictValue;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON value without duplicate object names")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(StrictValue(Value::Bool(value)))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
        Ok(StrictValue(Value::Number(value.into())))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
        Ok(StrictValue(Value::Number(value.into())))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Number::from_f64(value)
            .map(Value::Number)
            .map(StrictValue)
            .ok_or_else(|| E::custom("non-finite JSON number"))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        canonical_json::validate_string(value).map_err(E::custom)?;
        Ok(StrictValue(Value::String(value.into())))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        canonical_json::validate_string(&value).map_err(E::custom)?;
        Ok(StrictValue(Value::String(value)))
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(StrictValue(Value::Null))
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(StrictValue(Value::Null))
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        StrictValue::deserialize(deserializer)
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::new();
        while let Some(value) = sequence.next_element::<StrictValue>()? {
            values.push(value.0);
        }
        Ok(StrictValue(Value::Array(values)))
    }

    fn visit_map<A>(self, mut object: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut values = Map::new();
        while let Some(StrictString(key)) = object.next_key::<StrictString>()? {
            if values.contains_key(&key) {
                return Err(de::Error::custom(format!(
                    "duplicate JSON object member: {key}"
                )));
            }
            let value = object.next_value::<StrictValue>()?;
            values.insert(key, value.0);
        }
        Ok(StrictValue(Value::Object(values)))
    }
}

#[cfg(test)]
mod tests {
    use alloc::{format, string::ToString, vec};

    use super::*;
    use serde_json::json;

    const ULID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
    const OTHER_ULID: &str = "01BX5ZZKBKACTAV9WEVGEMMVRZ";

    fn envelope(kind: MessageKind, with_role: bool) -> Envelope {
        let payload = json!({ "candidate_oid": "abc123", "decision": "approved" });
        let canonical = canonical_json::to_vec(&payload).unwrap();
        Envelope {
            protocol: BASE_PROTOCOL.into(),
            pipeline_definition_id: "software-change".into(),
            pipeline_definition_version: 1,
            pipeline_definition_digest: Sha256Digest::of_bytes(b"pipeline"),
            run_id: format!("flow_{ULID}").parse().unwrap(),
            node_path: "work_items/W003/review".into(),
            stage_instance_id: format!("stage_{ULID}").parse().unwrap(),
            parent_stage_instance_id: None,
            stage_protocol: "herdr.adversarial-review/v1".into(),
            component_version: "1.0.0".into(),
            component_digest: Sha256Digest::of_bytes(b"component"),
            role_binding_id: with_role.then(|| format!("role_{ULID}").parse().unwrap()),
            attempt: 1,
            iteration: 0,
            message_kind: kind,
            message_id: format!("msg_{ULID}").parse().unwrap(),
            causation_id: None,
            report_type: "REVIEW_DECISION".into(),
            subject_kind: "candidate-commit/v1".into(),
            subject_id: "commit:abc123".into(),
            expected_scheduler_revision: None,
            expected_stage_revision: None,
            expected_slot_version: Some(0),
            input_manifest_digest: Sha256Digest::of_bytes(b"manifest"),
            payload_digest: Sha256Digest::of_bytes(&canonical),
            payload,
            artifacts: Vec::new(),
        }
    }

    fn agent_context(report: &Envelope) -> AuthenticatedAgentContext {
        AuthenticatedAgentContext {
            run_id: report.run_id.clone(),
            stage_instance_id: report.stage_instance_id.clone(),
            role_binding_id: report.role_binding_id.clone().unwrap(),
            participant_principal_id: format!("principal_{ULID}").parse().unwrap(),
            agent_session_id: "session-1".into(),
            stage_protocol: report.stage_protocol.clone(),
            component_digest: report.component_digest,
            input_manifest_digest: report.input_manifest_digest,
            allowed_report_types: vec![report.report_type.clone()],
        }
    }

    #[test]
    fn accepts_only_the_message_kind_owned_by_the_authenticated_channel() {
        let report = envelope(MessageKind::AgentReport, true);
        let context = agent_context(&report);
        assert_eq!(
            report.validate_submission(SubmissionAuthority::AgentCredential(&context)),
            Ok(())
        );
        assert_eq!(
            envelope(MessageKind::HumanCommand, false)
                .validate_submission(SubmissionAuthority::AgentCredential(&context)),
            Err(EnvelopeValidationError::AuthorityMismatch)
        );
        assert_eq!(
            envelope(MessageKind::HumanCommand, false)
                .validate_submission(SubmissionAuthority::ProtectedHumanChannel),
            Ok(())
        );
        assert_eq!(
            envelope(MessageKind::InternalCommand, false)
                .validate_submission(SubmissionAuthority::InternalCoordinator),
            Ok(())
        );
    }

    #[test]
    fn rejects_submitted_committed_events_for_every_authority() {
        let report = envelope(MessageKind::AgentReport, true);
        let context = agent_context(&report);
        for authority in [
            SubmissionAuthority::AgentCredential(&context),
            SubmissionAuthority::ProtectedHumanChannel,
            SubmissionAuthority::InternalCoordinator,
        ] {
            assert_eq!(
                envelope(MessageKind::CommittedEvent, false).validate_submission(authority),
                Err(EnvelopeValidationError::CommittedEventCannotBeSubmitted)
            );
        }
    }

    #[test]
    fn binds_agent_credentials_to_the_exact_report_context() {
        let report = envelope(MessageKind::AgentReport, true);
        let mut wrong_role = agent_context(&report);
        wrong_role.role_binding_id = format!("role_{OTHER_ULID}").parse().unwrap();
        assert_eq!(
            report.validate_submission(SubmissionAuthority::AgentCredential(&wrong_role)),
            Err(EnvelopeValidationError::AgentBindingMismatch)
        );

        let mut wrong_run = agent_context(&report);
        wrong_run.run_id = format!("flow_{OTHER_ULID}").parse().unwrap();
        assert_eq!(
            report.validate_submission(SubmissionAuthority::AgentCredential(&wrong_run)),
            Err(EnvelopeValidationError::AgentBindingMismatch)
        );

        let mut forbidden_report = agent_context(&report);
        forbidden_report.allowed_report_types = vec!["PROGRESS_REPORTED".into()];
        assert_eq!(
            report.validate_submission(SubmissionAuthority::AgentCredential(&forbidden_report)),
            Err(EnvelopeValidationError::AgentReportTypeForbidden)
        );
    }

    #[test]
    fn forbids_role_bindings_on_human_and_internal_commands() {
        assert_eq!(
            envelope(MessageKind::HumanCommand, true)
                .validate_submission(SubmissionAuthority::ProtectedHumanChannel),
            Err(EnvelopeValidationError::RoleBindingForbidden)
        );
    }

    #[test]
    fn detects_payload_tampering() {
        let mut report = envelope(MessageKind::AgentReport, true);
        let context = agent_context(&report);
        report.payload = json!({ "candidate_oid": "different" });

        assert_eq!(
            report.validate_submission(SubmissionAuthority::AgentCredential(&context)),
            Err(EnvelopeValidationError::PayloadDigestMismatch)
        );
    }

    #[test]
    fn rejects_unknown_protocol_versions() {
        let mut report = envelope(MessageKind::AgentReport, true);
        let context = agent_context(&report);
        report.protocol = "herdr.flow/v2".into();

        assert_eq!(
            report.validate_submission(SubmissionAuthority::AgentCredential(&context)),
            Err(EnvelopeValidationError::UnsupportedProtocol)
        );
    }

    #[test]
    fn strict_parser_round_trips_a_complete_envelope() {
        let report = envelope(MessageKind::AgentReport, true);
        let json = serde_json::to_vec(&report).unwrap();

        assert_eq!(Envelope::from_json_slice(&json).unwrap(), report);
    }

    #[test]
    fn strict_parser_rejects_missing_and_unknown_header_fields() {
        let report = envelope(MessageKind::AgentReport, true);
        let mut value = serde_json::to_value(&report).unwrap();
        value.as_object_mut().unwrap().remove("stage_protocol");
        assert!(Envelope::from_json_slice(&serde_json::to_vec(&value).unwrap()).is_err());

        let mut value = serde_json::to_value(&report).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert("future_field".into(), json!(1));
        assert!(Envelope::from_json_slice(&serde_json::to_vec(&value).unwrap()).is_err());
    }

    #[test]
    fn strict_parser_rejects_plain_and_escaped_duplicate_names() {
        let report = envelope(MessageKind::AgentReport, true);
        let encoded = serde_json::to_string(&report).unwrap();
        let plain_duplicate = encoded.replace(
            "\"decision\":\"approved\"",
            "\"decision\":\"approved\",\"decision\":\"changes_requested\"",
        );
        let escaped_duplicate = encoded.replace(
            "\"decision\":\"approved\"",
            "\"decision\":\"approved\",\"\\u0064ecision\":\"changes_requested\"",
        );

        let plain_error = Envelope::from_json_slice(plain_duplicate.as_bytes()).unwrap_err();
        let escaped_error = Envelope::from_json_slice(escaped_duplicate.as_bytes()).unwrap_err();
        assert!(plain_error
            .to_string()
            .contains("duplicate JSON object member"));
        assert!(escaped_error
            .to_string()
            .contains("duplicate JSON object member"));
    }

    #[test]
    fn strict_parser_normalizes_colliding_numeric_spellings_before_exposure() {
        let mut lower = envelope(MessageKind::AgentReport, true);
        lower.payload = json!({ "value": 9_007_199_254_740_992_u64 });
        lower.payload_digest =
            Sha256Digest::of_bytes(&canonical_json::to_vec(&lower.payload).unwrap());
        let mut higher = envelope(MessageKind::AgentReport, true);
        higher.payload = json!({ "value": 9_007_199_254_740_993_u64 });
        higher.payload_digest =
            Sha256Digest::of_bytes(&canonical_json::to_vec(&higher.payload).unwrap());

        let lower = Envelope::from_json_slice(&serde_json::to_vec(&lower).unwrap()).unwrap();
        let higher = Envelope::from_json_slice(&serde_json::to_vec(&higher).unwrap()).unwrap();

        assert_eq!(lower.payload, higher.payload);
        assert_eq!(lower.payload_digest, higher.payload_digest);
        assert_eq!(lower.payload["value"].as_u64(), None);
    }

    #[test]
    fn strict_parser_normalizes_negative_and_positive_zero_before_exposure() {
        let mut positive = envelope(MessageKind::AgentReport, true);
        positive.payload = json!({ "value": 0.0 });
        positive.payload_digest =
            Sha256Digest::of_bytes(&canonical_json::to_vec(&positive.payload).unwrap());
        let mut negative = envelope(MessageKind::AgentReport, true);
        negative.payload = json!({ "value": -0.0 });
        negative.payload_digest =
            Sha256Digest::of_bytes(&canonical_json::to_vec(&negative.payload).unwrap());

        let positive = Envelope::from_json_slice(&serde_json::to_vec(&positive).unwrap()).unwrap();
        let negative = Envelope::from_json_slice(&serde_json::to_vec(&negative).unwrap()).unwrap();

        assert_eq!(positive.payload, negative.payload);
        assert_eq!(positive.payload_digest, negative.payload_digest);
        assert!(!negative.payload["value"]
            .as_f64()
            .unwrap()
            .is_sign_negative());
    }

    #[test]
    fn validation_rejects_programmatic_payloads_that_bypass_normalization() {
        for payload in [
            json!({ "value": 9_007_199_254_740_993_u64 }),
            json!({ "value": -0.0 }),
        ] {
            let mut report = envelope(MessageKind::AgentReport, true);
            report.payload = payload;
            report.payload_digest =
                Sha256Digest::of_bytes(&canonical_json::to_vec(&report.payload).unwrap());
            let context = agent_context(&report);

            assert_eq!(
                report.validate_submission(SubmissionAuthority::AgentCredential(&context)),
                Err(EnvelopeValidationError::PayloadNotNormalized)
            );
        }
    }

    #[test]
    fn strict_parser_rejects_literal_and_escaped_unicode_noncharacters() {
        let report = envelope(MessageKind::AgentReport, true);
        let encoded = serde_json::to_string(&report).unwrap();
        let cases = [
            encoded.replace("approved", "\u{fdd0}"),
            encoded.replace("approved", "\\ufdd0"),
            encoded.replace("\"decision\"", "\"\u{ffff}\""),
            encoded.replace("\"decision\"", "\"\\uffff\""),
        ];

        for invalid in cases {
            assert!(Envelope::from_json_slice(invalid.as_bytes())
                .unwrap_err()
                .to_string()
                .contains("Unicode noncharacters"));
        }
    }
}

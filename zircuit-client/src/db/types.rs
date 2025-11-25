use serde::{Deserialize, Serialize, Serializer};
use sqlx::types::chrono::{DateTime, Utc};
use sqlx::{FromRow, Type};
use std::collections::HashMap;
use strum;
use strum::{Display, EnumString};

#[derive(Debug, Clone, FromRow, PartialEq, Eq)]
pub struct ProofRequest {
    /// A unique identifier generated for the proof. UUID4 format.
    pub proof_id: String,

    /// The UTC datetime the proof request was submitted in ISO8601 format.
    pub request_created: DateTime<Utc>,

    /// The UTC datetime the proof request results were received in ISO8601 format.
    pub request_completed: Option<DateTime<Utc>>, // Changed to Option<DateTime<Utc>>

    /// Arbitrary metadata for the proof request that was specified at creation time.
    #[sqlx(json)]
    pub meta: Option<sqlx::types::Json<HashMap<String, String>>>,

    /// URL of the remote location where proof input.
    pub proof_input_url: String,

    /// URL of the remote location where proof is stored (i.e., S3)
    pub proof_url: Option<String>,

    /// Proof request job status
    pub status: JobStatus,

    /// Type of requested prover to be used for proving
    pub prover: ProverType,

    /// Proof request type (i.e. range, aggregate)
    pub request_type: RequestType,
}

impl ProofRequest {
    pub fn is_finalized(&self) -> bool {
        self.status == JobStatus::Completed || self.status == JobStatus::Failed
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Type, Default, PartialEq, Eq)]
#[sqlx(type_name = "smallint")]
#[repr(i16)]
pub enum JobStatus {
    #[default]
    Created = 0,
    Submitted = 1,
    Completed = 2,
    Failed = 3,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Type, PartialEq, Eq, Display, EnumString)]
#[sqlx(type_name = "text")]
pub enum ProverType {
    Sp1Cpu,
    Sp1Mock,
    Sp1Cuda,
    Sindri,
}

impl From<i16> for JobStatus {
    fn from(value: i16) -> Self {
        match value {
            0 => JobStatus::Created,
            1 => JobStatus::Submitted,
            2 => JobStatus::Completed,
            3 => JobStatus::Failed,
            _ => panic!("Invalid job status: {value}"),
        }
    }
}

#[derive(
    sqlx::Type, Debug, Copy, Clone, PartialEq, Eq, Default, Serialize, Deserialize, Display,
)]
#[sqlx(type_name = "smallint")]
#[repr(i16)]
pub enum RequestType {
    #[default]
    Range = 0,
    Aggregation = 1,
}

impl From<i16> for RequestType {
    fn from(value: i16) -> Self {
        match value {
            0 => RequestType::Range,
            1 => RequestType::Aggregation,
            _ => panic!("Invalid request type: {value}"),
        }
    }
}

// Manual implementation of Serialize to handle custom datetime serialization
impl Serialize for ProofRequest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeStruct;

        let mut state = serializer.serialize_struct("ProofRequest", 9)?;
        state.serialize_field("proof_id", &self.proof_id)?;
        state.serialize_field("request_created", &self.request_created.to_rfc3339())?;

        if let Some(completed) = &self.request_completed {
            state.serialize_field("request_completed", &completed.to_rfc3339())?;
        } else {
            state.serialize_field("request_completed", &None::<String>)?;
        }

        state.serialize_field("meta", &self.meta)?;
        state.serialize_field("proof_input_url", &self.proof_input_url)?;
        state.serialize_field("proof_url", &self.proof_url)?;
        state.serialize_field("status", &self.status)?;
        state.serialize_field("prover", &self.prover)?;
        state.serialize_field("request_type", &self.request_type)?;
        state.end()
    }
}

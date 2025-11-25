use crate::db::types::{ProofRequest, ProverType};
use crate::{JobStatus, RequestType};
use anyhow::Result;
use sqlx::PgPool;
use std::collections::HashMap;
use tracing::info;
use uuid::Uuid;

pub struct DriverDBClient {
    pool: PgPool,
}

impl DriverDBClient {
    pub async fn new(database_url: &str) -> Result<Self> {
        let pool = PgPool::connect(database_url).await?;

        info!("Database configured successfully.");

        Ok(DriverDBClient { pool })
    }

    /// used for testing as the pool is provided with sql::test
    #[cfg(test)]
    pub(crate) fn new_with_pool(pool: PgPool) -> Self {
        DriverDBClient { pool }
    }

    /// Inserts a request into the database.
    pub async fn insert_request(
        &self,
        proof_input_url: &str,
        meta: Option<HashMap<String, String>>,
        prover: ProverType,
        request_type: RequestType,
    ) -> Result<ProofRequest, sqlx::Error> {
        let proof_id = Uuid::new_v4(); // Generate a new UUID for the proof_id

        let meta_json: Option<serde_json::Value> = meta
            .map(serde_json::to_value)
            .transpose()
            .map_err(|_| sqlx::Error::InvalidArgument("Could not serialize meta".to_string()))?;

        let inserted_request = sqlx::query_as!(
            ProofRequest,
            r#"
            INSERT INTO proof_request_client.proof_requests (proof_id, request_created, proof_input_url, meta, status, prover, request_type)
            VALUES ($1, NOW(), $2, $3, $4, $5, $6)
            RETURNING
                proof_id,
                request_created,
                request_completed,
                meta as "meta: _", -- Type hint for optional JSONB column
                proof_input_url,
                proof_url,
                status as "status!: JobStatus", -- Type hint for enum mapping
                prover as "prover!: ProverType", -- Type hint for enum mapping
                request_type as "request_type!: RequestType" -- Type hint for enum mapping
            "#,
            proof_id,
            proof_input_url,
            meta_json,
            JobStatus::Created as i16, // Cast the enum variant to its underlying i16 value
            prover.to_string(),
            request_type as i16 // Cast the enum variant to its underlying i16 value
        )
            .fetch_one(&self.pool)
            .await?;

        Ok(inserted_request)
    }

    /// Fetches a request from the database.
    pub async fn fetch_request(&self, proof_uuid: &str) -> Result<ProofRequest, sqlx::Error> {
        let request_uuid = Uuid::parse_str(proof_uuid)
            .map_err(|e| sqlx::Error::InvalidArgument(format!("Invalid UUID format: {e}")))?;

        sqlx::query_as!(
            ProofRequest,
            r#"
            SELECT
                proof_id,
                request_created,
                request_completed,
                meta as "meta: _",
                proof_input_url,
                proof_url,
                status as "status!: JobStatus",
                prover as "prover!: ProverType",
                request_type as "request_type!: RequestType"
            FROM proof_request_client.proof_requests
            WHERE proof_id = $1
            "#,
            request_uuid
        )
        .fetch_one(&self.pool)
        .await
    }

    pub async fn update_request(
        &self,
        proof_uuid: &str,
        status: JobStatus,
        proof_url: Option<String>,
    ) -> Result<ProofRequest, sqlx::Error> {
        let request_uuid = Uuid::parse_str(proof_uuid)
            .map_err(|e| sqlx::Error::InvalidArgument(format!("Invalid UUID format: {e}")))?;

        sqlx::query_as!(
            ProofRequest,
            r#"
            UPDATE proof_request_client.proof_requests
            SET
                status = $1::smallint,
                proof_url = $2,
                request_completed = CASE
                    WHEN $1::smallint IN ($3, $4) THEN NOW()
                    ELSE request_completed
                END
            WHERE proof_id = $5
            RETURNING
                proof_id,
                request_created,
                request_completed,
                meta as "meta: _",
                proof_input_url,
                proof_url,
                status as "status!: JobStatus",
                prover as "prover!: ProverType", -- Type hint for enum mapping
                request_type as "request_type!: RequestType" -- Type hint for enum mapping
            "#,
            status as i16,
            proof_url,
            JobStatus::Completed as i16,
            JobStatus::Failed as i16,
            request_uuid
        )
        .fetch_one(&self.pool)
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::types::chrono::Utc;
    use sqlx::PgPool;
    use uuid::{Variant, Version};

    #[sqlx::test]
    async fn test_proof_insert(pool: PgPool) -> Result<(), sqlx::Error> {
        let driver = DriverDBClient { pool };

        let now_time = Utc::now();
        let request = driver
            .insert_request(
                "proof_input.json",
                None,
                ProverType::Sp1Cpu,
                RequestType::Range,
            )
            .await?;

        let request_uuid = Uuid::parse_str(&request.proof_id).expect("Could not parse UUID");
        assert_eq!(request_uuid.get_version(), Some(Version::Random));
        assert_eq!(request_uuid.get_variant(), Variant::RFC4122);
        assert!(request.request_created.timestamp() >= now_time.timestamp());
        assert!(request.request_completed.is_none());
        assert!(request.meta.is_none());
        assert_eq!(request.proof_input_url, "proof_input.json");
        assert!(request.proof_url.is_none());
        assert_eq!(request.prover, ProverType::Sp1Cpu);
        assert_eq!(request.prover.to_string(), "Sp1Cpu".to_string());
        assert_eq!(request.status, JobStatus::Created);

        Ok(())
    }

    #[sqlx::test]
    async fn test_proof_insert_with_meta(pool: PgPool) -> Result<(), sqlx::Error> {
        let driver = DriverDBClient { pool };

        let meta = HashMap::from([("key1".to_string(), "value1".to_string())]);

        let request = driver
            .insert_request(
                "proof_input.json",
                Some(meta.clone()),
                ProverType::Sp1Mock,
                RequestType::Range,
            )
            .await?;

        let request_meta = request.meta.expect("Request meta should be Some").0;
        assert_eq!(meta, request_meta);
        assert_eq!(request.prover.to_string(), "Sp1Mock".to_string());

        Ok(())
    }

    #[sqlx::test]
    async fn test_proof_request_fetch(pool: PgPool) -> Result<(), sqlx::Error> {
        let driver = DriverDBClient { pool };
        let request = driver
            .insert_request(
                "proof_input.json",
                None,
                ProverType::Sp1Cuda,
                RequestType::Range,
            )
            .await?;

        let request_fetched = driver.fetch_request(&request.proof_id).await?;

        assert_eq!(request.prover.to_string(), "Sp1Cuda".to_string());
        assert_eq!(request, request_fetched);

        Ok(())
    }

    #[sqlx::test]
    async fn test_fetch_non_existing_request(pool: PgPool) -> Result<(), sqlx::Error> {
        let driver = DriverDBClient { pool };

        let random_uuid = Uuid::new_v4();

        let request_fetched = driver.fetch_request(&random_uuid.to_string()).await;
        assert!(request_fetched.is_err());

        Ok(())
    }

    #[sqlx::test]
    async fn test_update_request_status_to_submitted(pool: PgPool) -> Result<(), sqlx::Error> {
        let driver = DriverDBClient { pool };

        // Insert a request
        let request = driver
            .insert_request(
                "proof_input.json",
                None,
                ProverType::Sp1Cpu,
                RequestType::Range,
            )
            .await?;

        // Update the status to Submitted (should not set request_completed)
        driver
            .update_request(&request.proof_id, JobStatus::Submitted, None)
            .await?;

        // Fetch and verify
        let updated_request = driver.fetch_request(&request.proof_id).await?;
        assert_eq!(updated_request.status, JobStatus::Submitted);
        assert!(updated_request.request_completed.is_none());
        assert!(updated_request.proof_url.is_none());

        // Update the status to Failed (should set request_completed)
        driver
            .update_request(&request.proof_id, JobStatus::Failed, None)
            .await?;
        let updated_request = driver.fetch_request(&request.proof_id).await?;
        assert_eq!(updated_request.status, JobStatus::Failed);
        assert!(updated_request.request_completed.is_some());
        assert!(updated_request.proof_url.is_none());

        Ok(())
    }

    #[sqlx::test]
    async fn test_update_request_status_to_completed_with_proof_url(
        pool: PgPool,
    ) -> Result<(), sqlx::Error> {
        let driver = DriverDBClient { pool };

        // Insert a request
        let request = driver
            .insert_request(
                "proof_input.json",
                None,
                ProverType::Sp1Cpu,
                RequestType::Range,
            )
            .await?;
        let proof_url = "s3://proof.json";

        let before_update = Utc::now();

        // Update the status to Completed (should set request_completed)
        driver
            .update_request(
                &request.proof_id,
                JobStatus::Completed,
                Some(proof_url.to_string()),
            )
            .await?;

        // Fetch and verify
        let updated_request = driver.fetch_request(&request.proof_id).await?;
        assert_eq!(updated_request.status, JobStatus::Completed);
        assert!(updated_request.request_completed.is_some());
        assert!(
            updated_request.request_completed.unwrap().timestamp() >= before_update.timestamp()
        );
        assert_eq!(updated_request.proof_url, Some(proof_url.to_string()));

        Ok(())
    }

    #[sqlx::test]
    async fn test_update_non_existing_request(pool: PgPool) -> Result<(), sqlx::Error> {
        let driver = DriverDBClient { pool };

        let random_uuid = Uuid::new_v4();

        let result = driver
            .update_request(&random_uuid.to_string(), JobStatus::Submitted, None)
            .await;
        assert!(result.is_err());

        Ok(())
    }
}

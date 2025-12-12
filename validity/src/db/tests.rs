#[cfg(test)]
mod db_tests {
    use alloy_primitives::{Address, B256};
    use anyhow::Result;
    use chrono::Utc;
    use std::str::FromStr;

    use crate::db::{DriverDBClient, OPSuccinctRequest, RequestMode, RequestStatus, RequestType};

    // DB Must be running. See README
    #[tokio::test]
    async fn test_insert_request() -> Result<()> {
        // Setup test database
        // let pool = setup_test_db().await;
        let database_url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://op-succinct@localhost:5432/op-succinct".to_string());
        let db_client = DriverDBClient::new(&database_url).await?;

        let preroot = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 22, 33, 44, 55];
        let postroot = vec![9, 8, 7, 6, 5, 4, 3, 2, 1, 0, 1, 2];

        // Create test data
        let now = Utc::now().naive_utc();
        let test_request = OPSuccinctRequest {
            id: 0, // Will be set by the database
            status: RequestStatus::Unrequested,
            req_type: RequestType::Range,
            mode: RequestMode::Real,
            start_block: 100,
            end_block: 200,
            created_at: now,
            updated_at: now,
            proof_request_id: None,
            proof_request_time: None,
            checkpointed_l1_block_number: Some(50),
            checkpointed_l1_block_hash: Some(
                B256::from_str(
                    "0x1234567890123456789012345678901234567890123456789012345678901234",
                )
                .unwrap()
                .to_vec(),
            ),
            execution_statistics: serde_json::json!({}),
            witnessgen_duration: None,
            execution_duration: None,
            prove_duration: None,
            range_vkey_commitment: B256::from_str(
                "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
            )
            .unwrap()
            .to_vec(),
            aggregation_vkey_hash: None,
            rollup_config_hash: B256::from_str(
                "0xfedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321",
            )
            .unwrap()
            .to_vec(),
            relay_tx_hash: None,
            proof: None,
            total_nb_transactions: 1000,
            total_eth_gas_used: 5000,
            total_l1_fees: "1000000000000000000".parse().unwrap(),
            total_tx_fees: "2000000000000000000".parse().unwrap(),
            l1_chain_id: 1,
            l2_chain_id: 2,
            contract_address: None,
            prover_address: Some(
                Address::from_str("0x1234567890123456789012345678901234567890")
                    .unwrap()
                    .to_vec(),
            ),
            l1_head_block_number: Some(45),
            preroot: Some(preroot.clone()),
            postroot: Some(postroot.clone()),
            contract_root: None,
            cost_exec_timeout_secs: None,
            max_exec_timeout_retries: Some(5),
        };

        // Insert the request
        let result = db_client.insert_request(&test_request).await;
        assert!(
            result.is_ok(),
            "Failed to insert request: {:?}",
            result.err()
        );

        // Fetch the request back from the database
        let fetched_requests = db_client
            .fetch_requests_by_status(
                RequestStatus::Unrequested,
                &crate::CommitmentConfig {
                    range_vkey_commitment: B256::from_slice(&test_request.range_vkey_commitment),
                    agg_vkey_hash: B256::from_slice(&test_request.rollup_config_hash), // Using rollup_config_hash as a placeholder
                    rollup_config_hash: B256::from_slice(&test_request.rollup_config_hash),
                },
                test_request.l1_chain_id,
                test_request.l2_chain_id,
            )
            .await
            .expect("Failed to fetch requests");

        // Verify we got exactly one request back
        assert_eq!(fetched_requests.len(), 1);
        let fetched_request = &fetched_requests[0];

        // Verify all fields match
        assert_eq!(fetched_request.status, test_request.status);
        assert_eq!(fetched_request.req_type, test_request.req_type);
        assert_eq!(fetched_request.mode, test_request.mode);
        assert_eq!(fetched_request.start_block, test_request.start_block);
        assert_eq!(fetched_request.end_block, test_request.end_block);
        assert_eq!(
            fetched_request.checkpointed_l1_block_number,
            test_request.checkpointed_l1_block_number
        );
        assert_eq!(
            fetched_request.checkpointed_l1_block_hash,
            test_request.checkpointed_l1_block_hash
        );
        assert_eq!(
            fetched_request.range_vkey_commitment,
            test_request.range_vkey_commitment
        );
        assert_eq!(
            fetched_request.rollup_config_hash,
            test_request.rollup_config_hash
        );
        assert_eq!(
            fetched_request.total_nb_transactions,
            test_request.total_nb_transactions
        );
        assert_eq!(
            fetched_request.total_eth_gas_used,
            test_request.total_eth_gas_used
        );
        assert_eq!(fetched_request.total_l1_fees, test_request.total_l1_fees);
        assert_eq!(fetched_request.total_tx_fees, test_request.total_tx_fees);
        assert_eq!(fetched_request.l1_chain_id, test_request.l1_chain_id);
        assert_eq!(fetched_request.l2_chain_id, test_request.l2_chain_id);
        assert_eq!(fetched_request.prover_address, test_request.prover_address);
        assert_eq!(
            fetched_request.l1_head_block_number,
            test_request.l1_head_block_number
        );
        assert_eq!(fetched_request.preroot, Some(preroot));
        assert_eq!(fetched_request.postroot, Some(postroot));

        Ok(())
    }
}

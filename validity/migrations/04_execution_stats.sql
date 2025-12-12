CREATE TABLE execution_stats (
    -- Same id as requests table
    id BIGSERIAL PRIMARY KEY,
    report_execution_duration_ms BIGINT,
    total_instruction_count BIGINT,
    oracle_verify_instruction_count BIGINT,
    derivation_instruction_count BIGINT,
    block_execution_instruction_count BIGINT,
    blob_verification_instruction_count BIGINT,
    cycles_per_block BIGINT,
    eth_gas_used BIGINT,
    eth_gas_used_per_block BIGINT,
    total_sp1_gas BIGINT,
    gas_used_per_block BIGINT,
    total_nb_transactions BIGINT,
    high_memory_threshold BIGINT
);
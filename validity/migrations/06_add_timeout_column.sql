-- Add NULLable cost_exec_timeout column to execution_stats table
ALTER TABLE requests
ADD COLUMN IF NOT EXISTS cost_exec_timeout_secs BIGINT,
ADD COLUMN IF NOT EXISTS max_exec_timeout_retries BIGINT
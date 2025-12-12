CREATE SCHEMA IF NOT EXISTS proof_request_client;

CREATE TABLE IF NOT EXISTS proof_request_client.proof_requests
(
    -- A unique identifier generated for the proof. Stored as UUID4.
    proof_id          UUID PRIMARY KEY NOT NULL,

    -- The UTC datetime the proof request was submitted.
    request_created   TIMESTAMPTZ      NOT NULL DEFAULT NOW(),

    -- The UTC datetime the proof request results were received.
    request_completed TIMESTAMPTZ,

    -- Arbitrary metadata for the proof request specified at creation time.
    meta              JSONB,

    -- URL of the remote location where proof input is stored (i.e., S3).
    proof_input_url   TEXT             NOT NULL,

    -- URL of the remote location where proof is stored (i.e., S3).
    proof_url         TEXT,

    --- Type of requested prover to be used for proving.
    prover            TEXT             NOT NULL,

    -- Proof request type (i.e., range, aggregate). Stored as a Rust enum that maps to i16 (smallint) in SQLx.
    request_type      SMALLINT         NOT NULL,

    -- The proof request job status. Stored as a Rust enum that maps to i16 (smallint) in SQLx.
    status            SMALLINT         NOT NULL
);

-- Add comments to the table and columns for better documentation
COMMENT ON TABLE proof_request_client.proof_requests IS 'Stores information about proof requests.';
COMMENT ON COLUMN proof_request_client.proof_requests.proof_id IS 'Unique identifier for the proof request (UUID4).';
COMMENT ON COLUMN proof_request_client.proof_requests.request_created IS 'UTC datetime when the proof request was submitted. Defaults to current time.';
COMMENT ON COLUMN proof_request_client.proof_requests.request_completed IS 'UTC datetime when the proof request results were received (optional).';
COMMENT ON COLUMN proof_request_client.proof_requests.meta IS 'Arbitrary metadata associated with the proof request.';
COMMENT ON COLUMN proof_request_client.proof_requests.proof_input_url IS 'URL of the remote location where proof input is stored.';
COMMENT ON COLUMN proof_request_client.proof_requests.proof_url IS 'URL to the remote location where the proof is stored.';
COMMENT ON COLUMN proof_request_client.proof_requests.prover IS 'Type of prover to be used for proving (maps to ProverType enum).';
COMMENT ON COLUMN proof_request_client.proof_requests.request_type IS 'Proof request type (i.e., range, aggregate) (maps to RequestType enum).';
COMMENT ON COLUMN proof_request_client.proof_requests.status IS 'Current job status of the proof request (maps to JobStatus enum).';

CREATE INDEX IF NOT EXISTS idx_proof_requests_status ON proof_request_client.proof_requests (status);
CREATE INDEX IF NOT EXISTS idx_proof_requests_created ON proof_request_client.proof_requests (request_created);

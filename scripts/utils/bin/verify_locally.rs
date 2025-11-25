use alloy_sol_types::SolValue;
use anyhow::Error;
use clap::Parser;
use op_succinct_client_utils::{boot::BootInfoStruct, AGGREGATION_OUTPUTS_SIZE};
use sindri::{client::SindriClient, integrations::sp1_v5::SP1ProofInfo, JobStatus};
use sp1_sdk::ProverClient;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the file containing the base64-encoded proof
    #[arg(required = true)]
    proof_id: String,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args = Args::parse();

    // make sure your sindri key is set in `SINDRI_API_KEY` env
    let client = SindriClient::default();
    let local_sp1_client = ProverClient::from_env();

    match client.get_proof(&args.proof_id, None, None, None).await {
        Ok(proof_info) => {
            match proof_info.status {
                JobStatus::Ready => {
                    let sp1_proof = proof_info.to_sp1_proof_with_public().unwrap();

                    // Ensures that circuit is properly configured so that Sindri sends back
                    // compatible proofs This should say "v4.0.0-rc.3"
                    println!("SP1 version: {:?}", sp1_proof.sp1_version);

                    // Displays the full proof
                    // You can match the "encoded_proof" field to the calldata sent to L2OO contract
                    println!("Proof: {:?}", sp1_proof.proof);

                    // Decode the aggregation outputs so that they are human readable.
                    // These are the public inputs as far as the proof knows them, they are not
                    // actually sent to the L2OO contract. So calldata should be carefully compared
                    // with the outputs below.
                    let mut public_values = sp1_proof.public_values.clone();
                    let mut raw_boot_info = [0u8; AGGREGATION_OUTPUTS_SIZE];
                    public_values.read_slice(&mut raw_boot_info);
                    let boot_info = BootInfoStruct::abi_decode(&raw_boot_info).unwrap();
                    println!("Aggregation outputs: {boot_info:?}");

                    // Attempt to verify the proof locally
                    // If this fails, then there is something corrupted within the groth16 proof
                    // itself
                    let verifying_key = proof_info.get_sp1_verifying_key().unwrap();
                    match local_sp1_client.verify(&sp1_proof, &verifying_key) {
                        Ok(_) => {
                            println!("Proof verified locally");
                        }
                        Err(e) => {
                            println!("Proof verification failed: {e:?}");
                        }
                    }
                }
                JobStatus::Failed => {
                    // If this fails, then there was some Sindri backend issue
                    // e.g. proof inputs were not properly formatted
                    println!("Proof {} failed", args.proof_id);
                }
                _ => {}
            }
        }
        Err(e) => {
            // If this fails, then there is a Sindri API key (credential) issue
            println!("Error getting proof {}: {}", args.proof_id, e);
        }
    }

    Ok(())
}

use std::{
    collections::HashMap,
    fs,
    io::{BufReader, Write},
    path::PathBuf,
};

use op_succinct_host_utils::fetcher::OPSuccinctDataFetcher;
use sindri::{client::SindriClient, JobStatus, ProofInput};
use sp1_sdk::SP1Stdin;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // make sure your sindri key is set in `SINDRI_API_KEY` env
    let client = SindriClient::default();

    // Get the path to the previously saved SP1Stdin files
    let data_fetcher = OPSuccinctDataFetcher::new_with_rollup_config().await?;
    let l2_chain_id = data_fetcher.get_l2_chain_id().await?;
    let cargo_metadata = cargo_metadata::MetadataCommand::new().exec().unwrap();
    let root_dir = PathBuf::from(cargo_metadata.workspace_root);
    let stdin_dir = root_dir.join(format!("execution-reports/{l2_chain_id}"));

    let mut proof_ids = Vec::new();
    if stdin_dir.exists() {
        for entry in fs::read_dir(&stdin_dir)? {
            let path = entry?.path();
            if path.to_string_lossy().ends_with("stdin.bin") {
                let filename = path.file_name().unwrap().to_string_lossy();
                let parts: Vec<&str> = filename.split('-').collect();
                let start_block = parts[0].parse::<u64>().unwrap();
                let end_block = parts[1].parse::<u64>().unwrap();

                let file = std::fs::File::open(&path)?;
                let reader = BufReader::new(file);
                let stdin: SP1Stdin = bincode::deserialize_from(reader)?;
                let proof_input = ProofInput::try_from(stdin).unwrap();
                println!(
                    "Sending request for proof start_block: {start_block}, end_block: {end_block}"
                );
                let mut meta = HashMap::new();
                meta.insert("start_block".to_string(), start_block.to_string());
                meta.insert("end_block".to_string(), end_block.to_string());

                let proof_info = client
                    .request_proof("range-elf-embedded", proof_input, Some(meta), None, None)
                    .await;

                match proof_info {
                    Ok(proof_info) => {
                        println!(
                            "Sindri Proof ID for {}: {:?}",
                            filename, proof_info.proof_id
                        );
                        proof_ids.push(proof_info.proof_id);
                    }
                    Err(e) => {
                        println!("Error: {e:?}");
                    }
                }
            }
        }

        let sindri_report_path =
            root_dir.join(format!("execution-reports/{l2_chain_id}/sindri-report.log"));

        println!("Waiting for {} proofs", proof_ids.len());
        // Check on the status of our range proofs every minute
        while !proof_ids.is_empty() {
            let mut to_remove = Vec::new();
            for proof_id in &proof_ids {
                match client.get_proof(proof_id, None, None, None).await {
                    Ok(proof_info) => match proof_info.status {
                        JobStatus::Ready => {
                            println!("Proof {proof_id} is ready");
                            fs::OpenOptions::new()
                                .append(true)
                                .create(true)
                                .open(&sindri_report_path)?
                                .write_all(
                                    format!("{}\n", serde_json::to_string(&proof_info)?).as_bytes(),
                                )?;
                            to_remove.push(proof_id.clone());
                        }
                        JobStatus::Failed => {
                            println!("Proof {proof_id} failed");
                            fs::OpenOptions::new()
                                .append(true)
                                .create(true)
                                .open(&sindri_report_path)?
                                .write_all(
                                    format!("{}\n", serde_json::to_string(&proof_info)?).as_bytes(),
                                )?;
                            to_remove.push(proof_id.clone());
                        }
                        _ => {}
                    },
                    Err(e) => {
                        println!("Error: {e:?}");
                    }
                }
            }
            // get rid of jobs that are finalized
            for id in to_remove {
                proof_ids.retain(|x| x != &id);
            }
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        }
    }

    Ok(())
}

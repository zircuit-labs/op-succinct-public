use alloy_primitives::{Address, B256};
use anyhow::Result;
use op_succinct_host_utils::{fetcher::OPSuccinctDataFetcher, OPSuccinctL2OutputOracle};

/// Get the latest proposed block number from the L2 output oracle.
pub async fn get_latest_proposed_block_number(
    address: Address,
    fetcher: &OPSuccinctDataFetcher,
) -> Result<u64> {
    let l2_output_oracle = OPSuccinctL2OutputOracle::new(address, fetcher.l1_provider.clone());
    let block_number = l2_output_oracle.latestBlockNumber().call().await?;

    // Convert the block number to a u64.
    let block_number = block_number.to::<u64>();
    Ok(block_number)
}

/// Get the latest proposed root from the L2 output oracle.
pub async fn get_latest_proposed_root(
    address: Address,
    fetcher: &OPSuccinctDataFetcher,
) -> Result<B256> {
    let l2_output_oracle = OPSuccinctL2OutputOracle::new(address, fetcher.l1_provider.clone());
    let latest_output_index = l2_output_oracle.latestOutputIndex().call().await?;
    let proposed_root: OPSuccinctL2OutputOracle::OutputProposal = l2_output_oracle
        .getL2Output(latest_output_index)
        .call()
        .await?;

    Ok(proposed_root.outputRoot)
}

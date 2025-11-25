use std::{fmt::Debug, sync::Arc};

use alloy_primitives::Address;
use anyhow::Result;
use async_trait::async_trait;
use kona_derive::{BlobProvider, ChainProvider, EthereumDataSource};
use kona_driver::PipelineCursor;
use kona_genesis::{L1ChainConfig, RollupConfig};
use kona_preimage::CommsClient;
use kona_proof::{
    l1::{OracleL1ChainProvider, OraclePipeline},
    l2::OracleL2ChainProvider,
    FlushableCache,
};
use op_succinct_client_utils::witness::executor::WitnessExecutor;
use spin::RwLock;

pub struct ETHDAWitnessExecutor<O, B>
where
    O: CommsClient + FlushableCache + Send + Sync + Debug,
    B: BlobProvider + Send + Sync + Debug + Clone,
{
    _marker: std::marker::PhantomData<(O, B)>,
}

#[allow(clippy::new_without_default)]
impl<O, B> ETHDAWitnessExecutor<O, B>
where
    O: CommsClient + FlushableCache + Send + Sync + Debug,
    B: BlobProvider + Send + Sync + Debug + Clone,
{
    pub fn new() -> Self {
        Self {
            _marker: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<O, B> WitnessExecutor for ETHDAWitnessExecutor<O, B>
where
    O: CommsClient + FlushableCache + Send + Sync + Debug,
    B: BlobProvider + Send + Sync + Debug + Clone,
{
    type O = O;
    type B = B;
    type L1 = OracleL1ChainProvider<Self::O>;
    type L2 = OracleL2ChainProvider<Self::O>;
    type DA = EthereumDataSource<Self::L1, Self::B>;

    async fn create_pipeline(
        &self,
        rollup_config: Arc<RollupConfig>,
        l1_config: Arc<L1ChainConfig>,
        cursor: Arc<RwLock<PipelineCursor>>,
        oracle: Arc<Self::O>,
        beacon: Self::B,
        mut l1_provider: Self::L1,
        l2_provider: Self::L2,
        pre_sender_address: Address,
        pre_nonce: u64,
        batch_sender_changed: bool,
    ) -> Result<OraclePipeline<Self::O, Self::L1, Self::L2, Self::DA>> {
        let da_provider = EthereumDataSource::new_from_parts(
            l1_provider.clone(),
            beacon,
            &rollup_config,
            (!batch_sender_changed).then_some((pre_sender_address, pre_nonce)),
        );

        let l1_blocks = if !batch_sender_changed {
            let origin_number = cursor.read().origin().number;
            Some(l1_provider.block_numbers(origin_number).await?)
        } else {
            None
        };

        Ok(OraclePipeline::new(
            rollup_config,
            l1_config,
            cursor,
            oracle,
            da_provider,
            l1_provider,
            l2_provider,
            l1_blocks,
        )
        .await?)
    }
}

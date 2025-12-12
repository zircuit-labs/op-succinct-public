Our main proposer entrypoint is `run` defined in `proposer.rs`: it's gonna call `run_loop_iteration` every 30 seconds (that's the default sleep config)

`run_loop_iteration` (also defined in `proposer.rs`) goes through a checklist:
* `validate_contract_config()`
* `log_proposer_metrics()`
* `handle_ongoing_tasks()`
* `set_orphaned_tasks_to_failed()`
* `handle_proving_requests()`
* `add_new_ranges()`
* `create_aggregation_proofs()`
* `request_queued_proofs()`
* `submit_agg_proofs()`
* `proof_requester.db_client.update_chain_lock(...)`


### If updating SQL table or adding to it, run the following commands to update the offline sqlx artifacts
```
docker network create kt-devnet
docker compose up postgres -d
export DATABASE_URL=postgres://op-succinct@localhost:5432/op-succinct
cargo sqlx migrate run
cargo sqlx prepare
```

### If roots are bogus we can run the following script to update the database block ranges to the correct pre and post roots
```
python3 ./scripts/update_db_roots.py --start-block 2049571
```

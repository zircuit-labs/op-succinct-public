# Zircuit Op-Succinct

This repository contains the source code for the Zircuit Op-Succinct proving
suite. It is a fork of the original
[Op-Succinct](https://github.com/matter-labs/op-succinct) repository with
modifications to support Zircuit.

For general information about Op-Succinct, see their original
[Docs](https://succinctlabs.github.io/op-succinct). It describes the overall
architecture, the deployment and configuration of the contracts and the usage of
some of the provided scripts.

Note that the contracts are not part of this repository but rather are found in
our
[zkr-monorepo-official](https://github.com/zircuit-labs/zkr-monorepo-official)
repository.

## Prerequisites

- [Rust toolchain](https://rust-lang.org/tools/install/) (Rust 1.90 nightly
  recommended)
- [SP1 toolchain](https://docs.succinct.xyz/docs/sp1/getting-started/install)
- [Foundry](https://book.getfoundry.sh/getting-started/installation)

## Programs

The programs for proving the execution and derivation of the L2 state
transitions and proof aggregation are found in the `programs` directory. The
compiled ELFs are found in the `elfs` directory.

Our programs used to generate the proofs are using our own fork of
[Kona](https://github.com/op-rs/kona) and can be found in this repository
[zr-kona-official](https://github.com/zircuit-labs/zr-kona-official).

### Computing program verifying keys

To compute the verifying keys for compiled ELFs, you can run the following
command:

```bash
cargo run -r --bin config-local
```

## Proving scripts

There are also scripts available for running the range and aggregation programs:

- `multi` — Runs the range program producing witnesses and running the range
  program.
- `agg` — Runs the aggregation program with provided proofs.

## Zircuit Bug Bounty Program

This repository is subject to the Zircuit Bug Bounty Program. Please see
[SECURITY.md](SECURITY.md) for more information.

## Contact Zircuit

We are happy to talk to you on [Discord](https://discord.gg/zircuit).

## Licensing

This repository is dual‑licensed under MIT and Apache‑2.0. Some optional
integrations may depend on third‑party services or crates under their own
licenses. Consult the root `LICENSE` files and third‑party notices for details.

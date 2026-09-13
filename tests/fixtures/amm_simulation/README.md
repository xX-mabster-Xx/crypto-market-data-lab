# AMM simulation parity fixtures

These fixtures verify that the Python canonical codec and the TypeScript
canonical codec produce identical SHA-256 hashes for the same economic
projection (spec section §6.2, AS37).

## Files

- `snapshot-parity-hashes.json` — golden hashes for the `amm_snapshot` domain.
  Each entry contains the protocol name, the expected hash, and the fields
  that differ from the base projection.  Both code paths compute the hash from
  the same projection dict and assert equality.

- `snapshot-parity-projection.json` — the base economic projection skeleton
  (synthetic CPMM) used by the parity test.  Protocol-specific variants are
  derived by overriding the `protocol`, `model_version`, `pool`, `pool_refs`,
  and `sdk_versions` fields.

## Generating new fixtures

```bash
PYTHONPATH=src .venv/bin/python -m market_data_lab.amm_simulation.cli \
    generate-parity-fixtures
```

This writes `snapshot-parity-hashes.json` from the Python `economic_projection`
of each supported pool type.  Verify with:

```bash
PYTHONPATH=src .venv/bin/python -m unittest tests.test_amm_simulation_parity
npm --prefix workers/solana-quote-worker test
```

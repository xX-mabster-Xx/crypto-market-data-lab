# AMM simulation fixtures

`amm-canonical.json` and `amm-canonical-hash.json` are the offline golden
fixture for the shared Python↔TypeScript canonical codec.  The payload uses
sorted object keys, decimal strings for economic amounts, explicit nulls, and
SHA-256 over the domain wrapper.

## Orca classic swap fixture

`orca-whirlpool-sdk-swap.json` is an offline golden fixture capturing the locked
`@orca-so/whirlpools-sdk` `0.22.0` classic (non-adaptive) swap path for a
synthetic pool (`tick_spacing=64`, `fee_rate=500`, `protocol_fee_rate=300`,
liquidity `100000000000000`, current tick `-6144` with three initialized ticks
below).  Generator: `workers/solana-quote-worker/test/amm/gen_orca_fixture.mjs`
(no network; built entirely from locally pinned SDK).  Exact-in and exact-out
(expected input) amounts, fee, end tick and end sqrt-price are recorded per
direction so the Python and TypeScript pure adapters are verified against a
measurement that is not produced by the implementation under test.

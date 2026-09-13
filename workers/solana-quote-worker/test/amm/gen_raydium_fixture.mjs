import { CurveCalculator } from "@raydium-io/raydium-sdk-v2";
import BN from "bn.js";

const cases = [];
const inputs = ["1000000", "987654321", "1"];
const reserves = [
  ["1000000000", "1000000000"],
  ["5000000000000", "2500000000000"],
];
const feeOns = [0, 1, 2];
for (const [ra, rb] of reserves) {
  for (const feeOn of feeOns) {
    for (const amount of inputs) {
      for (const aToB of [true, false]) {
        const creatorOnInput = feeOn === 0 ? true : (feeOn === 1 ? aToB : !aToB);
        const [rin, rout] = aToB ? [ra, rb] : [rb, ra];
        const r = CurveCalculator.swapBaseInput(
          new BN(amount), new BN(rin), new BN(rout),
          new BN(2500), new BN(120), new BN(120), new BN(40), creatorOnInput,
        );
        cases.push({
          reserve_a: ra, reserve_b: rb, fee_on: feeOn, a_to_b: aToB,
          input_amount_raw: amount,
          creator_fee_on_input: creatorOnInput,
          new_input_vault_amount: r.newInputVaultAmount.toString(10),
          new_output_vault_amount: r.newOutputVaultAmount.toString(10),
          output_amount_raw: r.outputAmount.toString(10),
          trade_fee_raw: r.tradeFee.toString(10),
          protocol_fee_raw: r.protocolFee.toString(10),
          fund_fee_raw: r.fundFee.toString(10),
          creator_fee_raw: r.creatorFee.toString(10),
        });
      }
    }
  }
}
console.log(JSON.stringify({ sdk: "@raydium-io/raydium-sdk-v2", trade_fee_rate: "2500", creator_fee_rate: "120", protocol_fee_rate: "120", fund_fee_rate: "40", cases }, null, 2));

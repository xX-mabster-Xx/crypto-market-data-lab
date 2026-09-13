/** Shared typed error for unsupported pool variants across AMM adapters.
 *
 * Every adapter re-exports this single class so ``instanceof`` checks in the
 * path executor and tests always succeed regardless of which adapter raised.
 */
export class UnsupportedVariant extends Error {}

export { UnsupportedVariant as UnsupportedPoolVariant };

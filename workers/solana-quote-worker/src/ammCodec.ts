/** Canonical economic encoding shared with the Python AMM simulation core. */

export function canonicalJson(value: unknown): string {
  return JSON.stringify(_canonicalValue(value));
}

export function canonicalHash(value: unknown, domain: string): string {
  if (domain.length === 0) throw new Error("canonical hash domain must be non-empty");
  return sha256Hex(canonicalJson({ domain, value }));
}

function _canonicalValue(value: unknown): unknown {
  if (value === null) return null;
  if (typeof value === "string" || typeof value === "boolean") return value;
  if (typeof value === "bigint") return value.toString(10);
  if (typeof value === "number") {
    if (!Number.isSafeInteger(value)) throw new Error("canonical numbers must be safe integers");
    return value;
  }
  if (Array.isArray(value)) return value.map(_canonicalValue);
  if (typeof value === "object") {
    const source = value as Record<string, unknown>;
    if (Object.keys(source).some((key) => source[key] === undefined)) {
      throw new Error("canonical objects must use explicit null instead of undefined");
    }
    return Object.fromEntries(
      Object.keys(source)
        .sort()
        .map((key) => [key, _canonicalValue(source[key])]),
    );
  }
  throw new Error(`unsupported canonical value type ${typeof value}`);
}

function sha256Hex(value: string): string {
  const bytes = new TextEncoder().encode(value);
  if (globalThis.crypto?.subtle === undefined) {
    throw new Error("canonical hashing requires WebCrypto");
  }
  return globalThis.crypto.subtle.digest("SHA-256", bytes).then((digest) => {
    return Array.from(new Uint8Array(digest))
      .map((byte) => byte.toString(16).padStart(2, "0"))
      .join("");
  }) as unknown as string;
}

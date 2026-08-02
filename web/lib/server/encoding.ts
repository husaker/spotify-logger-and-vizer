const encoder = new TextEncoder();
const decoder = new TextDecoder();

export function bytes(value: string): Uint8Array<ArrayBuffer> {
  return new Uint8Array(encoder.encode(value));
}

export function text(value: ArrayBuffer | Uint8Array): string {
  return decoder.decode(value);
}

export function base64Url(input: ArrayBuffer | Uint8Array): string {
  const data = input instanceof Uint8Array ? input : new Uint8Array(input);
  let binary = "";
  for (const byte of data) binary += String.fromCharCode(byte);
  return btoa(binary).replaceAll("+", "-").replaceAll("/", "_").replace(/=+$/, "");
}

export function fromBase64Url(value: string): Uint8Array<ArrayBuffer> {
  const padded = value.replaceAll("-", "+").replaceAll("_", "/").padEnd(Math.ceil(value.length / 4) * 4, "=");
  const binary = atob(padded);
  const output = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index += 1) output[index] = binary.charCodeAt(index);
  return output;
}

export function constantTimeEqual(left: string, right: string): boolean {
  const a = bytes(left);
  const b = bytes(right);
  let mismatch = a.length ^ b.length;
  const size = Math.max(a.length, b.length);
  for (let index = 0; index < size; index += 1) {
    mismatch |= (a[index % Math.max(1, a.length)] ?? 0) ^ (b[index % Math.max(1, b.length)] ?? 0);
  }
  return mismatch === 0;
}

export function randomToken(size = 24): string {
  return base64Url(crypto.getRandomValues(new Uint8Array(size)));
}

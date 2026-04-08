/**
 * Pongo decoder: decimal-aware erasure + Gorilla XOR decompression for double columns.
 *
 * Flag protocol (before each XOR value):
 *   "0"       (1 bit)  — erased, same dp as previous
 *   "10"      (2 bits) — not erased
 *   "11XXXXX" (7 bits) — erased, new dp (5 bits, 0-18)
 *
 * Wire format: [4 bytes count LE] [bitstream with interleaved flags + Gorilla XOR].
 */

import { BitReader } from './gorilla.js';

// Precomputed powers of 10
const MAX_DP = 18;
const POW10 = new Float64Array(MAX_DP + 1);
POW10[0] = 1;
for (let i = 1; i <= MAX_DP; i++) POW10[i] = POW10[i - 1] * 10;

type Bits64 = [number, number]; // [lo, hi]

export function decodePongo(data: Uint8Array, count: number): Float64Array {
  const r = new BitReader(data);

  const storedCount = r.readRawInt();
  if (storedCount !== count) {
    throw new Error(`Pongo count mismatch: header=${storedCount}, expected=${count}`);
  }

  const result = new Float64Array(count);
  const tmp = new Float64Array(1);
  const tmpU8 = new Uint8Array(tmp.buffer);

  // First value: raw 64 bits (no flag)
  let storedVal = r.readBits64();
  setBits(tmp, tmpU8, storedVal);
  result[0] = tmp[0];

  let storedLeadingZeros = 0x7fffffff;
  let storedTrailingZeros = 0;
  let lastDp = 0x7fffffff;

  for (let i = 1; i < count; i++) {
    // Read Pongo flag
    let erased: boolean;
    let dp = lastDp;

    const flag = r.readBit();
    if (flag === 0) {
      // "0" = erased, same dp
      erased = true;
    } else {
      const secondBit = r.readBit();
      if (secondBit === 0) {
        // "10" = not erased
        erased = false;
      } else {
        // "11XXXXX" = erased, new dp
        erased = true;
        dp = r.readBitsInt(5);
        lastDp = dp;
      }
    }

    // Gorilla XOR decompression
    if (r.readBit() === 0) {
      // Same value as previous — no change to storedVal
    } else {
      if (r.readBit() === 1) {
        // New window
        storedLeadingZeros = r.readBitsInt(5);
        let significantBits = r.readBitsInt(6);
        if (significantBits === 0) significantBits = 64;
        storedTrailingZeros = 64 - significantBits - storedLeadingZeros;
      }
      const significantBits = 64 - storedLeadingZeros - storedTrailingZeros;
      const valueBits = r.readBits64N(significantBits);
      const shifted = shiftLeft64(valueBits, storedTrailingZeros);
      storedVal = xor64(storedVal, shifted);
    }

    // Restore if erased
    if (erased) {
      setBits(tmp, tmpU8, storedVal);
      result[i] = restore(tmp[0], dp);
    } else {
      setBits(tmp, tmpU8, storedVal);
      result[i] = tmp[0];
    }
  }

  return result;
}

/** Restore original double from erased value via decimal rounding */
function restore(erased: number, dp: number): number {
  if (dp <= 0) return erased;
  const abs = Math.abs(erased);
  const scaled = Math.round(abs * POW10[dp]);
  let restored = scaled / POW10[dp];
  if (erased < 0) restored = -restored;
  return restored;
}

// --- 64-bit helpers (same as gorilla.ts) ---

function setBits(f64: Float64Array, u8: Uint8Array, bits: Bits64): void {
  u8[0] = bits[0] & 0xff;
  u8[1] = (bits[0] >>> 8) & 0xff;
  u8[2] = (bits[0] >>> 16) & 0xff;
  u8[3] = (bits[0] >>> 24) & 0xff;
  u8[4] = bits[1] & 0xff;
  u8[5] = (bits[1] >>> 8) & 0xff;
  u8[6] = (bits[1] >>> 16) & 0xff;
  u8[7] = (bits[1] >>> 24) & 0xff;
}

function xor64(a: Bits64, b: Bits64): Bits64 {
  return [(a[0] ^ b[0]) >>> 0, (a[1] ^ b[1]) >>> 0];
}

function shiftLeft64(v: Bits64, n: number): Bits64 {
  if (n === 0) return v;
  if (n >= 32) return [0, (v[0] << (n - 32)) >>> 0];
  return [
    (v[0] << n) >>> 0,
    ((v[1] << n) | (v[0] >>> (32 - n))) >>> 0,
  ];
}

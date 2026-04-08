/**
 * Gorilla XOR decompression for double columns.
 *
 * Implements the value decompression from the Facebook Gorilla paper (VLDB 2015).
 * Wire format: [4 bytes count LE] [bitstream].
 */
export function decodeGorilla(data: Uint8Array, count: number): Float64Array {
  const r = new BitReader(data);

  const storedCount = r.readRawInt();
  if (storedCount !== count) {
    throw new Error(`Gorilla count mismatch: header=${storedCount}, expected=${count}`);
  }

  const result = new Float64Array(count);
  const tmp = new Float64Array(1);
  const tmpBytes = new Uint8Array(tmp.buffer);

  // First value: raw 64 bits
  let storedVal = r.readBits64();
  setBits(tmp, tmpBytes, storedVal);
  result[0] = tmp[0];

  let storedLeadingZeros = 0x7fffffff;
  let storedTrailingZeros = 0;

  for (let i = 1; i < count; i++) {
    if (r.readBit() === 0) {
      // Same value
      result[i] = tmp[0];
    } else {
      if (r.readBit() === 1) {
        // New window: 5-bit leading + 6-bit significantBits
        storedLeadingZeros = r.readBitsInt(5);
        let significantBits = r.readBitsInt(6);
        if (significantBits === 0) significantBits = 64;
        storedTrailingZeros = 64 - significantBits - storedLeadingZeros;
      }
      // Read significant bits
      const significantBits = 64 - storedLeadingZeros - storedTrailingZeros;
      const valueBits = r.readBits64N(significantBits);
      // Shift left by trailingZeros and XOR
      const shifted = shiftLeft64(valueBits, storedTrailingZeros);
      storedVal = xor64(storedVal, shifted);
      setBits(tmp, tmpBytes, storedVal);
      result[i] = tmp[0];
    }
  }

  return result;
}

// --- 64-bit operations using two 32-bit halves [lo, hi] ---

type Bits64 = [number, number]; // [lo (uint32), hi (uint32)]

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
  if (n >= 32) return [(0) >>> 0, (v[0] << (n - 32)) >>> 0];
  return [
    (v[0] << n) >>> 0,
    ((v[1] << n) | (v[0] >>> (32 - n))) >>> 0,
  ];
}

/** Bit-level reader for Gorilla bitstream */
export class BitReader {
  private buf: Uint8Array;
  private bytePos: number;
  private bitPos: number; // bits remaining in current byte (8=fresh)

  constructor(data: Uint8Array) {
    this.buf = data;
    this.bytePos = 0;
    this.bitPos = 8;
  }

  readRawInt(): number {
    const v =
      this.buf[this.bytePos] |
      (this.buf[this.bytePos + 1] << 8) |
      (this.buf[this.bytePos + 2] << 16) |
      (this.buf[this.bytePos + 3] << 24);
    this.bytePos += 4;
    this.bitPos = 8;
    return v >>> 0;
  }

  readBit(): number {
    const bit = (this.buf[this.bytePos] >>> (this.bitPos - 1)) & 1;
    this.bitPos--;
    if (this.bitPos === 0) {
      this.bytePos++;
      this.bitPos = 8;
    }
    return bit;
  }

  /** Read up to 32 bits as an unsigned integer */
  readBitsInt(n: number): number {
    let result = 0;
    for (let i = 0; i < n; i++) {
      result = (result << 1) | this.readBit();
    }
    return result >>> 0;
  }

  /** Read exactly 64 bits as Bits64 [lo, hi] */
  readBits64(): Bits64 {
    const hi = this.readBitsInt(32);
    const lo = this.readBitsInt(32);
    return [lo >>> 0, hi >>> 0];
  }

  /** Read N bits (0-64) as Bits64 */
  readBits64N(n: number): Bits64 {
    if (n === 0) return [0, 0];
    if (n <= 32) {
      const lo = this.readBitsInt(n);
      return [lo >>> 0, 0];
    }
    const hi = this.readBitsInt(n - 32);
    const lo = this.readBitsInt(32);
    return [lo >>> 0, hi >>> 0];
  }
}

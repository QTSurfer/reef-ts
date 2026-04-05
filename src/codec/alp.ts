/**
 * ALP (Adaptive Lossless floating-Point) decoder.
 * Reads the wire format written by AlpCompressor (Java).
 *
 * Wire format:
 *   [4 bytes] total count (LE int)
 *   Per vector (up to 1024 values):
 *     [1 byte]  e (exponent)
 *     [1 byte]  f (factor)
 *     [1 byte]  bitWidth
 *     [8 bytes] frame (LE long)
 *     [2 bytes] exceptionCount (LE short)
 *     [2 bytes] packedLength (LE short)
 *     [N bytes] bit-packed deltas
 *     Per exception:
 *       [2 bytes] index (LE short)
 *       [8 bytes] original bits (LE long)
 */

const VECTOR_SIZE = 1024;

/** Precomputed powers of 10 */
const POW10 = new Float64Array(19);
POW10[0] = 1;
for (let i = 1; i < 19; i++) POW10[i] = POW10[i - 1] * 10;

export function decodeAlp(data: Uint8Array): Float64Array {
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  let pos = 0;

  // ALP internal format uses big-endian (Java DataOutputStream)
  const count = view.getInt32(pos, false);
  pos += 4;

  const result = new Float64Array(count);
  let offset = 0;

  while (offset < count) {
    const vectorSize = Math.min(VECTOR_SIZE, count - offset);

    const e = data[pos++];
    const f = data[pos++];
    const bitWidth = data[pos++];

    // Frame: 8 bytes BE (Java DataOutputStream.writeLong)
    const frameHi = view.getInt32(pos, false);
    const frameLo = view.getUint32(pos + 4, false);
    const frame = frameHi * 0x100000000 + frameLo;
    pos += 8;

    const exceptionCount = view.getUint16(pos, false);
    pos += 2;

    const packedLength = view.getUint16(pos, false);
    pos += 2;

    // Unpack bit-packed deltas
    const packed = data.subarray(pos, pos + packedLength);
    pos += packedLength;

    // Decode: unpack + add frame + ALP decode
    const factor10 = POW10[f];
    const exponent10 = POW10[e];

    for (let i = 0; i < vectorSize; i++) {
      const delta = readBits(packed, i * bitWidth, bitWidth);
      const encoded = frame + delta;
      result[offset + i] = (encoded * factor10) / exponent10;
    }

    // Apply exceptions (BE format from Java)
    for (let j = 0; j < exceptionCount; j++) {
      const idx = view.getUint16(pos, false);
      pos += 2;
      // Read original double bits as float64 (BE)
      result[offset + idx] = view.getFloat64(pos, false);
      pos += 8;
    }

    offset += vectorSize;
  }

  return result;
}

function readBits(buf: Uint8Array, bitPos: number, bitWidth: number): number {
  if (bitWidth === 0) return 0;

  let byteIdx = bitPos >> 3;
  let bitOffset = bitPos & 7;
  let result = 0;
  let bitsRead = 0;

  while (bitsRead < bitWidth) {
    const bitsToRead = Math.min(8 - bitOffset, bitWidth - bitsRead);
    const mask = (1 << bitsToRead) - 1;
    result |= ((buf[byteIdx] >>> bitOffset) & mask) << bitsRead;
    bitsRead += bitsToRead;
    bitOffset = 0;
    byteIdx++;
  }

  return result;
}

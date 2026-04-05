/**
 * Delta-of-delta + zigzag varint decoder for long columns (timestamps).
 * Uses BigInt internally for precision with large values (nanosecond timestamps),
 * then converts to Float64Array for output.
 */
export function decodeDeltaVarint(data: Uint8Array, count: number): Float64Array {
  if (count === 0) return new Float64Array(0);

  const result = new Float64Array(count);
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  let pos = 0;

  // First value: 8 bytes LE as BigInt
  const first = view.getBigInt64(pos, true);
  result[0] = Number(first);
  pos += 8;

  if (count > 1) {
    let [prevDelta, newPos] = readZigzagVarintBig(data, pos);
    pos = newPos;
    let current = first + prevDelta;
    result[1] = Number(current);

    for (let i = 2; i < count; i++) {
      let dod: bigint;
      [dod, pos] = readZigzagVarintBig(data, pos);
      prevDelta = prevDelta + dod;
      current = current + prevDelta;
      result[i] = Number(current);
    }
  }

  return result;
}

function readZigzagVarintBig(data: Uint8Array, pos: number): [bigint, number] {
  let result = 0n;
  let shift = 0n;
  while (true) {
    const b = data[pos++];
    result |= BigInt(b & 0x7f) << shift;
    if ((b & 0x80) === 0) break;
    shift += 7n;
  }
  // Zigzag decode: (n >>> 1) ^ -(n & 1)
  return [(result >> 1n) ^ -(result & 1n), pos];
}

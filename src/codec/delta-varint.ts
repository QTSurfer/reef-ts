/**
 * Delta-of-delta + zigzag varint decoder for long columns (timestamps).
 * Values are stored as: first value (8 bytes LE) + first delta (zigzag varint) +
 * subsequent delta-of-deltas (zigzag varints).
 */
export function decodeDeltaVarint(data: Uint8Array, count: number): Float64Array {
  if (count === 0) return new Float64Array(0);

  const result = new Float64Array(count);
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  let pos = 0;

  // First value: 8 bytes LE (read as two 32-bit ints to stay in Number precision)
  const lo = view.getUint32(pos, true);
  const hi = view.getInt32(pos + 4, true);
  result[0] = hi * 0x100000000 + lo;
  pos += 8;

  if (count > 1) {
    let [prevDelta, newPos] = readZigzagVarint(data, pos);
    pos = newPos;
    result[1] = result[0] + prevDelta;

    for (let i = 2; i < count; i++) {
      let dod: number;
      [dod, pos] = readZigzagVarint(data, pos);
      const delta = prevDelta + dod;
      result[i] = result[i - 1] + delta;
      prevDelta = delta;
    }
  }

  return result;
}

function readZigzagVarint(data: Uint8Array, pos: number): [number, number] {
  let result = 0;
  let shift = 0;
  while (true) {
    const b = data[pos++];
    result |= (b & 0x7f) << shift;
    if ((b & 0x80) === 0) break;
    shift += 7;
  }
  // Zigzag decode
  return [(result >>> 1) ^ -(result & 1), pos];
}

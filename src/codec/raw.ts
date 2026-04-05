/** Decode raw LE-encoded longs (as Float64Array for JS compatibility) */
export function decodeRawLongs(data: Uint8Array, count: number): Float64Array {
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const result = new Float64Array(count);
  for (let i = 0; i < count; i++) {
    const lo = view.getUint32(i * 8, true);
    const hi = view.getInt32(i * 8 + 4, true);
    result[i] = hi * 0x100000000 + lo;
  }
  return result;
}

/** Decode raw LE-encoded doubles (zero-copy when possible) */
export function decodeRawDoubles(data: Uint8Array, count: number): Float64Array {
  // If aligned, zero-copy
  if (data.byteOffset % 8 === 0) {
    return new Float64Array(data.buffer, data.byteOffset, count);
  }
  // Otherwise copy
  const result = new Float64Array(count);
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  for (let i = 0; i < count; i++) {
    result[i] = view.getFloat64(i * 8, true);
  }
  return result;
}

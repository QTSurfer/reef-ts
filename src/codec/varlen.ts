/**
 * Variable-length binary codec decoder.
 * Supports NONE, ZSTD, and GZIP block compression.
 */
import { decompress as zstdDecompress } from 'fzstd';

const COMPRESSION_NONE = 0;
const COMPRESSION_ZSTD = 1;
const COMPRESSION_GZIP = 2;

export function decodeVarlen(data: Uint8Array, count: number): (Uint8Array | null)[] {
  let pos = 0;
  const compression = data[pos++];

  let payload: Uint8Array;

  if (compression === COMPRESSION_NONE) {
    const len = readInt32LE(data, pos);
    pos += 4;
    payload = data.subarray(pos, pos + len);
  } else if (compression === COMPRESSION_ZSTD) {
    const _uncompressedLen = readInt32LE(data, pos);
    pos += 4;
    const compressedLen = readInt32LE(data, pos);
    pos += 4;
    const compressed = data.subarray(pos, pos + compressedLen);
    payload = zstdDecompress(compressed);
  } else if (compression === COMPRESSION_GZIP) {
    const _uncompressedLen = readInt32LE(data, pos);
    pos += 4;
    const compressedLen = readInt32LE(data, pos);
    pos += 4;
    const compressed = data.subarray(pos, pos + compressedLen);
    payload = gunzipSync(compressed);
  } else {
    throw new Error(`Unknown varlen compression: ${compression}`);
  }

  // Parse values from payload
  const result: (Uint8Array | null)[] = new Array(count);
  let pPos = 0;
  const pView = new DataView(payload.buffer, payload.byteOffset, payload.byteLength);

  for (let i = 0; i < count; i++) {
    const len = pView.getInt32(pPos, true);
    pPos += 4;
    if (len < 0) {
      result[i] = null;
    } else {
      result[i] = payload.slice(pPos, pPos + len);
      pPos += len;
    }
  }

  return result;
}

/**
 * Synchronous gzip is not available in browsers.
 * Use {@link decodeVarlenAsync} for gzip-compressed columns.
 */
function gunzipSync(_data: Uint8Array): Uint8Array {
  throw new Error('Synchronous gzip not supported — use decodeVarlenAsync() instead');
}

/** Async gzip decompression using native DecompressionStream (browser) */
export async function decodeVarlenAsync(
  data: Uint8Array,
  count: number,
): Promise<(Uint8Array | null)[]> {
  let pos = 0;
  const compression = data[pos++];

  let payload: Uint8Array;

  if (compression === COMPRESSION_NONE) {
    const len = readInt32LE(data, pos);
    pos += 4;
    payload = data.subarray(pos, pos + len);
  } else if (compression === COMPRESSION_ZSTD) {
    const _uncompressedLen = readInt32LE(data, pos);
    pos += 4;
    const compressedLen = readInt32LE(data, pos);
    pos += 4;
    payload = zstdDecompress(data.subarray(pos, pos + compressedLen));
  } else if (compression === COMPRESSION_GZIP) {
    const _uncompressedLen = readInt32LE(data, pos);
    pos += 4;
    const compressedLen = readInt32LE(data, pos);
    pos += 4;
    const compressed = data.subarray(pos, pos + compressedLen);
    const ds = new DecompressionStream('gzip');
    const writer = ds.writable.getWriter();
    writer.write(compressed as unknown as BufferSource);
    writer.close();
    const reader = ds.readable.getReader();
    const chunks: Uint8Array[] = [];
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      chunks.push(value);
    }
    const totalLen = chunks.reduce((s, c) => s + c.length, 0);
    payload = new Uint8Array(totalLen);
    let offset = 0;
    for (const chunk of chunks) {
      payload.set(chunk, offset);
      offset += chunk.length;
    }
  } else {
    throw new Error(`Unknown varlen compression: ${compression}`);
  }

  const result: (Uint8Array | null)[] = new Array(count);
  let pPos = 0;
  const pView = new DataView(payload.buffer, payload.byteOffset, payload.byteLength);

  for (let i = 0; i < count; i++) {
    const len = pView.getInt32(pPos, true);
    pPos += 4;
    if (len < 0) {
      result[i] = null;
    } else {
      result[i] = payload.slice(pPos, pPos + len);
      pPos += len;
    }
  }

  return result;
}

function readInt32LE(data: Uint8Array, pos: number): number {
  return data[pos] | (data[pos + 1] << 8) | (data[pos + 2] << 16) | (data[pos + 3] << 24);
}

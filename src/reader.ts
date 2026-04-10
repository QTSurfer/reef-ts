import {
  MAGIC,
  FOOTER_MAGIC,
  FLAG_HAS_EVENTS,
  FLAG_HAS_FOOTER,
  FLAG_HAS_CHECKSUMS,
  DataType,
  Codec,
} from './constants.js';
import { decodeDeltaVarint } from './codec/delta-varint.js';
import { decodeAlp } from './codec/alp.js';
import { decodeVarlen, decodeVarlenAsync } from './codec/varlen.js';
import { decodeRawLongs, decodeRawDoubles } from './codec/raw.js';
import { decodeGorilla } from './codec/gorilla.js';
import { decodePongo } from './codec/pongo.js';

export interface ColumnInfo {
  name: string;
  dataType: DataType;
  codec: Codec;
  metadata: Record<string, string>;
}

interface ColumnLocation {
  pos: number;
  len: number;
}

/**
 * Lastra file reader for TypeScript/JavaScript.
 *
 * Reads `.lastra` files written by the Java ReefWriter.
 * Supports selective column access — only requested columns are decompressed.
 * Verifies per-column CRC32 checksums when present (FLAG_HAS_CHECKSUMS).
 *
 * @example
 * ```ts
 * const reader = new LastraReader(buffer);
 * const ts = reader.readSeriesLong('ts');
 * const close = reader.readSeriesDouble('close');
 * const meta = reader.getSeriesColumn('ema1').metadata;
 * ```
 */
export class LastraReader {
  private readonly data: Uint8Array;
  private readonly view: DataView;
  readonly seriesRowCount: number;
  readonly eventsRowCount: number;
  readonly seriesColumns: ColumnInfo[];
  readonly eventColumns: ColumnInfo[];
  readonly hasChecksums: boolean;
  private readonly seriesLocs: ColumnLocation[];
  private readonly eventLocs: ColumnLocation[];
  private readonly seriesCrcs: number[];
  private readonly eventCrcs: number[];

  constructor(buffer: ArrayBuffer | Uint8Array) {
    this.data = buffer instanceof Uint8Array ? buffer : new Uint8Array(buffer);
    this.view = new DataView(this.data.buffer, this.data.byteOffset, this.data.byteLength);

    let pos = 0;

    // Header
    const magic = this.view.getUint32(pos, true);
    pos += 4;
    if (magic !== MAGIC) {
      throw new Error(`Not a Lastra file (magic: 0x${magic.toString(16)})`);
    }
    const version = this.view.getUint16(pos, true);
    pos += 2;
    if (version > 1) {
      throw new Error(`Unsupported Reef version: ${version}`);
    }
    const flags = this.view.getUint16(pos, true);
    pos += 2;
    this.seriesRowCount = this.view.getInt32(pos, true);
    pos += 4;
    const seriesColCount = this.view.getInt32(pos, true);
    pos += 4;
    this.eventsRowCount = this.view.getInt32(pos, true);
    pos += 4;
    const eventColCount = this.view.getUint16(pos, true);
    pos += 2;

    // Column descriptors
    const [seriesCols, newPos1] = this.readColumnDescriptors(pos, seriesColCount);
    this.seriesColumns = seriesCols;
    pos = newPos1;

    const hasEvents = (flags & FLAG_HAS_EVENTS) !== 0;
    if (hasEvents && eventColCount > 0) {
      const [eventCols, newPos2] = this.readColumnDescriptors(pos, eventColCount);
      this.eventColumns = eventCols;
      pos = newPos2;
    } else {
      this.eventColumns = [];
    }

    // Column data locations
    this.seriesLocs = [];
    for (let i = 0; i < seriesColCount; i++) {
      const len = this.view.getInt32(pos, true);
      pos += 4;
      this.seriesLocs.push({ pos, len });
      pos += len;
    }
    this.eventLocs = [];
    for (let i = 0; i < this.eventColumns.length; i++) {
      const len = this.view.getInt32(pos, true);
      pos += 4;
      this.eventLocs.push({ pos, len });
      pos += len;
    }

    // Footer: offsets + optional CRCs
    this.hasChecksums = (flags & FLAG_HAS_CHECKSUMS) !== 0;
    this.seriesCrcs = [];
    this.eventCrcs = [];

    const hasFooter = (flags & FLAG_HAS_FOOTER) !== 0;
    if (hasFooter) {
      const totalCols = seriesColCount + this.eventColumns.length;
      let footerInts = totalCols; // offsets
      if (this.hasChecksums) footerInts += totalCols; // CRCs
      footerInts += 1; // REF! magic

      const footerStart = this.data.byteLength - footerInts * 4;
      const fv = new DataView(this.data.buffer, this.data.byteOffset + footerStart, footerInts * 4);
      let fp = 0;

      // Skip offsets (we already computed positions by scanning)
      fp += totalCols * 4;

      // Read CRCs
      if (this.hasChecksums) {
        for (let i = 0; i < seriesColCount; i++) {
          this.seriesCrcs.push(fv.getUint32(fp, true));
          fp += 4;
        }
        for (let i = 0; i < this.eventColumns.length; i++) {
          this.eventCrcs.push(fv.getUint32(fp, true));
          fp += 4;
        }
      }

      const footerMagic = fv.getUint32(fp, true);
      if (footerMagic !== FOOTER_MAGIC) {
        throw new Error('Invalid Lastra footer');
      }
    }
  }

  // --- Series readers ---

  readSeriesLong(name: string): Float64Array {
    const [idx, col] = this.findColumn(this.seriesColumns, name);
    const loc = this.seriesLocs[idx];
    this.verifyCrc(loc, this.seriesCrcs, idx, name);
    return this.decodeLong(loc, this.seriesRowCount, col.codec);
  }

  readSeriesDouble(name: string): Float64Array {
    const [idx, col] = this.findColumn(this.seriesColumns, name);
    const loc = this.seriesLocs[idx];
    this.verifyCrc(loc, this.seriesCrcs, idx, name);
    return this.decodeDouble(loc, this.seriesRowCount, col.codec);
  }

  readSeriesBinary(name: string): (Uint8Array | null)[] {
    const [idx] = this.findColumn(this.seriesColumns, name);
    const loc = this.seriesLocs[idx];
    this.verifyCrc(loc, this.seriesCrcs, idx, name);
    return decodeVarlen(this.data.subarray(loc.pos, loc.pos + loc.len), this.seriesRowCount);
  }

  // --- Event readers ---

  readEventLong(name: string): Float64Array {
    const [idx, col] = this.findColumn(this.eventColumns, name);
    const loc = this.eventLocs[idx];
    this.verifyCrc(loc, this.eventCrcs, idx, name);
    return this.decodeLong(loc, this.eventsRowCount, col.codec);
  }

  readEventDouble(name: string): Float64Array {
    const [idx, col] = this.findColumn(this.eventColumns, name);
    const loc = this.eventLocs[idx];
    this.verifyCrc(loc, this.eventCrcs, idx, name);
    return this.decodeDouble(loc, this.eventsRowCount, col.codec);
  }

  readEventBinary(name: string): (Uint8Array | null)[] {
    const [idx] = this.findColumn(this.eventColumns, name);
    const loc = this.eventLocs[idx];
    this.verifyCrc(loc, this.eventCrcs, idx, name);
    return decodeVarlen(this.data.subarray(loc.pos, loc.pos + loc.len), this.eventsRowCount);
  }

  /** Async binary reader (uses native DecompressionStream for gzip in browser) */
  async readEventBinaryAsync(name: string): Promise<(Uint8Array | null)[]> {
    const [idx] = this.findColumn(this.eventColumns, name);
    const loc = this.eventLocs[idx];
    this.verifyCrc(loc, this.eventCrcs, idx, name);
    return decodeVarlenAsync(this.data.subarray(loc.pos, loc.pos + loc.len), this.eventsRowCount);
  }

  // --- Column info ---

  getSeriesColumn(name: string): ColumnInfo {
    return this.findColumn(this.seriesColumns, name)[1];
  }

  getEventColumn(name: string): ColumnInfo {
    return this.findColumn(this.eventColumns, name)[1];
  }

  // --- Internal ---

  private verifyCrc(loc: ColumnLocation, crcs: number[], idx: number, name: string): void {
    if (!this.hasChecksums || idx >= crcs.length) return;
    const colData = this.data.subarray(loc.pos, loc.pos + loc.len);
    const actual = crc32(colData);
    if (actual !== crcs[idx]) {
      throw new Error(
        `CRC32 mismatch on column '${name}': expected 0x${crcs[idx].toString(16).padStart(8, '0')}, got 0x${actual.toString(16).padStart(8, '0')}`,
      );
    }
  }

  private decodeLong(loc: ColumnLocation, count: number, codec: Codec): Float64Array {
    const colData = this.data.subarray(loc.pos, loc.pos + loc.len);
    switch (codec) {
      case Codec.DELTA_VARINT:
        return decodeDeltaVarint(colData, count);
      case Codec.RAW:
        return decodeRawLongs(colData, count);
      default:
        throw new Error(`Unsupported codec for LONG: ${codec}`);
    }
  }

  private decodeDouble(loc: ColumnLocation, count: number, codec: Codec): Float64Array {
    const colData = this.data.subarray(loc.pos, loc.pos + loc.len);
    switch (codec) {
      case Codec.ALP:
        return decodeAlp(colData);
      case Codec.GORILLA:
        return decodeGorilla(colData, count);
      case Codec.PONGO:
        return decodePongo(colData, count);
      case Codec.RAW:
        return decodeRawDoubles(colData, count);
      default:
        throw new Error(`Unsupported codec for DOUBLE: ${codec}`);
    }
  }

  private readColumnDescriptors(pos: number, count: number): [ColumnInfo[], number] {
    const columns: ColumnInfo[] = [];
    for (let i = 0; i < count; i++) {
      const codec = this.data[pos++] as Codec;
      const dataType = this.data[pos++] as DataType;
      const colFlags = this.data[pos++];
      const nameLen = this.data[pos++];
      const name = new TextDecoder().decode(this.data.subarray(pos, pos + nameLen));
      pos += nameLen;

      let metadata: Record<string, string> = {};
      if (colFlags & 0x02) {
        const metaLen = this.view.getUint16(pos, true);
        pos += 2;
        const metaJson = new TextDecoder().decode(this.data.subarray(pos, pos + metaLen));
        pos += metaLen;
        try {
          metadata = JSON.parse(metaJson);
        } catch {
          // ignore malformed metadata
        }
      }
      columns.push({ name, dataType, codec, metadata });
    }
    return [columns, pos];
  }

  private findColumn(columns: ColumnInfo[], name: string): [number, ColumnInfo] {
    const idx = columns.findIndex((c) => c.name === name);
    if (idx < 0) throw new Error(`Column not found: ${name}`);
    return [idx, columns[idx]];
  }
}

// --- CRC32 (IEEE 802.3, same as java.util.zip.CRC32) ---

const CRC_TABLE = new Uint32Array(256);
for (let i = 0; i < 256; i++) {
  let c = i;
  for (let j = 0; j < 8; j++) {
    c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
  }
  CRC_TABLE[i] = c;
}

function crc32(data: Uint8Array): number {
  let crc = 0xffffffff;
  for (let i = 0; i < data.length; i++) {
    crc = CRC_TABLE[(crc ^ data[i]) & 0xff] ^ (crc >>> 8);
  }
  return (crc ^ 0xffffffff) >>> 0;
}

import {
  MAGIC,
  FOOTER_MAGIC,
  FLAG_HAS_EVENTS,
  FLAG_HAS_FOOTER,
  FLAG_HAS_CHECKSUMS,
  FLAG_HAS_ROW_GROUPS,
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

export interface RowGroupStats {
  rowCount: number;
  offset: number;
  tsMin: number;
  tsMax: number;
}

interface ColumnLocation {
  pos: number;
  len: number;
}

/**
 * Lastra file reader for TypeScript/JavaScript.
 *
 * Reads `.lastra` files written by the Java LastraWriter.
 * Supports selective column access, row group filtering, and CRC32 verification.
 *
 * @example
 * ```ts
 * const reader = new LastraReader(buffer);
 * // Full read
 * const ts = reader.readSeriesLong('ts');
 * // Row group filtering
 * for (let i = 0; i < reader.rowGroupCount; i++) {
 *   const stats = reader.rowGroupStats(i);
 *   if (stats.tsMax >= from && stats.tsMin <= to) {
 *     const close = reader.readRowGroupDouble(i, 'close');
 *   }
 * }
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
  readonly rowGroupCount: number;

  // Flat layout (no row groups)
  private readonly seriesLocs: ColumnLocation[];
  private readonly seriesCrcs: number[];

  // Row group layout
  private readonly rgStats: RowGroupStats[];
  private readonly rgColLocs: ColumnLocation[][]; // [rgIndex][colIndex]
  private readonly rgColCrcs: number[][]; // [rgIndex][colIndex]

  // Events (always flat)
  private readonly eventLocs: ColumnLocation[];
  private readonly eventCrcs: number[];

  private readonly seriesColCount: number;

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
      throw new Error(`Unsupported Lastra version: ${version}`);
    }
    const flags = this.view.getUint16(pos, true);
    pos += 2;
    this.seriesRowCount = this.view.getInt32(pos, true);
    pos += 4;
    this.seriesColCount = this.view.getInt32(pos, true);
    pos += 4;
    this.eventsRowCount = this.view.getInt32(pos, true);
    pos += 4;
    const eventColCount = this.view.getUint16(pos, true);
    pos += 2;

    // Column descriptors
    const [seriesCols, newPos1] = this.readColumnDescriptors(pos, this.seriesColCount);
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

    this.hasChecksums = (flags & FLAG_HAS_CHECKSUMS) !== 0;
    const hasRowGroups = (flags & FLAG_HAS_ROW_GROUPS) !== 0;
    const hasFooter = (flags & FLAG_HAS_FOOTER) !== 0;

    this.seriesLocs = [];
    this.seriesCrcs = [];
    this.rgStats = [];
    this.rgColLocs = [];
    this.rgColCrcs = [];
    this.eventLocs = [];
    this.eventCrcs = [];

    if (hasFooter && hasRowGroups) {
      // Scan RG data forward: groups of seriesColCount columns
      const tempRgColLocs: ColumnLocation[][] = [];
      let scanPos = pos;
      while (true) {
        const colLocs: ColumnLocation[] = [];
        let tempPos = scanPos;
        let valid = true;
        for (let c = 0; c < this.seriesColCount; c++) {
          if (tempPos + 4 > this.data.byteLength) { valid = false; break; }
          const len = this.view.getInt32(tempPos, true);
          if (len < 0 || tempPos + 4 + len > this.data.byteLength) { valid = false; break; }
          colLocs.push({ pos: tempPos + 4, len });
          tempPos += 4 + len;
        }
        if (!valid) break;
        tempRgColLocs.push(colLocs);
        scanPos = tempPos;
        if (tempRgColLocs.length > 100000) break;
      }

      // Read events data
      for (let i = 0; i < this.eventColumns.length; i++) {
        const len = this.view.getInt32(scanPos, true);
        this.eventLocs.push({ pos: scanPos + 4, len });
        scanPos += 4 + len;
      }

      // Parse footer: rgCount, stats, CRCs
      let fp = scanPos;
      const rgCount = this.view.getInt32(fp, true);
      fp += 4;

      for (let i = 0; i < rgCount; i++) {
        const rgOffset = this.view.getInt32(fp, true); fp += 4;
        const rgRows = this.view.getInt32(fp, true); fp += 4;
        const tsMin = Number(this.getLongLE(fp)); fp += 8;
        const tsMax = Number(this.getLongLE(fp)); fp += 8;
        this.rgStats.push({ rowCount: rgRows, offset: rgOffset, tsMin, tsMax });
      }

      if (this.hasChecksums) {
        for (let i = 0; i < rgCount; i++) {
          const crcs: number[] = [];
          for (let c = 0; c < this.seriesColCount; c++) {
            crcs.push(this.view.getUint32(fp, true));
            fp += 4;
          }
          this.rgColCrcs.push(crcs);
        }
      }

      // Event CRCs
      // Event offsets already read by scanning; skip offset ints in footer
      fp += this.eventColumns.length * 4;
      if (this.hasChecksums) {
        for (let i = 0; i < this.eventColumns.length; i++) {
          this.eventCrcs.push(this.view.getUint32(fp, true));
          fp += 4;
        }
      }

      this.rgColLocs.push(...tempRgColLocs);
      this.rowGroupCount = rgCount;

    } else if (hasFooter) {
      // Flat layout (no row groups)
      // Scan data forward
      for (let i = 0; i < this.seriesColCount; i++) {
        const len = this.view.getInt32(pos, true);
        this.seriesLocs.push({ pos: pos + 4, len });
        pos += 4 + len;
      }
      for (let i = 0; i < this.eventColumns.length; i++) {
        const len = this.view.getInt32(pos, true);
        this.eventLocs.push({ pos: pos + 4, len });
        pos += 4 + len;
      }

      // Parse footer from end
      const totalCols = this.seriesColCount + this.eventColumns.length;
      let footerInts = totalCols;
      if (this.hasChecksums) footerInts += totalCols;
      footerInts += 1;

      const footerStart = this.data.byteLength - footerInts * 4;
      const fv = new DataView(this.data.buffer, this.data.byteOffset + footerStart, footerInts * 4);
      let fp = totalCols * 4; // skip offsets

      if (this.hasChecksums) {
        for (let i = 0; i < this.seriesColCount; i++) {
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

      this.rowGroupCount = 1;
    } else {
      this.rowGroupCount = 1;
    }
  }

  // --- Row group access ---

  /** Statistics for a specific row group. Returns undefined for flat files. */
  rowGroupStats(rgIndex: number): RowGroupStats | undefined {
    return this.rgStats[rgIndex];
  }

  /** All row group statistics. Empty for flat files. */
  allRowGroupStats(): readonly RowGroupStats[] {
    return this.rgStats;
  }

  readRowGroupLong(rgIndex: number, name: string): Float64Array {
    const [colIdx, col] = this.findColumn(this.seriesColumns, name);
    const loc = this.rgColLocs[rgIndex][colIdx];
    this.verifyRgCrc(rgIndex, colIdx, loc, name);
    return this.decodeLong(loc, this.rgStats[rgIndex].rowCount, col.codec);
  }

  readRowGroupDouble(rgIndex: number, name: string): Float64Array {
    const [colIdx, col] = this.findColumn(this.seriesColumns, name);
    const loc = this.rgColLocs[rgIndex][colIdx];
    this.verifyRgCrc(rgIndex, colIdx, loc, name);
    return this.decodeDouble(loc, this.rgStats[rgIndex].rowCount, col.codec);
  }

  readRowGroupBinary(rgIndex: number, name: string): (Uint8Array | null)[] {
    const [colIdx] = this.findColumn(this.seriesColumns, name);
    const loc = this.rgColLocs[rgIndex][colIdx];
    this.verifyRgCrc(rgIndex, colIdx, loc, name);
    return decodeVarlen(this.data.subarray(loc.pos, loc.pos + loc.len), this.rgStats[rgIndex].rowCount);
  }

  // --- Series readers (concatenate all RGs if present) ---

  readSeriesLong(name: string): Float64Array {
    if (this.rgStats.length > 0) {
      const result = new Float64Array(this.seriesRowCount);
      let offset = 0;
      for (let rg = 0; rg < this.rgStats.length; rg++) {
        const chunk = this.readRowGroupLong(rg, name);
        result.set(chunk, offset);
        offset += chunk.length;
      }
      return result;
    }
    const [idx, col] = this.findColumn(this.seriesColumns, name);
    const loc = this.seriesLocs[idx];
    this.verifyCrc(loc, this.seriesCrcs, idx, name);
    return this.decodeLong(loc, this.seriesRowCount, col.codec);
  }

  readSeriesDouble(name: string): Float64Array {
    if (this.rgStats.length > 0) {
      const result = new Float64Array(this.seriesRowCount);
      let offset = 0;
      for (let rg = 0; rg < this.rgStats.length; rg++) {
        const chunk = this.readRowGroupDouble(rg, name);
        result.set(chunk, offset);
        offset += chunk.length;
      }
      return result;
    }
    const [idx, col] = this.findColumn(this.seriesColumns, name);
    const loc = this.seriesLocs[idx];
    this.verifyCrc(loc, this.seriesCrcs, idx, name);
    return this.decodeDouble(loc, this.seriesRowCount, col.codec);
  }

  readSeriesBinary(name: string): (Uint8Array | null)[] {
    if (this.rgStats.length > 0) {
      const result: (Uint8Array | null)[] = [];
      for (let rg = 0; rg < this.rgStats.length; rg++) {
        result.push(...this.readRowGroupBinary(rg, name));
      }
      return result;
    }
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

  private verifyRgCrc(rgIndex: number, colIdx: number, loc: ColumnLocation, name: string): void {
    if (!this.hasChecksums || rgIndex >= this.rgColCrcs.length) return;
    const expected = this.rgColCrcs[rgIndex][colIdx];
    const colData = this.data.subarray(loc.pos, loc.pos + loc.len);
    const actual = crc32(colData);
    if (actual !== expected) {
      throw new Error(
        `CRC32 mismatch on RG ${rgIndex} column '${name}': expected 0x${expected.toString(16).padStart(8, '0')}, got 0x${actual.toString(16).padStart(8, '0')}`,
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

  private getLongLE(pos: number): bigint {
    const lo = this.view.getUint32(pos, true);
    const hi = this.view.getInt32(pos + 4, true);
    return BigInt(lo) | (BigInt(hi) << 32n);
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

import {
  MAGIC,
  FOOTER_MAGIC,
  FLAG_HAS_EVENTS,
  FLAG_HAS_FOOTER,
  DataType,
  Codec,
} from './constants.js';
import { decodeDeltaVarint } from './codec/delta-varint.js';
import { decodeAlp } from './codec/alp.js';
import { decodeVarlen, decodeVarlenAsync } from './codec/varlen.js';
import { decodeRawLongs, decodeRawDoubles } from './codec/raw.js';

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
 * Reef file reader for TypeScript/JavaScript.
 *
 * Reads `.reef` files written by the Java ReefWriter.
 * Supports selective column access — only requested columns are decompressed.
 *
 * @example
 * ```ts
 * const reader = new ReefReader(buffer);
 * const ts = reader.readSeriesLong('ts');
 * const close = reader.readSeriesDouble('close');
 * const meta = reader.getSeriesColumn('ema1').metadata;
 * ```
 */
export class ReefReader {
  private readonly data: Uint8Array;
  private readonly view: DataView;
  readonly seriesRowCount: number;
  readonly eventsRowCount: number;
  readonly seriesColumns: ColumnInfo[];
  readonly eventColumns: ColumnInfo[];
  private readonly seriesLocs: ColumnLocation[];
  private readonly eventLocs: ColumnLocation[];

  constructor(buffer: ArrayBuffer | Uint8Array) {
    this.data = buffer instanceof Uint8Array ? buffer : new Uint8Array(buffer);
    this.view = new DataView(this.data.buffer, this.data.byteOffset, this.data.byteLength);

    let pos = 0;

    // Header
    const magic = this.view.getUint32(pos, true);
    pos += 4;
    if (magic !== MAGIC) {
      throw new Error(`Not a Reef file (magic: 0x${magic.toString(16)})`);
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
  }

  // --- Series readers ---

  readSeriesLong(name: string): Float64Array {
    const [idx, col] = this.findColumn(this.seriesColumns, name);
    const loc = this.seriesLocs[idx];
    return this.decodeLong(loc, this.seriesRowCount, col.codec);
  }

  readSeriesDouble(name: string): Float64Array {
    const [idx, col] = this.findColumn(this.seriesColumns, name);
    const loc = this.seriesLocs[idx];
    return this.decodeDouble(loc, this.seriesRowCount, col.codec);
  }

  readSeriesBinary(name: string): (Uint8Array | null)[] {
    const [idx] = this.findColumn(this.seriesColumns, name);
    const loc = this.seriesLocs[idx];
    return decodeVarlen(this.data.subarray(loc.pos, loc.pos + loc.len), this.seriesRowCount);
  }

  // --- Event readers ---

  readEventLong(name: string): Float64Array {
    const [idx, col] = this.findColumn(this.eventColumns, name);
    const loc = this.eventLocs[idx];
    return this.decodeLong(loc, this.eventsRowCount, col.codec);
  }

  readEventDouble(name: string): Float64Array {
    const [idx, col] = this.findColumn(this.eventColumns, name);
    const loc = this.eventLocs[idx];
    return this.decodeDouble(loc, this.eventsRowCount, col.codec);
  }

  readEventBinary(name: string): (Uint8Array | null)[] {
    const [idx] = this.findColumn(this.eventColumns, name);
    const loc = this.eventLocs[idx];
    return decodeVarlen(this.data.subarray(loc.pos, loc.pos + loc.len), this.eventsRowCount);
  }

  /** Async binary reader (uses native DecompressionStream for gzip in browser) */
  async readEventBinaryAsync(name: string): Promise<(Uint8Array | null)[]> {
    const [idx] = this.findColumn(this.eventColumns, name);
    const loc = this.eventLocs[idx];
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

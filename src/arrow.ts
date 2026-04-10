/**
 * Bridge between ReefReader and Apache Arrow.
 *
 * Converts decoded Reef series data into an Arrow Table, enabling
 * interoperability with any Arrow-compatible tool (e.g. DuckDB-WASM,
 * Perspective, Arquero, Observable Plot).
 *
 * @example
 * ```ts
 * import { ReefReader } from '@qtsurfer/reef';
 * import { reefToArrow } from '@qtsurfer/reef/arrow';
 *
 * const reader = new ReefReader(buffer);
 * const table = reefToArrow(reader);
 * ```
 */

import {
  tableFromArrays,
  Float64,
  Utf8,
  type Table,
  type TypeMap,
} from 'apache-arrow';
import { ReefReader, type ColumnInfo } from './reader.js';
import { DataType } from './constants.js';

/**
 * Converts all series columns from a ReefReader into an Arrow Table.
 *
 * Type mapping:
 * - LONG → Float64 (Arrow Int64 requires BigInt; Float64 preserves millisecond timestamps)
 * - DOUBLE → Float64
 * - BINARY → Utf8 (decoded as UTF-8 strings)
 */
export function reefToArrow(reader: ReefReader): Table {
  const arrays: Record<string, Float64Array | string[]> = {};

  for (const col of reader.seriesColumns) {
    switch (col.dataType) {
      case DataType.LONG:
        arrays[col.name] = reader.readSeriesLong(col.name);
        break;
      case DataType.DOUBLE:
        arrays[col.name] = reader.readSeriesDouble(col.name);
        break;
      case DataType.BINARY: {
        const bins = reader.readSeriesBinary(col.name);
        const decoder = new TextDecoder();
        arrays[col.name] = bins.map((b) => (b ? decoder.decode(b) : ''));
        break;
      }
    }
  }

  return tableFromArrays(arrays);
}

/**
 * Converts selected series columns from a ReefReader into an Arrow Table.
 *
 * @param columnNames - columns to include (others are skipped, not decompressed)
 */
export function reefToArrowColumns(
  reader: ReefReader,
  columnNames: string[],
): Table {
  const arrays: Record<string, Float64Array | string[]> = {};

  for (const name of columnNames) {
    const col = reader.getSeriesColumn(name);
    switch (col.dataType) {
      case DataType.LONG:
        arrays[name] = reader.readSeriesLong(name);
        break;
      case DataType.DOUBLE:
        arrays[name] = reader.readSeriesDouble(name);
        break;
      case DataType.BINARY: {
        const bins = reader.readSeriesBinary(name);
        const decoder = new TextDecoder();
        arrays[name] = bins.map((b) => (b ? decoder.decode(b) : ''));
        break;
      }
    }
  }

  return tableFromArrays(arrays);
}

/**
 * Convenience: read a Reef buffer and return an Arrow Table in one call.
 */
export function readReefAsArrow(buffer: ArrayBuffer | Uint8Array): Table {
  return reefToArrow(new ReefReader(buffer));
}

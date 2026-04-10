/** Magic bytes: "LAST" in LE */
export const MAGIC = 0x4C415354;

/** Footer sentinel: "LAS!" in LE */
export const FOOTER_MAGIC = 0x4C415321;

/** Current format version */
export const VERSION = 1;

/** Header size in bytes (4+2+2+4+4+4+2) */
export const HEADER_SIZE = 22;

export enum DataType {
  LONG = 0,
  DOUBLE = 1,
  BINARY = 2,
}

export enum Codec {
  RAW = 0,
  DELTA_VARINT = 1,
  ALP = 2,
  VARLEN = 3,
  VARLEN_ZSTD = 4,
  VARLEN_GZIP = 5,
  GORILLA = 6,
  PONGO = 7,
}

export const FLAG_HAS_EVENTS = 1;
export const FLAG_HAS_FOOTER = 1 << 1;
export const FLAG_HAS_CHECKSUMS = 1 << 2;

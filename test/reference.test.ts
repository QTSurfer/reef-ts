import { describe, it, expect } from 'vitest';
import { ReefReader, DataType, Codec } from '../src/index.js';
import { readFileSync, existsSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const FIXTURE = join(__dirname, 'fixtures', 'reference.reef');

/**
 * Reference fixture test: validates the TS reader against known values
 * produced by the Java ReefWriter (ReefReferenceFixtureTest.java).
 *
 * Data: Random(42) for close, Random(99) for rsi, baseTs=1711152000000000000ns
 */
describe('Reference fixture (Java writer → TS reader)', () => {
  const data = existsSync(FIXTURE) ? new Uint8Array(readFileSync(FIXTURE)) : null;

  it('should have fixture file', () => {
    expect(data).not.toBeNull();
  });

  it('should parse header', () => {
    if (!data) return;
    const r = new ReefReader(data);
    expect(r.seriesRowCount).toBe(100);
    expect(r.eventsRowCount).toBe(5);
    expect(r.seriesColumns).toHaveLength(3);
    expect(r.eventColumns).toHaveLength(3);
  });

  it('should read series column descriptors', () => {
    if (!data) return;
    const r = new ReefReader(data);
    expect(r.seriesColumns[0]).toMatchObject({ name: 'ts', dataType: DataType.LONG, codec: Codec.DELTA_VARINT });
    expect(r.seriesColumns[1]).toMatchObject({ name: 'close', dataType: DataType.DOUBLE, codec: Codec.ALP });
    expect(r.seriesColumns[2]).toMatchObject({ name: 'rsi1', dataType: DataType.DOUBLE, codec: Codec.ALP });
  });

  it('should read column metadata', () => {
    if (!data) return;
    const r = new ReefReader(data);
    const rsi = r.getSeriesColumn('rsi1');
    expect(rsi.metadata.indicator).toBe('rsi');
    expect(rsi.metadata.periods).toBe('14');
  });

  it('should decode timestamps with exact values', () => {
    if (!data) return;
    const r = new ReefReader(data);
    const ts = r.readSeriesLong('ts');
    expect(ts).toHaveLength(100);
    expect(ts[0]).toBe(1_711_152_000_000_000_000);
    expect(ts[99]).toBe(1_711_152_099_000_000_000);
    // Monotonically increasing, 1s intervals
    for (let i = 1; i < 100; i++) {
      expect(ts[i] - ts[i - 1]).toBe(1_000_000_000);
    }
  });

  it('should decode close prices with exact values', () => {
    if (!data) return;
    const r = new ReefReader(data);
    const close = r.readSeriesDouble('close');
    expect(close).toHaveLength(100);
    // Reference values from Java: Random(42)
    expect(close[0]).toBeCloseTo(65007.28, 2);
    expect(close[1]).toBeCloseTo(65011.83, 2);
    expect(close[99]).toBeCloseTo(65423.50, 2);
    // All should be in range
    for (let i = 0; i < 100; i++) {
      expect(close[i]).toBeGreaterThan(64000);
      expect(close[i]).toBeLessThan(66000);
    }
  });

  it('should decode rsi with exact values', () => {
    if (!data) return;
    const r = new ReefReader(data);
    const rsi = r.readSeriesDouble('rsi1');
    expect(rsi).toHaveLength(100);
    // Reference values from Java: Random(99)
    expect(rsi[0]).toBeCloseTo(72.25, 2);
    expect(rsi[1]).toBeCloseTo(34.73, 2);
    // RSI range 0-100
    for (let i = 0; i < 100; i++) {
      expect(rsi[i]).toBeGreaterThanOrEqual(0);
      expect(rsi[i]).toBeLessThanOrEqual(100);
    }
  });

  it('should decode event timestamps', () => {
    if (!data) return;
    const r = new ReefReader(data);
    const evTs = r.readEventLong('ts');
    expect(evTs).toHaveLength(5);
    const base = 1_711_152_000_000_000_000;
    expect(evTs[0]).toBe(base + 10_000_000_000);
    expect(evTs[1]).toBe(base + 25_000_000_000);
    expect(evTs[2]).toBe(base + 40_000_000_000);
    expect(evTs[3]).toBe(base + 60_000_000_000);
    expect(evTs[4]).toBe(base + 85_000_000_000);
  });

  it('should decode event types', () => {
    if (!data) return;
    const r = new ReefReader(data);
    const types = r.readEventBinary('type');
    expect(types).toHaveLength(5);
    const dec = new TextDecoder();
    expect(dec.decode(types[0]!)).toBe('BUY');
    expect(dec.decode(types[1]!)).toBe('SELL');
    expect(dec.decode(types[2]!)).toBe('BUY');
    expect(dec.decode(types[3]!)).toBe('STOP_LOSS');
    expect(dec.decode(types[4]!)).toBe('SELL');
  });

  it('should decode event data with nulls', () => {
    if (!data) return;
    const r = new ReefReader(data);
    const evData = r.readEventBinary('data');
    expect(evData).toHaveLength(5);
    const dec = new TextDecoder();
    expect(dec.decode(evData[0]!)).toContain('65042.17');
    expect(dec.decode(evData[1]!)).toContain('65100.33');
    expect(evData[2]).toBeNull();
    expect(dec.decode(evData[3]!)).toContain('stop_hit');
    expect(evData[4]).toBeNull();
  });
});

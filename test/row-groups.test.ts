import { describe, it, expect } from 'vitest';
import { readFileSync, existsSync } from 'fs';
import { join } from 'path';
import { LastraReader } from '../src/reader.js';

const FIXTURE = join(__dirname, 'fixtures', 'row-groups.lastra');

describe('Row groups (Java writer → TS reader)', () => {
  it('should have fixture file', () => {
    expect(existsSync(FIXTURE)).toBe(true);
  });

  it('should parse row group count and total rows', () => {
    const buf = readFileSync(FIXTURE);
    const r = new LastraReader(buf);

    // 10000 rows / 3600 RG size = 3 RGs (3600 + 3600 + 2800)
    expect(r.seriesRowCount).toBe(10000);
    expect(r.rowGroupCount).toBe(3);
    expect(r.seriesColumns.length).toBe(2);
    expect(r.seriesColumns[0].name).toBe('ts');
    expect(r.seriesColumns[1].name).toBe('close');
  });

  it('should have correct per-RG stats', () => {
    const buf = readFileSync(FIXTURE);
    const r = new LastraReader(buf);

    const rg0 = r.rowGroupStats(0)!;
    expect(rg0.rowCount).toBe(3600);
    expect(rg0.tsMin).toBe(1711152000000);
    expect(rg0.tsMax).toBe(1711152000000 + 3599 * 1000);

    const rg1 = r.rowGroupStats(1)!;
    expect(rg1.rowCount).toBe(3600);
    expect(rg1.tsMin).toBe(1711152000000 + 3600 * 1000);

    const rg2 = r.rowGroupStats(2)!;
    expect(rg2.rowCount).toBe(2800);
    expect(rg2.tsMax).toBe(1711152000000 + 9999 * 1000);
  });

  it('should read a single row group selectively', () => {
    const buf = readFileSync(FIXTURE);
    const r = new LastraReader(buf);

    const rg1Close = r.readRowGroupDouble(1, 'close');
    expect(rg1Close.length).toBe(3600);
    // Values should be valid prices (~65000)
    expect(rg1Close[0]).toBeGreaterThan(64000);
    expect(rg1Close[0]).toBeLessThan(66000);
  });

  it('should read full series concatenating all RGs', () => {
    const buf = readFileSync(FIXTURE);
    const r = new LastraReader(buf);

    const allTs = r.readSeriesLong('ts');
    expect(allTs.length).toBe(10000);
    // First and last timestamps
    expect(allTs[0]).toBe(1711152000000);
    expect(allTs[9999]).toBe(1711152000000 + 9999 * 1000);
    // Monotonically increasing
    for (let i = 1; i < allTs.length; i++) {
      expect(allTs[i]).toBeGreaterThan(allTs[i - 1]);
    }

    const allClose = r.readSeriesDouble('close');
    expect(allClose.length).toBe(10000);
  });

  it('should filter RGs by temporal range', () => {
    const buf = readFileSync(FIXTURE);
    const r = new LastraReader(buf);

    // Query: rows 4000-5000 → only RG 1 (3600-7199)
    const queryFrom = 1711152000000 + 4000 * 1000;
    const queryTo = 1711152000000 + 5000 * 1000;

    let matched = 0;
    for (let i = 0; i < r.rowGroupCount; i++) {
      const stats = r.rowGroupStats(i)!;
      if (stats.tsMax >= queryFrom && stats.tsMin <= queryTo) {
        matched++;
      }
    }
    expect(matched).toBe(1);
  });

  it('should verify CRC32 per row group', () => {
    const buf = readFileSync(FIXTURE);
    const r = new LastraReader(buf);
    expect(r.hasChecksums).toBe(true);

    // Reading should not throw (CRC verified internally)
    for (let rg = 0; rg < r.rowGroupCount; rg++) {
      r.readRowGroupLong(rg, 'ts');
      r.readRowGroupDouble(rg, 'close');
    }
  });
});

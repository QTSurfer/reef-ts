import { describe, it, expect, beforeAll } from 'vitest';
import { ReefReader, DataType, Codec } from '../src/index.js';
import { execSync } from 'node:child_process';
import { readFileSync, existsSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const FIXTURE_DIR = join(__dirname, 'fixtures');
const REEF_JAVA_DIR = join(__dirname, '..', '..', 'qtsurfer-reef');

describe('ReefReader', () => {
  beforeAll(() => {
    // Generate test fixtures from Java ReefWriter
    if (!existsSync(join(FIXTURE_DIR, 'series-only.reef'))) {
      execSync(
        `cd ${REEF_JAVA_DIR} && mvn -q compile exec:java -Dexec.mainClass=com.wualabs.qtsurfer.reef.ReefTestFixtureGenerator -Dexec.classpathScope=test -Dexec.args="${FIXTURE_DIR}" 2>/dev/null || true`,
      );
    }
  });

  it('should parse header and column descriptors', () => {
    const data = readFixture('series-only.reef');
    if (!data) return; // skip if no fixture

    const reader = new ReefReader(data);
    expect(reader.seriesRowCount).toBe(100);
    expect(reader.seriesColumns.length).toBe(2);
    expect(reader.seriesColumns[0].name).toBe('ts');
    expect(reader.seriesColumns[0].dataType).toBe(DataType.LONG);
    expect(reader.seriesColumns[0].codec).toBe(Codec.DELTA_VARINT);
    expect(reader.seriesColumns[1].name).toBe('close');
    expect(reader.seriesColumns[1].dataType).toBe(DataType.DOUBLE);
    expect(reader.seriesColumns[1].codec).toBe(Codec.ALP);
  });

  it('should decode series timestamps', () => {
    const data = readFixture('series-only.reef');
    if (!data) return;

    const reader = new ReefReader(data);
    const ts = reader.readSeriesLong('ts');
    expect(ts.length).toBe(100);
    // Timestamps should be monotonically increasing
    for (let i = 1; i < ts.length; i++) {
      expect(ts[i]).toBeGreaterThan(ts[i - 1]);
    }
  });

  it('should decode series doubles', () => {
    const data = readFixture('series-only.reef');
    if (!data) return;

    const reader = new ReefReader(data);
    const close = reader.readSeriesDouble('close');
    expect(close.length).toBe(100);
    // Prices should be in reasonable range
    for (let i = 0; i < close.length; i++) {
      expect(close[i]).toBeGreaterThan(60000);
      expect(close[i]).toBeLessThan(70000);
    }
  });

  it('should read column metadata', () => {
    const data = readFixture('with-metadata.reef');
    if (!data) return;

    const reader = new ReefReader(data);
    const ema = reader.getSeriesColumn('ema1');
    expect(ema.metadata.indicator).toBe('ema');
    expect(ema.metadata.periods).toBe('10');
  });

  it('should read events section', () => {
    const data = readFixture('with-events.reef');
    if (!data) return;

    const reader = new ReefReader(data);
    expect(reader.eventsRowCount).toBeGreaterThan(0);

    const eventTs = reader.readEventLong('ts');
    expect(eventTs.length).toBe(reader.eventsRowCount);

    const types = reader.readEventBinary('type');
    expect(types.length).toBe(reader.eventsRowCount);

    const decoder = new TextDecoder();
    for (const t of types) {
      if (t) {
        const str = decoder.decode(t);
        expect(['BUY', 'SELL', 'STOP_LOSS']).toContain(str);
      }
    }
  });
});

function readFixture(name: string): Uint8Array | null {
  const path = join(FIXTURE_DIR, name);
  if (!existsSync(path)) {
    console.warn(`Fixture not found: ${path} — run Java fixture generator first`);
    return null;
  }
  return new Uint8Array(readFileSync(path));
}

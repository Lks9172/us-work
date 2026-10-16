import { describe, expect, it } from 'vitest';

import { fullJitterDelay } from '../../packages/alarms/src/infrastructure/persistence/postgres-dispatch-repository';

function deterministicSequence(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (Math.imul(state, 1_664_525) + 1_013_904_223) >>> 0;
    return state / 0x1_0000_0000;
  };
}

describe('dispatch retry policy', () => {
  it('[RET-06] keeps first/last retry full-jitter bounds and the configured cap exact', () => {
    expect(fullJitterDelay(() => 0, 1)).toBe(0);
    expect(fullJitterDelay(() => 0.999_999_999, 1)).toBe(199);
    expect(fullJitterDelay(() => 0, 4)).toBe(0);
    expect(fullJitterDelay(() => 0.999_999_999, 4)).toBe(1_599);
    expect(fullJitterDelay(() => 0.999_999_999, 6)).toBe(6_399);
    expect(fullJitterDelay(() => 0.999_999_999, 30)).toBe(29_999);
  });

  it('[RET-06] is deterministic for the same injected seed', () => {
    const first = deterministicSequence(0x5eed_1234);
    const second = deterministicSequence(0x5eed_1234);

    const firstRun = Array.from({ length: 32 }, (_, index) =>
      fullJitterDelay(first, (index % 8) + 1),
    );
    const secondRun = Array.from({ length: 32 }, (_, index) =>
      fullJitterDelay(second, (index % 8) + 1),
    );

    expect(firstRun).toEqual(secondRun);
    expect(new Set(firstRun).size).toBeGreaterThan(16);
  });

  it('[U-RETRY-02] satisfies deterministic full-jitter bounds across seeded samples and cap edges', () => {
    for (let seed = 0; seed < 256; seed += 1) {
      const first = deterministicSequence(seed);
      const replay = deterministicSequence(seed);
      for (let attempt = 1; attempt <= 64; attempt += 1) {
        const expectedExclusiveCap = Math.min(30_000, 200 * 2 ** (attempt - 1));
        const delay = fullJitterDelay(first, attempt);
        expect(delay).toBe(fullJitterDelay(replay, attempt));
        expect(delay).toBeGreaterThanOrEqual(0);
        expect(delay).toBeLessThan(expectedExclusiveCap);
      }
    }
  });

  it.each([Number.NaN, Number.POSITIVE_INFINITY, -0.001, 1])(
    '[RET-06] rejects an invalid injected random sample %s instead of escaping its bounds',
    (sample) => {
      expect(() => fullJitterDelay(() => sample, 1)).toThrow(RangeError);
    },
  );
});

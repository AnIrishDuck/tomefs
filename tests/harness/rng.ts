/**
 * Seeded PRNG (xorshift128+) for reproducible random sequences in fuzz tests.
 *
 * All fuzz tests use this shared implementation to ensure consistent
 * seeding behavior and reproducibility across test files.
 */
export class Rng {
  private s0: number;
  private s1: number;

  constructor(seed: number) {
    this.s0 = this.splitmix32(seed);
    this.s1 = this.splitmix32(this.s0);
    if (this.s0 === 0 && this.s1 === 0) this.s1 = 1;
  }

  private splitmix32(x: number): number {
    x = (x + 0x9e3779b9) | 0;
    x = Math.imul(x ^ (x >>> 16), 0x85ebca6b);
    x = Math.imul(x ^ (x >>> 13), 0xc2b2ae35);
    return (x ^ (x >>> 16)) >>> 0;
  }

  /** Returns a random integer in [0, 2^32). */
  next(): number {
    let s0 = this.s0;
    let s1 = this.s1;
    const result = (s0 + s1) >>> 0;
    s1 ^= s0;
    this.s0 = ((s0 << 26) | (s0 >>> 6)) ^ s1 ^ (s1 << 9);
    this.s1 = (s1 << 13) | (s1 >>> 19);
    return result;
  }

  /** Returns a random integer in [0, max). */
  int(max: number): number {
    return this.next() % max;
  }

  /** Pick a random element from an array. */
  pick<T>(arr: T[]): T {
    return arr[this.int(arr.length)];
  }

  /** Returns random bytes of the given length. */
  bytes(length: number): Uint8Array {
    const buf = new Uint8Array(length);
    for (let i = 0; i < length; i++) {
      buf[i] = this.next() & 0xff;
    }
    return buf;
  }

  /** Returns true with the given probability (0.0 to 1.0). */
  bool(probability: number): boolean {
    return (this.next() / 0x100000000) < probability;
  }
}

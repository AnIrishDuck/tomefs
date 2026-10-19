export const CACHE_CONFIGS = {
  tiny: 4,       // 32 KB — maximum eviction pressure
  small: 16,     // 128 KB — moderate eviction
  medium: 64,    // 512 KB — working set partially fits
  large: 4096,   // 32 MB — working set fits, baseline
} as const;

export type CacheSize = keyof typeof CACHE_CONFIGS;

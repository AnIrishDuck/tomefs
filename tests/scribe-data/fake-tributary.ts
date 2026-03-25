/**
 * Fake Tributary Server for scribe-data integration tests.
 *
 * Simulates the core Server interface from tributary-client: blob storage
 * with sequence-numbered entries and paged retrieval. No real crypto —
 * blobs store plaintext SQL. Focused on the paging/sync behavior that
 * stresses tomefs's page cache.
 *
 * Modeled on tributary's FakeServer / TestFakeServer.
 */

/** A single blob stored on the fake server. */
export interface Blob {
  sequence: number;
  data: string;
  hash: string;
  prevHash: string;
  timestamp: number;
}

/** Metadata-only view of a blob (no data field). */
export interface BlobMeta {
  sequence: number;
  hash: string;
  prevHash: string;
  timestamp: number;
}

/** Result of a paged blob fetch. */
export interface BlobPage {
  blobs: Blob[];
  totalCount: number;
  hasMore: boolean;
}

/** Result of a paged metadata fetch. */
export interface BlobMetaPage {
  metadata: BlobMeta[];
  totalCount: number;
  hasMore: boolean;
}

/** Status returned by sync operations. */
export interface SyncStatus {
  fetched: number;
  total: number;
  complete: boolean;
}

/**
 * Fake in-memory blob server.
 *
 * Each "stream" (identified by a string key) has an ordered list of blobs.
 * Blobs are appended with auto-incrementing sequence numbers and simple
 * hash chaining.
 */
export class FakeServer {
  private streams = new Map<string, Blob[]>();
  private _connected = true;

  /** Store a blob (SQL statement) on a stream. Returns the sequence number. */
  storeBlob(streamKey: string, data: string): number {
    this.assertConnected();
    const blobs = this.getOrCreateStream(streamKey);
    const sequence = blobs.length + 1;
    const prevHash = blobs.length > 0 ? blobs[blobs.length - 1].hash : "genesis";
    const hash = `hash-${streamKey}-${sequence}`;

    blobs.push({
      sequence,
      data,
      hash,
      prevHash,
      timestamp: Date.now(),
    });

    return sequence;
  }

  /** Store multiple blobs at once. Returns the final sequence number. */
  storeBlobBatch(streamKey: string, items: string[]): number {
    let seq = 0;
    for (const data of items) {
      seq = this.storeBlob(streamKey, data);
    }
    return seq;
  }

  /**
   * Fetch blobs after `startSequence`, limited by `max`.
   * This is the core paging primitive used during sync.
   */
  getBlobsPage(streamKey: string, startSequence: number, max: number): BlobPage {
    this.assertConnected();
    const blobs = this.streams.get(streamKey) ?? [];
    const after = blobs.filter((b) => b.sequence > startSequence);
    const page = after.slice(0, max);

    return {
      blobs: page,
      totalCount: blobs.length,
      hasMore: after.length > max,
    };
  }

  /** Metadata-only paged fetch. */
  getBlobMetaPage(streamKey: string, startSequence: number, max: number): BlobMetaPage {
    this.assertConnected();
    const blobs = this.streams.get(streamKey) ?? [];
    const after = blobs.filter((b) => b.sequence > startSequence);
    const page = after.slice(0, max);

    return {
      metadata: page.map(({ sequence, hash, prevHash, timestamp }) => ({
        sequence,
        hash,
        prevHash,
        timestamp,
      })),
      totalCount: blobs.length,
      hasMore: after.length > max,
    };
  }

  /** Get latest blob metadata for a stream. */
  getLatestMeta(streamKey: string): BlobMeta | null {
    this.assertConnected();
    const blobs = this.streams.get(streamKey);
    if (!blobs || blobs.length === 0) return null;
    const last = blobs[blobs.length - 1];
    return {
      sequence: last.sequence,
      hash: last.hash,
      prevHash: last.prevHash,
      timestamp: last.timestamp,
    };
  }

  /** Total blob count for a stream. */
  getBlobCount(streamKey: string): number {
    return (this.streams.get(streamKey) ?? []).length;
  }

  /** Simulate network disconnect. */
  disconnect(): void {
    this._connected = false;
  }

  /** Simulate network reconnect. */
  reconnect(): void {
    this._connected = true;
  }

  get connected(): boolean {
    return this._connected;
  }

  private assertConnected(): void {
    if (!this._connected) {
      throw new Error("FakeServer: disconnected");
    }
  }

  private getOrCreateStream(streamKey: string): Blob[] {
    let blobs = this.streams.get(streamKey);
    if (!blobs) {
      blobs = [];
      this.streams.set(streamKey, blobs);
    }
    return blobs;
  }
}

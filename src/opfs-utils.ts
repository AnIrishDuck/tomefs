/**
 * Shared utilities for OPFS backends.
 *
 * Both OpfsBackend and OpfsSahBackend need identical path encoding,
 * error detection, and directory layout constants. Centralizing them
 * here avoids duplication and ensures bug fixes apply to both backends.
 */

export interface IterableDirectoryHandle extends FileSystemDirectoryHandle {
  keys(): AsyncIterableIterator<string>;
}

export function isNotFoundError(err: unknown): boolean {
  return err instanceof DOMException && err.name === "NotFoundError";
}

export const PAGES_DIR = "pages";
export const META_DIR = "meta";

const HEX_TABLE: string[] = new Array(256);
for (let i = 0; i < 256; i++) {
  HEX_TABLE[i] = i.toString(16).padStart(2, "0");
}

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

export function encodePath(path: string): string {
  const bytes = textEncoder.encode(path);
  const parts = new Array<string>(bytes.length);
  for (let i = 0; i < bytes.length; i++) {
    parts[i] = HEX_TABLE[bytes[i]];
  }
  return parts.join("");
}

export function decodePath(hex: string): string {
  const bytes = new Uint8Array(hex.length / 2);
  for (let i = 0; i < bytes.length; i++) {
    bytes[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  }
  return textDecoder.decode(bytes);
}

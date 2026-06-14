import { highlight } from '@arborium/arborium';
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';

const nm = join(process.cwd(), 'node_modules');

export const arboriumConfig = {
  logger: {
    debug() {},
    warn: console.warn,
    error: console.error,
  },
  resolveHostJs: () => import('@arborium/arborium/arborium_host.js'),
  resolveHostWasm: () =>
    readFile(join(nm, '@arborium/arborium/dist/arborium_host_bg.wasm')),
  resolveJs: () => import('@arborium/rust/grammar.js'),
  resolveWasm: () => readFile(join(nm, '@arborium/rust/grammar_bg.wasm')),
};

export async function highlightRust(source) {
  return highlight('rust', source, arboriumConfig);
}

import { readFile } from "node:fs/promises";

const arboriumConfig = {
  logger: {
    debug() {},
    warn: console.warn,
    error: console.error,
  },
  resolveHostJs: () => import("@arborium/arborium/arborium_host.js"),
  resolveHostWasm: () =>
    readFile(
      new URL(import.meta.resolve("@arborium/arborium/arborium_host_bg.wasm")),
    ),
  resolveJs: ({ language }) => import(`@arborium/${language}/grammar.js`),
  resolveWasm: ({ language }) =>
    readFile(
      new URL(import.meta.resolve(`@arborium/${language}/grammar_bg.wasm`)),
    ),
};

/**
 * Highlight source code at build time using arborium (tree-sitter WASM).
 * Returns an HTML string of <a-*> token tags — render with set:html.
 */
export async function highlightCode(language, source) {
  const { highlight } = await import("@arborium/arborium");
  return highlight(language, source, arboriumConfig);
}

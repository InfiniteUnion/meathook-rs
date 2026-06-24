# Security Policy

## Supported Versions

Security fixes are provided for the latest published version of the
`meathook-rs` crate (library name: `meathook`).

Until the project reaches `1.0`, compatibility may change between minor
versions. Please test against the latest release before reporting an issue
that may already be fixed.

## Reporting a Vulnerability

Please do not open a public issue for a suspected security vulnerability.

Report privately using [GitHub Security Advisories](https://github.com/InfiniteUnion/meathook-rs/security/advisories/new)
for this repository, or by contacting a maintainer through the
[InfiniteUnion organization](https://github.com/InfiniteUnion) or
[zeon256](https://github.com/zeon256). Include:

- affected crate version or commit
- a minimal reproducer (Rust snippet, pipeline configuration, or spool
  layout as appropriate)
- expected behavior
- observed behavior
- impact assessment

You should receive an initial response within 7 days. If the report is
accepted, a fix and advisory will be coordinated before public disclosure
where practical.

## Security Scope

Issues generally considered security-sensitive include:

- panics, hangs, or excessive resource usage triggered by untrusted record
  batches, spool segment files, or parquet payloads during ingest, flush, or
  recovery
- path traversal or unintended file overwrite when using [`DiskSpool`] with
  attacker-controlled spool directories or pipeline names
- unsound Rust or memory safety issues in meathook libraries or sink
  combinators
- leakage of secrets (for example `HF_TOKEN` or other bearer tokens) through
  logs, error messages, or HTTP debug output in [`HfSink`] or transport
  adapters
- spool recovery that executes unsafe actions on corrupt or attacker-crafted
  JSONL segments beyond skipping individual bad lines
- [`HfSink`] commits that write outside the intended repo path or branch due
  to insufficient validation of `repo`, `branch`, or `path_in_repo`
- generated or hand-written HTTP actions that construct unsafe URLs or
  headers from untrusted metadata

Issues generally not considered vulnerabilities by themselves:

- bugs in a remote API or dataset being polled that do not stem from meathook
  code
- application misuse of collectors, sink stacks, or environment variables
  (for example exposing `HF_TOKEN` in shell history or config files)
- data loss from disk failure, `SIGKILL`, or missing persistent volumes when
  the README's durability table already describes the risk
- missing sink or collector features unless they cause incorrect, unsafe, or
  exploitable runtime behavior

## Disclosure

Accepted vulnerabilities will be fixed in a patch release when possible.
Public disclosure should include the affected versions, fixed versions,
impact, and suggested mitigation.

[`DiskSpool`]: https://docs.rs/meathook-rs/latest/meathook/layer/disk/struct.DiskSpool.html
[`HfSink`]: https://docs.rs/meathook-rs/latest/meathook/sink/huggingface/struct.HfSink.html

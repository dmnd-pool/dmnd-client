Use only README for documenting things, use ASD-STE100 communication standard for both documentation
and your answear. Do not make up world if yuo need to do it make sure to define them before. Use a
modularized logic with good abstractions (hide complexity under high level reusable abstractions)
always. Do not reimplement things if they are already available. Try to keep functions small (modules
are internally complex, module interfaces are simple, functions are simple when possible)

Put new things that will be need by every new agent in this file, not everything keep it as short as
possible. (tooling, how to execute things, any file that should never be changed ecc ecc)

Always reuse the stile and the conventions used by the files that you are changing.

## Protected control tests

The user uses `tests/**` to evaluate agent changes. These rules take precedence over
the other test and check requirements in this file.

- Do not read, search, change, compile, or run `tests/**` or `.github/workflows/mining-e2e.yml`.
- Do not inspect related test binaries, saved output, or copies in Git history or remote repositories.
- Exclude protected files from searches, diffs, reviews, and checks. Scope commands to permitted files.
- These restrictions also apply to tools and delegated agents.
- Do not change `build.rs`; it selects the protected test transport API.
- Access requires an explicit user instruction. A general request to review or fix code does not authorize access.

## Commit message rules
Every commit message must clearly say what was done and why it was done. When adding new features it
must describe which is the underling logic. Never include things that can be seen with a git diff,
like which file or function are changes unless they are necessary tho explain why something have
been done or the underling logic.

The first line must be written in imperative form and must read like the commit itself is performing
the action.

The first line must start with exactly one of these verbs:
- `ADD`: new feature or addition of something new
- `FIX`: bug fix
- `UPDATE`: improvement to something already present and backward compatible
- `UPGRADE`: improvement or change that is not backward compatible
- `DELETE`: delete something

Do not use other leading verbs.

The first line must be specific and concise, and it must describe both the change and the reason
when possible.

Good style:
- `ADD share validation cache to reduce duplicate DB reads`
- `FIX stale job handling when prevhash changes during submit parsing`
- `UPDATE pool startup logging to expose selected template provider`
- `UPGRADE auth token format to include versioned payload parsing`

Bad style:
- `changed stuff`
- `misc fixes`
- `update code`
- `refactor`
- `ADD new thing`
- `FIX bug`

If more detail is needed, add a body after the first line explaining the rationale, constraints,
or important implementation notes, but keep the first line strong enough to stand on its own.

## Bug FIX rules
When asked to fix a bug, always write a test that reproduces the bug, verify that the test fails, and then write the fix.
Put agent-written tests in `src/` as unit tests. Run the relevant test with
`cargo test --lib <test_name>` or `cargo test --bin dmnd-client <test_name>`.
Do not use the protected control tests.

## Review rules
Any patch that increases algorithmic complexity to O(n²) or worse must be flagged.

## Required Checks
Run these before submitting code changes. Keep checks limited to production targets:

1. `rustfmt --edition 2021 src/lib.rs src/main.rs`
2. `cargo clippy --lib --bin dmnd-client --all-features -- -D warnings`
- Do not use `cargo fmt`, `cargo test` without a target, or checks with `--all-targets`.
  These commands can process the protected tests.
- Any code change must leave the relevant build and clippy invocations clean: no build warnings,
  clippy errors, or clippy warnings are allowed. Existing warnings or clippy findings encountered
  while validating the change must be fixed, not left in place.

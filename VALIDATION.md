# Validation — 2026-10-25

Actual machine: macOS 26.3 (25D2125), ARM64; rustc 1.98.0.
Only this selected repository was changed, on `feat/native-process-rust`.
SDK uses published git revision `9f91afcc071c3f301414dc62b33cfeac7d89a0cf`;
Cargo.lock is regenerated. Entrypoint uses two Tokio worker threads.

Passed locally:

- `cargo fmt --check`.
- `cargo clippy --locked --all-targets -j1 -- -D warnings` (strict, no lint suppressions).
- `cargo test --locked -j 1 -- --test-threads=1`: 18 domain tests and one actual
  stdio executable regression test. All 8 inherited tests are retained, plus 10 ownership/retry/concurrency regressions.
- `cargo build --release --locked -j 1 --target aarch64-apple-darwin`.
- Real executable identity/protocol/session routing, reverse host RPC, dispose,
  shutdown and EOF. Durable retain-before-write (including rejected/cancelled registration), pause/resume/stop, active timer after dispose/EOF restoration, notification failure/retry and removal retry, idle release, native-v2 persistence/migration.
- Frozen source pack matches `git show fe71ca87f21ac4ff31505010c228f8abda0f2a89`;
  all inherited test names, command IDs/preferences and original icons preserved.
- No app or Tauri Cargo dependency; SDK source is the immutable git dependency.
- Six original integer-synthesized WAVs match the generator and share no SHA with the restricted upstream research recordings. Silent audio and injected-clock tests cover scheduler sound/notification bookkeeping and shutdown cleanup.

Packaging uses actual native release executable bytes, a complete SHA-256 file
inventory and separate computed artifact metadata. Extracted-ZIP process smoke
uses OS-only PATH with a headless fixture host; no source/toolchain is needed by
the executable. See the matching `color-timers-*-package.log` and
`color-timers-*-package-smoke.log` under `/tmp/tezbar-external-rust` for results.

Not run: native Windows build/execution for this port; native CI is supplied but
has not been dispatched by this child. SDK native Windows CI evidence does not
constitute port validation. Live desktop sampler permissions, Finder/Explorer,
clipboard/paste, tray display, actual AI service, notifications and audible
playback are not exercised: fixture host acknowledges their typed RPC and timer
tests use silent audio. No clean-profile live Tezbar install was attempted here.

Parent owns publication, final repository/artifact URLs, dual-target CI, signing,
trusted catalog pins and clean-profile app smoke. No commits, pushes, repositories,
PRs or releases were created by this implementation child.

Focused finalization review (same date): package inventories now include SOURCE.json;
required runtime resources are explicit and validated before packaging. Color Picker
preserves executable sampler permissions through ZIP extraction and excludes a stale
Mac helper from Windows packages. Both packagers require their declared native OS/
architecture. CI uses macos-15 ARM and windows-2022, single-job locked tests/builds,
strict Clippy, and extracted-package process smoke. Windows Color Picker additionally
compiles its real PowerShell/C# sampler via -Check, before any DPI/capture/UI calls.
These are supplied CI checks, not claims that hosted Windows jobs have run.

Review fixes: cancelled background activation remains process-owned; idle mutations
retry failed/negative lease releases without requiring a scheduler. Stopping an
alarm removes notifications even after a lost show acknowledgement; removals have
a five-second bound. PowerShell resolves from the OS directory rather than PATH.
Two added domain regressions and an added real-pipe activation-cancellation scenario
pass locally. All inherited tests remain. GNU cross-target checks, if recorded in
the parent review report, do not establish native MSVC build/execution.

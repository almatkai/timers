# Timers — standalone Rust process

All nineteen commands, original preference identifiers, presets, stopwatch
controls, duration validation, monotonic active timing, wall-clock restoration,
concurrent storage updates, sound retries, and notification bookkeeping remain
in the standalone Rust crate. No Tauri, app crate, Node, Bun, installed Raycast
bundle, or app-specific native directory is used by the executable.

An idle view starts no scheduler. Active state creation/resume first awaits a
durable `host.background.retain {key:"timers",durable:true}` acknowledgement.
The process owns one scheduler and at most one sound player; command view
closure leaves active alarms running. Idle/paused state releases the lease;
notification delivery/removal and sound retries retain it until acknowledged.
Ringing notification acknowledgements are refreshed for each new host process
generation; completed single-shot delivery remains recorded. The generic host
restores durable activation with `background.activate`; a
manifest startupProbe imports an explicitly granted native-v2 store before the
first view after upgrade. Shutdown stops and joins the worker and kills audio
children while preserving durable activation for remaining persisted work.

The old six Mixkit recordings are excluded. All six filename IDs now select new
original synthesized melodies, so existing preferences still resolve but sound
different. `Submarine` retains its existing fallback to `alarmClock.wav`.
`python3 scripts/generate_tones.py` deterministically regenerates the six WAVs
using integer PCM arithmetic, without samples or third-party audio. Speak Timer
Name uses the platform speech system. Windows playback preserves the existing
limitation: only volume 1 is supported; other volume settings return an error.

```sh
python3 scripts/generate_tones.py --check
cargo test --locked -j 1 -- --test-threads=1
cargo build --release --locked -j 1
python3 scripts/package.py --target aarch64-apple-darwin --binary target/release/tezbar-timers
python3 scripts/smoke.py --target aarch64-apple-darwin --archive dist/timers-aarch64-apple-darwin.zip
```

Use a native Windows x64 runner and `target/release/tezbar-timers.exe` for the
`x86_64-pc-windows-msvc` package. Notifications go through acknowledged generic
host notifications, never AppleScript/bundle identity or timer-specific host IPC.
The baseline menu-bar commands retain their renderer List/view behavior; this
port does not add an OS tray that was absent from that adapter.

## Protocol and storage

`--protocol-info` and `--self-test` are headless. Normal invocation uses only
UTF-8 JSON-lines on stdin/stdout and logs errors to stderr. The real independent
`tezbar-extension-sdk` 0.1.0 handles protocol 1, sessions/cancellation, and host RPC.
`extension.json` preserves schema-3 commands/preferences and declares capabilities.
Storage is `<host-private-data-dir>/timers/storage.json`, updated atomically by
the SDK's shared transaction mutex. Malformed JSON is rejected without overwriting.
Only explicitly host-granted `migrationPaths` files are imported, read-only, once;
existing scoped values and unknown native-v2 keys are preserved. Preferences are
still supplied by the host per session and are never copied from global config.

Source baseline: `fe71ca87f21ac4ff31505010c228f8abda0f2a89`. See SOURCE.json and NOTICE.
The licenses describe source/data/artwork separately; no blanket ownership claim
is made for upstream assets or names.

## Packaging and publication

A target ZIP contains extension.json, bin/<executable>, resources/**, licenses/**,
LICENSE, NOTICE and README. Its `files` object hashes every file except its own
manifest. `scripts/package.py` validates native executable identity and emits a
separate artifact JSON with actual ZIP/executable SHA-256. Source artifacts remain
empty until parent publishes; no digest is invented and no user-machine Cargo
build is needed to install a published ZIP. `scripts/smoke.py` extracts/verifies
inventory and executes the real process against a headless host using OS-only PATH.

SDK is pinned to published immutable revision
`9f91afcc071c3f301414dc62b33cfeac7d89a0cf` from
https://github.com/almatkai/tezbar-extension-sdk-rust.git; Cargo.lock is delivered
with the executable source. CI enforces an immutable SDK revision. CI builds/tests/packages on native macOS ARM
and Windows x64; a workflow is not Windows validation evidence. Parent owns remote
repository creation, publication, final URLs/digests, signing, and catalog pinning.
See VALIDATION.md for actual local evidence and remaining integration checks.

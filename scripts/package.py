#!/usr/bin/env python3
"""Stage a deterministic ZIP and external catalog fragment; never publish."""
import argparse
import hashlib
import json
import pathlib
import platform
import sys
import stat
import subprocess
import zipfile

TARGETS = {"aarch64-apple-darwin": "tezbar-timers", "x86_64-pc-windows-msvc": "tezbar-timers.exe"}
ROOT = pathlib.Path(__file__).resolve().parents[1]


def sha(data):
    return hashlib.sha256(data).hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", choices=TARGETS, required=True)
    parser.add_argument("--binary", type=pathlib.Path, required=True)
    parser.add_argument("--output", type=pathlib.Path, default=ROOT / "dist")
    args = parser.parse_args()
    assert (sys.platform, platform.machine().lower()) in (
        [("darwin", "arm64")] if args.target == "aarch64-apple-darwin" else [("win32", "amd64")]
    ), "Package on the declared native target runner"
    manifest = json.loads((ROOT / "extension.json").read_text(encoding="utf-8"))
    # Run the actual built target on its native runner before packaging.
    info = json.loads(subprocess.check_output([str(args.binary.resolve()), "--protocol-info"], timeout=5))
    assert info["id"] == manifest["id"] and info["version"] == manifest["version"] and info["protocol"] == 1
    assert (ROOT / manifest["icon"]).is_file(), "Original icon is required"
    if manifest["slug"]=="color-picker" and args.target=="aarch64-apple-darwin":
        assert (ROOT / "resources/sampler/color-picker-helper").is_file(), "Build the package sampler before packaging"
    executable = "bin/" + TARGETS[args.target]
    files = {executable: args.binary.read_bytes()}
    for name in ["LICENSE", "NOTICE", "README.md", "SOURCE.json"]:
        files[name] = (ROOT / name).read_bytes()
    required = ['resources/command-icon.png', 'resources/tones/alarmClock.wav', 'resources/tones/dismemberedWoodpecker.wav', 'resources/tones/fluteRiff.wav', 'resources/tones/levelUp.wav', 'resources/tones/pianoChime.wav', 'resources/tones/terminator.wav', 'scripts/generate_tones.py']
    executables = {executable}
    required.extend(path.relative_to(ROOT).as_posix() for path in sorted((ROOT / "licenses").rglob("*")) if path.is_file())
    for name in sorted(required):
        path = ROOT / name
        assert path.is_file() and not path.is_symlink(), f"Missing/unsafe package resource: {name}"
        files[name] = path.read_bytes()
    manifest["files"] = {name: sha(data) for name, data in sorted(files.items())}
    # Archive SHA cannot be included inside itself. Full artifact metadata is
    # external and trusted/pinned by host catalog; source/archive artifacts stay {}.
    files["extension.json"] = (json.dumps(manifest, indent=2, ensure_ascii=False) + "\n").encode()
    args.output.mkdir(parents=True, exist_ok=True)
    archive = args.output / f"timers-{args.target}.zip"
    with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as package:
        for name, data in sorted(files.items()):
            entry = zipfile.ZipInfo(name, date_time=(2026, 1, 1, 0, 0, 0))
            entry.create_system = 3
            entry.external_attr = ((stat.S_IFREG | (0o755 if name in executables else 0o644)) << 16)
            entry.compress_type = zipfile.ZIP_DEFLATED
            package.writestr(entry, data)
    artifact = {"url": f"{manifest['repository']}/releases/download/v{manifest['version']}/{archive.name}",
                "sha256": sha(archive.read_bytes()), "executable": executable,
                "executableSha256": manifest["files"][executable]}
    fragment = {"id": manifest["id"], "version": manifest["version"], "artifacts": {args.target: artifact}}
    metadata = args.output / f"timers-{args.target}.artifact.json"
    metadata.write_text(json.dumps(fragment, indent=2) + "\n", encoding="utf-8")
    with zipfile.ZipFile(archive) as package:
        assert set(package.namelist()) == set(files)
        for name, digest in manifest["files"].items():
            assert sha(package.read(name)) == digest
    print(json.dumps({"archive": str(archive), "artifact": str(metadata), "sha256": artifact["sha256"]}))


if __name__ == "__main__":
    main()

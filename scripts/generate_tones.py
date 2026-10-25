#!/usr/bin/env python3
"""Original additive triangle-wave alerts; deterministic integer PCM, no samples.

Apache-2.0, same as the Rust port. Historic preference IDs are filenames only;
none of the Mixkit recordings or waveforms are included or reproduced.
"""
import argparse
import pathlib
import struct

RATE = 22050
ROOT = pathlib.Path(__file__).resolve().parents[1] / 'resources' / 'tones'
MELODIES = {
    'alarmClock.wav': [880, 660, 880, 660],
    'dismemberedWoodpecker.wav': [523, 523, 784, 523, 1047],
    'fluteRiff.wav': [659, 784, 880, 784, 659],
    'levelUp.wav': [523, 659, 784, 1047],
    'pianoChime.wav': [1047, 784, 659, 523],
    'terminator.wav': [220, 330, 440, 330, 220],
}


def synth(notes):
    samples = []
    count = RATE // 5
    for note in notes:
        phase = 0
        for tick in range(count):
            phase = (phase + note * (1 << 32) // RATE) & 0xffffffff
            # Triangle oscillator and linear attack/decay; every operation integer.
            triangle = (phase >> 16)
            triangle = triangle if triangle < 32768 else 65535 - triangle
            signed = triangle * 2 - 32767
            envelope = min(tick * 32767 // (RATE // 100), (count - tick) * 32767 // count, 32767)
            samples.append(signed * envelope // 32767 // 3)
        samples.extend([0] * (RATE // 25))
    pcm = struct.pack('<' + 'h' * len(samples), *samples)
    return b'RIFF' + struct.pack('<I', 36 + len(pcm)) + b'WAVEfmt ' + struct.pack('<IHHIIHH', 16, 1, 1, RATE, RATE * 2, 2, 16) + b'data' + struct.pack('<I', len(pcm)) + pcm


def main():
    parser=argparse.ArgumentParser();parser.add_argument("--check",action="store_true");args=parser.parse_args()
    ROOT.mkdir(parents=True, exist_ok=True)
    for name, notes in MELODIES.items():
        data=synth(notes)
        if args.check:assert (ROOT/name).read_bytes()==data, "Tone differs from deterministic generator: "+name
        else:(ROOT / name).write_bytes(data)


if __name__ == '__main__':
    main()

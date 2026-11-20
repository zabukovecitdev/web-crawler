#!/usr/bin/env python3
"""Find, count, strip and baseline comments in the C# and TypeScript sources. Used by the /delete-comment skill."""
import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(subprocess.run(['git', 'rev-parse', '--show-toplevel'], capture_output=True, text=True, check=True).stdout.strip())
PATTERNS = ['*.cs', '*.ts', '*.tsx', '*.js']
SKIP = {'src/frontend/public/mockServiceWorker.js'}
DIRECTIVE = re.compile(r'^(///\s*<reference\b|/\*\*\s*@type\b)')
BASELINE = ROOT / 'comments-baseline.txt'


def source_files(paths):
    args = ['git', 'ls-files', '--', *(paths or ['.'])]
    files = subprocess.run(args, cwd=ROOT, capture_output=True, text=True, check=True).stdout.split()
    return [f for f in files if f not in SKIP and any(Path(f).match(p) for p in PATTERNS)]


def skip_string(src, i, cs):
    if cs and src.startswith('"""', i):
        return src.find('"""', i + 3) + 3
    if cs and src[i] == "'":
        m = re.match(r"'(\\.|[^'\\])'", src[i:])
        return i + (len(m.group(0)) if m else 1)
    verbatim = cs and i > 0 and src[i - 1] == '@'
    quote, j = src[i], i + 1
    while j < len(src):
        if src[j] == '\\' and not verbatim:
            j += 2
            continue
        if src[j] == quote:
            if verbatim and src[j + 1:j + 2] == quote:
                j += 2
                continue
            break
        if src[j] == '\n' and quote != '`' and not verbatim:
            break
        j += 1
    return j + 1


def spans(src, cs):
    """(start, end) of every comment that is not a tool directive."""
    out, i = [], 0
    while i < len(src):
        if src[i] in '"\'`':
            i = skip_string(src, i, cs)
        elif src.startswith('//', i):
            end = src.find('\n', i)
            end = len(src) if end < 0 else end
            out.append((i, end))
            i = end
        elif src.startswith('/*', i):
            end = src.find('*/', i + 2) + 2
            out.append((i, end))
            i = end
        else:
            i += 1
    return [s for s in out if not DIRECTIVE.match(src[s[0]:s[1]])]


def read(f):
    return (ROOT / f).read_text(encoding='utf-8')


def line_of(src, offset):
    return src.count('\n', 0, offset) + 1


def blocks(f):
    """Comments grouped like a reader sees them: consecutive // lines form one block."""
    src = read(f)
    result = []
    for a, b in spans(src, f.endswith('.cs')):
        first, last = line_of(src, a), line_of(src, b)
        text = src[a:b]
        if result and text.startswith('//') and result[-1]['text'].startswith('//') and result[-1]['end'] + 1 == first:
            result[-1]['end'] = last
            result[-1]['text'] += '\n' + text
        else:
            result.append({'file': f, 'line': first, 'end': last, 'text': text})
    lines = src.split('\n')
    for block in result:
        rest = (line for line in lines[block['end']:block['end'] + 3] if line.strip())
        own_line = lines[block['line'] - 1].strip().startswith(('//', '/*'))
        block['code'] = next(rest, '').strip() if own_line else lines[block['line'] - 1].strip()
    return result


def count(f):
    """The number SB0001 and ESLint count: one per // line and one per /* */ block."""
    return len(spans(read(f), f.endswith('.cs')))


def strip(f):
    src = read(f)
    found = spans(src, f.endswith('.cs'))
    if not found:
        return 0
    marked = list(src)
    for a, b in reversed(found):
        marked[a:b] = ['\0']
    lines = []
    for line in ''.join(marked).split('\n'):
        rest = line.replace('\0', '')
        lines.append(None if '\0' in line and not rest.strip() else (rest.rstrip() if '\0' in line else line))
    out, k = [], 0
    while k < len(lines):
        if lines[k] is not None:
            out.append(lines[k])
            k += 1
            continue
        r = k
        while r < len(lines) and lines[r] is None:
            r += 1
        after_blank = r < len(lines) and lines[r] is not None and not lines[r].strip()
        if after_blank and (not out or not out[-1].strip()):
            r += 1
        k = r
    (ROOT / f).write_text('\n'.join(out), encoding='utf-8')
    return len(found)


def cmd_inventory(args):
    everything = [b for f in source_files(args.paths) for b in blocks(f)]
    for i, b in enumerate(everything):
        b['id'] = i
    if args.json:
        print(json.dumps(everything, indent=1))
        return
    current = None
    for b in everything:
        if b['file'] != current:
            current = b['file']
            print(f'\n=== {current}')
        text = '\n    '.join(line.strip() for line in b['text'].split('\n'))
        print(f"#{b['id']} L{b['line']}: {text}\n    -> {b['code'][:140]}")


def cmd_estimate(args):
    everything = [b for f in source_files(args.paths) for b in blocks(f)]
    lines = sum(b['end'] - b['line'] + 1 for b in everything)
    files = len({b['file'] for b in everything})
    print(f'{len(everything)} comment blocks, {lines} lines, in {files} files')


def cmd_strip(args):
    removed = sum(strip(f) for f in source_files(args.paths))
    print(f'removed {removed} comments')


def cmd_baseline(args):
    counts = {f: count(f) for f in source_files(['*.cs'])}
    BASELINE.write_text(''.join(f'{f}\t{n}\n' for f, n in sorted(counts.items()) if n), encoding='utf-8')
    print(f'{BASELINE.name}: {sum(counts.values())} C# comments in {sum(1 for n in counts.values() if n)} files')
    frontend = ROOT / 'src' / 'frontend'
    flag = '--suppress-rule=local/no-comments' if args.allow_existing else '--prune-suppressions'
    subprocess.run(['npx', 'eslint', '.', flag], cwd=frontend, check=False)
    print(f'eslint-suppressions.json: updated with eslint {flag}')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    for name, fn in [('inventory', cmd_inventory), ('estimate', cmd_estimate), ('strip', cmd_strip)]:
        p = sub.add_parser(name)
        p.add_argument('paths', nargs='*', help='git pathspecs, default: the whole repository')
        p.set_defaults(fn=fn)
    sub.choices['inventory'].add_argument('--json', action='store_true')
    p = sub.add_parser('baseline', help='rewrite both ratchet baselines from the current sources')
    p.add_argument('--allow-existing', action='store_true',
                   help='also baseline comments that are not in the frontend baseline yet (only when adopting the rule)')
    p.set_defaults(fn=cmd_baseline)
    args = parser.parse_args()
    args.fn(args)


if __name__ == '__main__':
    sys.exit(main())

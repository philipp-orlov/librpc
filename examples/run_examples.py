# Copyright (c) 2026 Philipp Orlov

"""Runs every calc client example (C++, Python, TypeScript, C#) against calc_server and checks that each prints
expected_output.txt. A language whose tools are missing is skipped.

    python3 run_examples.py --server calc_server --rpcc rpcc [--cpp calc_client] [--node node --esbuild esbuild] [--dotnet dotnet]
                           [--python python3] [--work DIR]
"""

import argparse
import os
import shutil
import socket
import subprocess
import sys
import time

here = os.path.dirname(os.path.abspath(__file__))
parser = argparse.ArgumentParser()
parser.add_argument("--server", required=True)
parser.add_argument("--rpcc", required=True)
parser.add_argument("--cpp")
parser.add_argument("--python", default=sys.executable)
parser.add_argument("--node")
parser.add_argument("--esbuild")
parser.add_argument("--dotnet")
parser.add_argument("--work", default=os.path.join(here, "..", "build-examples"))
args = parser.parse_args()
work = os.path.abspath(args.work)
shutil.rmtree(work, ignore_errors=True)
os.makedirs(work)
expected = open(os.path.join(here, "expected_output.txt")).read()
calc = os.path.join(here, "calc.bufs")


def run(cmd, **kw):
    return subprocess.run(cmd, capture_output=True, text=True, timeout=300, **kw)


def generate(flag, out):
    os.makedirs(out, exist_ok=True)
    r = run([args.rpcc, "--no-cpp", flag, "-o", out, calc])
    if r.returncode:
        sys.exit(f"rpcc {flag} failed: {r.stderr}")


with socket.socket() as s:
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
server = subprocess.Popen([args.server, "--port", str(port)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
url = f"ws://127.0.0.1:{port}/rpc"
for _ in range(100):
    try:
        socket.create_connection(("127.0.0.1", port), 0.1).close()
        break
    except OSError:
        time.sleep(0.05)

failures = 0
ran = []


def check(language, cmd, **kw):
    global failures
    r = run(cmd, **kw)
    if r.returncode or r.stdout != expected:
        failures += 1
        print(f"FAILED {language}: exit {r.returncode}\n--- stdout\n{r.stdout}--- stderr\n{r.stderr}")
    else:
        ran.append(language)
        print(f"ok - {language}")


try:
    if args.cpp:
        check("cpp", [args.cpp, url])

    generate("--py", os.path.join(work, "python"))
    env = dict(os.environ, PYTHONPATH=os.path.join(work, "python"))
    # the example puts examples/python/gen on its path; the generated module is found through PYTHONPATH here
    check("python", [args.python, os.path.join(here, "python", "calc_client.py"), url], env=env)

    if args.node and args.esbuild:
        gen = os.path.join(work, "ts-gen")
        os.makedirs(gen)
        r = run([args.rpcc, "--no-cpp", "--ts", "--ts-runtime", os.path.join(here, "..", "ts", "src", "index"), "-o", gen, calc])
        if r.returncode:
            sys.exit(r.stderr)
        src = os.path.join(work, "ts-src")
        shutil.copytree(os.path.join(here, "ts"), src, ignore=shutil.ignore_patterns("dist", "gen"))
        shutil.copytree(gen, os.path.join(src, "gen"))
        # the example imports '../../ts/src/index': keep that relative layout
        layout = os.path.join(work, "layout", "examples", "ts")
        os.makedirs(os.path.dirname(layout))
        shutil.move(src, layout)
        os.symlink(os.path.abspath(os.path.join(here, "..", "ts")), os.path.join(work, "layout", "ts"))
        os.symlink(os.path.abspath(os.path.join(here, "..", "..", "libbufs")), os.path.join(work, "layout", "libbufs"))
        bundle = os.path.join(work, "calc_client.mjs")
        r = run([args.esbuild, os.path.join(layout, "calc_client.ts"), "--bundle", "--platform=node", "--format=esm", f"--outfile={bundle}", "--log-level=warning"])
        if r.returncode:
            sys.exit(r.stderr)
        check("typescript", [args.node, bundle, url])

    if args.dotnet:
        project = os.path.join(work, "cs")
        shutil.copytree(os.path.join(here, "cs", "CalcClient"), project, ignore=shutil.ignore_patterns("bin", "obj", "gen"))
        generate("--cs", os.path.join(project, "gen"))
        # the project references ../../../cs/Librpc: point it at the real one
        csproj = os.path.join(project, "CalcClient.csproj")
        text = open(csproj).read().replace("../../../cs/Librpc/Librpc.csproj", os.path.abspath(os.path.join(here, "..", "cs", "Librpc", "Librpc.csproj")))
        open(csproj, "w").write(text)
        denv = dict(os.environ, DOTNET_ROOT=os.environ.get("DOTNET_ROOT", os.path.dirname(os.path.realpath(args.dotnet))), DOTNET_CLI_TELEMETRY_OPTOUT="1", DOTNET_NOLOGO="1", DOTNET_SYSTEM_GLOBALIZATION_INVARIANT="1")
        check("csharp", [args.dotnet, "run", "--project", project, "--", url], env=denv)
finally:
    server.kill()

print(f"examples run: {', '.join(ran)}")
sys.exit(1 if failures else 0)

// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// rpcc: the librpc protocol compiler.
//
//   rpcc [-o DIR] [-I DIR]... [--ts] [--cs] [--py] [--no-cpp] [--skeleton] [--ts-runtime MODULE] [--depfile FILE] SCHEMA.bufs
//
// Reads a libbufs schema that may contain rpc_service blocks and writes into DIR:
//   <stem>.bufs.hpp      the message classes (what bufsc writes)
//   <stem>.rpc.hpp       header-only server and client stubs                      (unless --no-cpp)
//   <stem>.bufs.ts       TypeScript messages and typed clients                    (--ts)
//   <stem>.bufs.cs       C# messages and typed async clients (cs/Librpc)           (--cs)
//   <stem>_bufs.py       Python messages and typed asyncio clients (python/librpc) (--py)
//   <stem>.server.cpp, <stem>.client.cpp   programs to start from                 (--skeleton)

#include <cstdio>
#include <string>
#include <vector>

#include "rpcc.hpp"

namespace {

void usage() {
    std::fprintf(stderr,
                 "usage: rpcc [-o DIR] [-I DIR]... [--ts] [--cs] [--py] [--no-cpp] [--skeleton] [--ts-runtime MODULE]\n"
                 "            [--depfile FILE] SCHEMA.bufs\n");
}

} // namespace

int main(int argc, char** argv) {
    rpcc::Options options;
    std::vector<std::string> includeDirs;
    std::string file;
    std::string depfile;
    bool cpp = true;
    bool ts = false;
    bool cs = false;
    bool py = false;
    bool skeleton = false;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "-o" && i + 1 < argc) {
            options.outDir = argv[++i];
        } else if (arg == "-I" && i + 1 < argc) {
            includeDirs.push_back(argv[++i]);
        } else if (arg.rfind("-I", 0) == 0 && arg.size() > 2) {
            includeDirs.push_back(arg.substr(2));
        } else if (arg == "--depfile" && i + 1 < argc) {
            depfile = argv[++i];
        } else if (arg == "--ts-runtime" && i + 1 < argc) {
            options.tsRuntime = argv[++i];
        } else if (arg == "--ts") {
            ts = true;
        } else if (arg == "--cs") {
            cs = true;
        } else if (arg == "--py") {
            py = true;
        } else if (arg == "--py-runtime" && i + 1 < argc) {
            options.pyRuntime = argv[++i];
        } else if (arg == "--no-cpp") {
            cpp = false;
        } else if (arg == "--skeleton") {
            skeleton = true;
        } else if (arg == "-h" || arg == "--help") {
            usage();
            return 0;
        } else if (!arg.empty() && arg[0] == '-') {
            std::fprintf(stderr, "rpcc: unknown option %s\n", arg.c_str());
            usage();
            return 2;
        } else if (file.empty()) {
            file = arg;
        } else {
            usage();
            return 2;
        }
    }
    if (file.empty()) {
        usage();
        return 2;
    }
    try {
        const bufsc::Schema schema = bufsc::load(file, includeDirs);
        rpcc::check(schema);
        std::string primary;
        if (cpp) {
            primary = bufsc::generateCpp(schema, options.outDir);
            rpcc::generateCppStubs(schema, options);
        }
        if (ts)
            primary = rpcc::generateTypeScript(schema, options);
        if (cs)
            primary = rpcc::generateCSharp(schema, options);
        if (py)
            primary = rpcc::generatePython(schema, options);
        if (skeleton)
            rpcc::generateSkeletons(schema, options);
        if (!depfile.empty()) {
            std::FILE* out = std::fopen(depfile.c_str(), "w");
            if (!out) {
                std::fprintf(stderr, "rpcc: cannot write %s\n", depfile.c_str());
                return 1;
            }
            std::fprintf(out, "%s:", primary.c_str());
            for (const std::string& read : schema.files)
                std::fprintf(out, " %s", read.c_str());
            std::fprintf(out, "\n");
            std::fclose(out);
        }
    } catch (const bufsc::SchemaError& error) {
        std::fprintf(stderr, "rpcc: %s\n", error.what());
        return 1;
    }
    return 0;
}

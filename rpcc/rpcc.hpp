// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <string>

#include "schema.hpp"  // libbufs/bufsc

namespace rpcc {

struct Options {
    std::string outDir = ".";
    std::string tsRuntime = "@librpc/client";  // module the generated TypeScript imports the runtime from
    std::string pyRuntime;                     // module the generated Python takes the bufs runtime from (default: bufs)
    std::string wireModule;                    // set when compiling rpc.bufs itself: no runtime import
};

// "namespace.Service.Method": the text the method id hashes.
std::string methodName(const bufsc::ServiceDef& service, const bufsc::RpcMethod& method);

// Checks that every method of every service of the main file is supported and that no two ids collide.
// Throws bufsc::SchemaError.
void check(const bufsc::Schema& schema);

// Writes <stem>.rpc.hpp (stubs) next to <stem>.bufs.hpp. Returns the path written.
std::string generateCppStubs(const bufsc::Schema& schema, const Options& options);

// Writes <stem>.server.cpp and <stem>.client.cpp: starting points that build and run. Returns the server's path.
std::string generateSkeletons(const bufsc::Schema& schema, const Options& options);

// Writes <stem>.bufs.ts: types, readers, encoders and typed clients. Returns the path written.
std::string generateTypeScript(const bufsc::Schema& schema, const Options& options);

// Writes <stem>.bufs.cs: C# messages and typed async clients. Returns the path written.
std::string generateCSharp(const bufsc::Schema& schema, const Options& options);

// Writes <stem>_bufs.py: Python messages and typed asyncio clients. Returns the path written.
std::string generatePython(const bufsc::Schema& schema, const Options& options);

// Helpers shared by the generators.
bool inMainFile(const bufsc::Schema& schema, const bufsc::Definition& def);
std::string stemOf(const std::string& path);
std::string cppNamespace(const std::string& ns);
std::string cppQualified(const bufsc::Definition& def);
void writeFile(const std::string& path, const std::string& text);

} // namespace rpcc

// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#include <fstream>
#include <map>

#include "rpcc.hpp"

namespace rpcc {

std::string methodName(const bufsc::ServiceDef& service, const bufsc::RpcMethod& method) {
    return service.qualified() + "." + method.name;
}

bool inMainFile(const bufsc::Schema& schema, const bufsc::Definition& def) { return def.file == schema.mainFile; }

std::string stemOf(const std::string& path) {
    const size_t slash = path.rfind('/');
    std::string name = slash == std::string::npos ? path : path.substr(slash + 1);
    const size_t dot = name.rfind('.');
    return dot == std::string::npos ? name : name.substr(0, dot);
}

std::string cppNamespace(const std::string& ns) {
    std::string out;
    for (char c : ns) {
        if (c == '.')
            out += "::";
        else
            out += c;
    }
    return out;
}

std::string cppQualified(const bufsc::Definition& def) {
    return "::" + (def.ns.empty() ? std::string() : cppNamespace(def.ns) + "::") + def.name;
}

void writeFile(const std::string& path, const std::string& text) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out)
        throw bufsc::SchemaError(path + ": cannot write");
    out << text;
}

void check(const bufsc::Schema& schema) {
    std::map<uint32_t, std::string> ids;
    // The envelope names a shared field by its slot, 64 at most.
    for (const auto& table : schema.tables)
        if (inMainFile(schema, *table))
            for (const bufsc::Field& f : table->fields)
                if (f.shared && f.refIndex >= 64)
                    throw bufsc::SchemaError(table->file + ":" + std::to_string(f.line) + ": " + table->name + "." + f.name +
                                             ": a (shared) field must be among the first 64 vectors, strings and tables of its table");
    for (const auto& service : schema.services) {
        if (!inMainFile(schema, *service))
            continue;
        for (const bufsc::RpcMethod& method : service->methods) {
            const std::string where = service->file + ":" + std::to_string(method.line) + ": ";
            if ((method.streaming == "client" || method.streaming == "bidi") && !method.natural)
                throw bufsc::SchemaError(where + service->name + "." + method.name + ": streaming: \"" + method.streaming +
                                         "\" needs the signature form with a stream parameter: Name(args, items: stream T)");
            if (method.streaming != "none" && method.streaming != "server" && method.streaming != "client" &&
                method.streaming != "bidi")
                throw bufsc::SchemaError(where + service->name + "." + method.name + ": unknown streaming \"" + method.streaming + "\"");
            if (method.trailerDef && !method.natural)
                throw bufsc::SchemaError(where + service->name + "." + method.name + ": a trailer is supported in the natural form only");
            if (method.trailerDef && method.natural && !method.isVoid && method.resultWrapped)
                throw bufsc::SchemaError(where + service->name + "." + method.name + ": a trailer needs a stream of tables");
            const std::string name = methodName(*service, method);
            uint32_t hash = 2166136261u;
            for (char c : name) {
                hash ^= static_cast<uint8_t>(c);
                hash *= 16777619u;
            }
            const auto [it, fresh] = ids.emplace(hash, name);
            if (!fresh)
                throw bufsc::SchemaError(where + "the method id of '" + name + "' collides with '" + it->second +
                                         "'; rename one");
        }
    }
}

} // namespace rpcc

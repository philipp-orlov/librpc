// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// <stem>_bufs.py with services: the messages come from bufsc's Python generator (libbufs/bufsc/gen_py.hpp); this adds, for
// every rpc_service, a typed asyncio client over the runtime in python/librpc.
//
//   rpc_service Svc {..}     ->  class SvcMethods (the method ids), class SvcClient (async methods; streams are returned
//                                synchronously, as RpcStream / RpcUpload / RpcDuplex)

#include "gen_py.hpp"  // libbufs/bufsc
#include "rpcc.hpp"

namespace rpcc {

namespace {

using bufsc::Base;
using bufsc::Field;
using bufsc::Type;

// Add -> add, PathLength -> path_length, HTTPGet -> http_get
std::string snake(const std::string& name) {
    std::string out;
    for (size_t i = 0; i < name.size(); ++i) {
        const char c = name[i];
        if (std::isupper(static_cast<unsigned char>(c))) {
            const bool prevLower = i > 0 && (std::islower(static_cast<unsigned char>(name[i - 1])) || std::isdigit(static_cast<unsigned char>(name[i - 1])));
            const bool nextLower = i + 1 < name.size() && std::islower(static_cast<unsigned char>(name[i + 1]));
            if (i > 0 && (prevLower || (std::isupper(static_cast<unsigned char>(name[i - 1])) && nextLower)))
                out += '_';
            out += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        } else {
            out += c;
        }
    }
    return out;
}

class RpcPyGenerator : public bufsc::PyGenerator {
public:
    using bufsc::PyGenerator::PyGenerator;

protected:
    std::string extraImports(bool services) override {
        return services ? "from librpc import RpcUpload, RpcDuplex\n" : "";
    }

    // The default of a natural-form parameter as a Python expression, or "" for none.
    std::string defaultExpr(const Field& f) {
        if (f.defaultText.empty() || f.type.vector)
            return {};
        switch (f.type.base) {
        case Base::Enum: return typeName(*f.type.enumDef) + "." + pyName(f.defaultText);
        case Base::Bool: return f.defaultText == "true" || f.defaultText == "1" ? "True" : "False";
        case Base::String:
        case Base::Struct:
        case Base::Table: return {};
        default:
            if (f.defaultText == "nan") return "float('nan')";
            if (f.defaultText == "inf" || f.defaultText == "+inf") return "float('inf')";
            if (f.defaultText == "-inf") return "float('-inf')";
            return f.defaultText;
        }
    }

    void emitService(std::ostream& out, const bufsc::ServiceDef& service) override {
        const std::string name = service.name;
        out << "class " << name << "Methods:\n    \"\"\"Method ids of " << service.qualified() << ".\"\"\"\n    SERVICE = '" << service.qualified() << "'\n";
        for (const bufsc::RpcMethod& m : service.methods) {
            uint32_t hash = 2166136261u;
            for (char c : methodName(service, m)) {
                hash ^= static_cast<uint8_t>(c);
                hash *= 16777619u;
            }
            out << "    " << pyName(m.name) << " = " << hex32(hash) << "\n";
        }
        out << "\n\nclass " << name << "Client:\n";
        doc(out, service.doc, "    ");
        out << "    def __init__(self, rpc):\n        self._rpc = rpc\n\n";
        for (const bufsc::RpcMethod& m : service.methods) {
            const std::string req = typeName(*m.requestDef);
            const std::string resp = typeName(*m.responseDef);
            const std::string id = name + "Methods." + pyName(m.name);
            const std::string fn = pyName(snake(m.name));
            if (m.natural) {
                size_t firstDefault = m.params.size();
                while (firstDefault > 0 && !defaultExpr(m.params[firstDefault - 1]).empty())
                    --firstDefault;
                std::string params = "self", dict;
                for (size_t i = 0; i < m.params.size(); ++i) {
                    const Field& f = m.params[i];
                    params += ", " + pyName(f.name);
                    if (i >= firstDefault)
                        params += "=" + defaultExpr(f);
                    dict += std::string(i ? ", " : "") + "'" + f.name + "': " + pyName(f.name);
                }
                params += ", *, timeout=None";
                const std::string request = req + ".encode({" + dict + "})";
                const std::string decode = m.isVoid ? "lambda body: None"
                                           : m.resultWrapped ? "lambda body: " + resp + ".decode(body).result"
                                                              : resp + ".decode";
                if (m.streaming == "client" || m.streaming == "bidi") {
                    const std::string itemType = typeName(*m.inputDef);
                    const std::string encode = m.inputWrapped ? "lambda item: " + itemType + ".encode({'item': item})" : itemType + ".encode";
                    if (m.streaming == "client") {
                        out << "    def " << fn << "(" << params << "):\n";
                        doc(out, m.doc, "        ");
                        out << "        return RpcUpload(self._rpc.stream(" << id << ", " << request << ", lambda body: None, timeout=timeout, decode_trailer="
                            << decode << "), " << encode << ")\n\n";
                    } else {
                        out << "    def " << fn << "(" << params << "):\n";
                        doc(out, m.doc, "        ");
                        out << "        return RpcDuplex(self._rpc.stream(" << id << ", " << request << ", " << decode << ", timeout=timeout), " << encode
                            << ")\n\n";
                    }
                    continue;
                }
                if (m.streaming == "server") {
                    out << "    def " << fn << "(" << params << "):\n";
                    doc(out, m.doc, "        ");
                    out << "        return self._rpc.stream(" << id << ", " << request << ", " << decode << ", timeout=timeout"
                        << (m.trailerDef ? ", decode_trailer=" + typeName(*m.trailerDef) + ".decode" : std::string()) << ")\n\n";
                } else {
                    out << "    async def " << fn << "(" << params << "):\n";
                    doc(out, m.doc, "        ");
                    out << "        return await self._rpc.call(" << id << ", " << request << ", " << decode << ", timeout=timeout)\n\n";
                }
                continue;
            }
            // table form
            if (m.streaming == "server") {
                out << "    def " << fn << "(self, request, *, timeout=None):\n";
                doc(out, m.doc, "        ");
                out << "        return self._rpc.stream(" << id << ", " << req << ".encode(request), " << resp << ".decode, timeout=timeout)\n\n";
            } else {
                out << "    async def " << fn << "(self, request, *, timeout=None):\n";
                doc(out, m.doc, "        ");
                out << "        return await self._rpc.call(" << id << ", " << req << ".encode(request), " << resp << ".decode, timeout=timeout)\n\n";
            }
        }
        out << "\n";
    }
};

} // namespace

std::string generatePython(const bufsc::Schema& schema, const Options& options) {
    bufsc::GenOptions gen;
    gen.outDir = options.outDir;
    gen.generator = "rpcc";
    gen.runtime = options.pyRuntime;
    const std::string path = options.outDir + "/" + stemOf(schema.mainFile) + "_bufs.py";
    writeFile(path, RpcPyGenerator(schema, gen).run());
    return path;
}

} // namespace rpcc

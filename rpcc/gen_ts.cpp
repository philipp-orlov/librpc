// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// <stem>.bufs.ts with services: the messages come from bufsc's TypeScript generator (libbufs/bufsc/gen_ts.hpp); this adds,
// for every rpc_service, a typed client over the runtime in ts/src.
//
//   rpc_service Svc {..}     ->  export const SvcMethods, export class SvcClient

#include "gen_ts.hpp"  // libbufs/bufsc
#include "rpcc.hpp"

namespace rpcc {

namespace {

using namespace bufsc::tsgen;
using bufsc::Base;
using bufsc::Field;
using bufsc::Type;

class RpcTsGenerator : public bufsc::TsGenerator {
public:
    RpcTsGenerator(const bufsc::Schema& schema, const bufsc::GenOptions& options, std::string runtime)
        : bufsc::TsGenerator(schema, options), tsRuntime_(std::move(runtime)) {}

protected:
    std::string runtimeModule() override { return tsRuntime_; }

    std::string extraImports(bool services) override {
        if (!services)
            return "";
        return "import { RpcClient, RpcStream, RpcUpload, RpcDuplex, CallOptions } from '" + tsRuntime_ + "';\n";
    }

    // ---- services -------------------------------------------------------------------------------------------

    void emitService(std::ostream& out, const bufsc::ServiceDef& service) override {
        const std::string name = service.name;
        out << "/** Method ids of " << service.qualified() << ". */\nexport const " << name << "Methods = {\n  service: '"
            << service.qualified() << "',\n";
        for (const bufsc::RpcMethod& m : service.methods) {
            uint32_t hash = 2166136261u;
            for (char c : methodName(service, m)) {
                hash ^= static_cast<uint8_t>(c);
                hash *= 16777619u;
            }
            out << "  " << m.name << ": " << hex32(hash) << ",\n";
        }
        out << "} as const;\n\n";
        doc(out, service.doc, "");
        out << "export class " << name << "Client {\n  constructor(readonly rpc: RpcClient) {}\n\n";
        for (const bufsc::RpcMethod& m : service.methods) {
            const std::string req = typeName(*m.requestDef);
            const std::string resp = typeName(*m.responseDef);
            doc(out, m.doc, "  ");
            if (m.natural) {
                size_t firstDefault = m.params.size();
                while (firstDefault > 0 && !defaultExpr(m.params[firstDefault - 1]).empty())
                    --firstDefault;
                std::string params, names;
                for (size_t i = 0; i < m.params.size(); ++i) {
                    const Field& f = m.params[i];
                    params += paramName(f.name) + ": " + initFieldType(f.type);
                    if (i >= firstDefault)
                        params += " = " + defaultExpr(f);
                    params += ", ";
                    names += (i ? ", " : "") + camel(f.name) + ": " + paramName(f.name);
                }
                const bool wrapped = m.resultWrapped;
                const std::string item = m.isVoid ? "void" : readFieldType(m.result);
                const std::string decode = m.isVoid ? "() => undefined"
                                           : wrapped ? "(body) => " + resp + ".decode(body).result"
                                                     : resp + ".decode";
                const std::string call = name + "Methods." + m.name + ", " + req + ".encode({ " + names + " }), " + decode + ", options";
                if (m.streaming == "client" || m.streaming == "bidi") {
                    // The items the caller sends: values for a wrapped item type, the table's init otherwise.
                    const std::string itemInit = m.inputWrapped ? initFieldType(m.input.type) : typeName(*m.inputDef) + "Init";
                    const std::string itemType = typeName(*m.inputDef);
                    const std::string encode = m.inputWrapped ? "(item: " + itemInit + ") => " + itemType + ".encode({ item })" : itemType + ".encode";
                    const std::string requestArgs = name + "Methods." + m.name + ", " + req + ".encode({ " + names + " })";
                    if (m.streaming == "client") {
                        out << "  " << camel(m.name) << "(" << params << "options?: CallOptions): RpcUpload<" << itemInit << ", " << item << "> {\n"
                            << "    return new RpcUpload(this.rpc.stream<never, " << item << ">(" << requestArgs << ", () => { throw new Error('no items'); }, options, "
                            << (m.isVoid ? "() => undefined as never" : decode) << "), " << encode << ");\n  }\n\n";
                    } else {
                        out << "  " << camel(m.name) << "(" << params << "options?: CallOptions): RpcDuplex<" << itemInit << ", " << item << "> {\n"
                            << "    return new RpcDuplex(this.rpc.stream<" << item << ">(" << requestArgs << ", " << decode << ", options), " << encode << ");\n  }\n\n";
                    }
                    continue;
                }
                if (m.streaming == "server" && m.trailerDef)
                    out << "  " << camel(m.name) << "(" << params << "options?: CallOptions): RpcStream<" << item << ", " << typeName(*m.trailerDef)
                        << "> {\n    return this.rpc.stream(" << call << ", " << typeName(*m.trailerDef) << ".decode);\n  }\n\n";
                else if (m.streaming == "server")
                    out << "  " << camel(m.name) << "(" << params << "options?: CallOptions): RpcStream<" << item << "> {\n    return this.rpc.stream("
                        << call << ");\n  }\n\n";
                else
                    out << "  " << camel(m.name) << "(" << params << "options?: CallOptions): Promise<" << item << "> {\n    return this.rpc.call("
                        << call << ");\n  }\n\n";
                continue;
            }
            if (m.streaming == "server") {
                out << "  " << camel(m.name) << "(request: " << req << "Init, options?: CallOptions): RpcStream<" << resp << "> {\n"
                    << "    return this.rpc.stream(" << name << "Methods." << m.name << ", " << req << ".encode(request), " << resp
                    << ".decode, options);\n  }\n\n";
            } else {
                out << "  " << camel(m.name) << "(request: " << req << "Init, options?: CallOptions): Promise<" << resp << "> {\n"
                    << "    return this.rpc.call(" << name << "Methods." << m.name << ", " << req << ".encode(request), " << resp
                    << ".decode, options);\n  }\n\n";
            }
        }
        out << "}\n\n";
    }


    std::string tsRuntime_;
};

} // namespace

std::string generateTypeScript(const bufsc::Schema& schema, const Options& options) {
    bufsc::GenOptions gen;
    gen.outDir = options.outDir;
    gen.generator = "rpcc";
    const std::string path = options.outDir + "/" + stemOf(schema.mainFile) + ".bufs.ts";
    writeFile(path, RpcTsGenerator(schema, gen, options.tsRuntime).run());
    return path;
}

} // namespace rpcc

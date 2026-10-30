// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// <stem>.bufs.cs with services: the messages come from bufsc's C# generator (libbufs/bufsc/gen_cs.hpp); this adds, for
// every rpc_service, a typed async client over the Librpc runtime (cs/Librpc).
//
//   rpc_service Svc {..}     ->  public static class SvcMethods, public sealed class SvcClient

#include "gen_cs.hpp"  // libbufs/bufsc
#include "rpcc.hpp"

namespace rpcc {

namespace {

using namespace bufsc::csgen;
using bufsc::Base;
using bufsc::Field;
using bufsc::Type;

class RpcCsGenerator : public bufsc::CsGenerator {
public:
    using bufsc::CsGenerator::CsGenerator;

protected:
    std::string extraUsings() override { return "using System.Threading.Tasks;\nusing Librpc;\n"; }

    // ---- services -----------------------------------------------------------------------------------------

    // A parameter of a client method.
    std::string paramType(const Type& t) {
        if (t.vector && t.base == Base::UInt8)
            return "byte[]";
        return initType(t);
    }

    static bool trailingDefault(const bufsc::RpcMethod& m, size_t i) {
        for (size_t k = i; k < m.params.size(); ++k)
            if (m.params[k].defaultText.empty() || m.params[k].type.vector || m.params[k].type.base == Base::String ||
                m.params[k].type.base == Base::Struct || m.params[k].type.base == Base::Table)
                return false;
        return true;
    }

    std::string defaultExpr(const Field& f) {
        const std::string& d = f.defaultText;
        switch (f.type.base) {
        case Base::Enum: return qualified(*f.type.enumDef) + "." + d;
        case Base::Bool: return d == "true" || d == "1" ? "true" : "false";
        case Base::Int64: return d + "L";
        case Base::UInt64: return d + "UL";
        case Base::Float32: return d == "nan" ? "float.NaN" : d == "inf" ? "float.PositiveInfinity" : d == "-inf" ? "float.NegativeInfinity" : d + "f";
        case Base::Float64: return d == "nan" ? "double.NaN" : d == "inf" ? "double.PositiveInfinity" : d == "-inf" ? "double.NegativeInfinity" : d;
        case Base::UInt32: return d + "u";
        case Base::Int8:
        case Base::UInt8:
        case Base::Int16:
        case Base::UInt16: return "(" + std::string(scalar(f.type.base).cs) + ")" + d;
        default: return d;
        }
    }

    // The value a response reader's `Result` property gives, as the caller wants it (spans copied to arrays).
    std::string resultType(const bufsc::RpcMethod& m) {
        if (m.isVoid)
            return "void";
        const Type& t = m.result;
        if (!m.resultWrapped)
            return qualified(*m.responseDef);
        if (t.vector && (t.base == Base::UInt8 || (t.base != Base::Bool && t.base != Base::Enum && t.base != Base::Struct && t.base != Base::String &&
                                                      t.base != Base::Table)))
            return std::string(t.base == Base::UInt8 ? "byte" : scalar(t.base).cs) + "[]";
        return readType(t);
    }

    std::string resultExpr(const bufsc::RpcMethod& m, const std::string& reader) {
        if (!m.resultWrapped)
            return reader;
        const Type& t = m.result;
        const std::string access = reader + "." + member(qualified(*m.responseDef).substr(qualified(*m.responseDef).rfind('.') + 1), "result");
        if (t.vector && t.base != Base::Bool && t.base != Base::Enum && t.base != Base::Struct && t.base != Base::String && t.base != Base::Table)
            return access + ".ToArray()";
        return access;
    }

    void emitService(std::ostream& out, const bufsc::ServiceDef& service) override {
        const std::string name = service.name;
        out << "    /// <summary>Method ids of " << service.qualified() << ".</summary>\n    public static class " << name << "Methods\n    {\n"
            << "        public const string Service = \"" << service.qualified() << "\";\n";
        for (const bufsc::RpcMethod& m : service.methods) {
            uint32_t hash = 2166136261u;
            for (char c : methodName(service, m)) {
                hash ^= static_cast<uint8_t>(c);
                hash *= 16777619u;
            }
            out << "        public const uint " << m.name << " = " << hex32(hash) << ";\n";
        }
        out << "    }\n\n";
        doc(out, service.doc, "    ");
        out << "    public sealed class " << name << "Client\n    {\n        private readonly RpcClient _rpc;\n\n"
            << "        public " << name << "Client(RpcClient rpc) { _rpc = rpc; }\n\n";
        for (const bufsc::RpcMethod& m : service.methods) {
            if (!m.natural) {
                emitTableMethod(out, service, m);
                continue;
            }
            doc(out, m.doc, "        ");
            const std::string req = qualified(*m.requestDef);
            const std::string resp = qualified(*m.responseDef);
            // parameters
            std::string params, init;
            for (size_t i = 0; i < m.params.size(); ++i) {
                const Field& f = m.params[i];
                params += paramType(f.type) + " " + local(f.name);
                if (trailingDefault(m, i))
                    params += " = " + defaultExpr(f);
                params += ", ";
                init += (i ? ", " : "") + member(m.requestDef->name + "Init", f.name) + " = " + local(f.name);
            }
            const std::string encodeRequest = req + ".Encode(new " + req + "Init { " + init + " })";
            const std::string methodRef = name + "Methods." + m.name;
            const std::string cs = pascal(m.name) + "Async";
            const bool valued = m.resultWrapped;
            const std::string itemOut = m.isVoid ? "object" : resultType(m);  // the type of a response / stream item
            std::string decodeOut = "b => " + resp + ".Decode(b)";  // the response table
            if (valued)
                decodeOut = "b => " + resultExpr(m, resp + ".Decode(b)");
            const std::string rawIn = m.inputStream ? itemInput(m) : std::string();
            if (m.streaming == "none") {
                if (m.isVoid)
                    out << "        public Task " << cs << "(" << params << "CallOptions? options = null) =>\n            _rpc.CallAsync(" << methodRef << ", "
                        << encodeRequest << ", b => " << resp << ".Decode(b), options);\n\n";
                else
                    out << "        public Task<" << itemOut << "> " << cs << "(" << params << "CallOptions? options = null) =>\n            _rpc.CallAsync("
                        << methodRef << ", " << encodeRequest << ", " << decodeOut << ", options);\n\n";
            } else if (m.streaming == "server") {
                const std::string item = m.isVoid ? "object" : itemOut;
                if (m.trailerDef)
                    out << "        public RpcStream<" << item << ", " << qualified(*m.trailerDef) << "> " << cs << "(" << params
                        << "CallOptions? options = null) =>\n            _rpc.Stream<" << item << ", " << qualified(*m.trailerDef) << ">(" << methodRef << ", "
                        << encodeRequest << ", " << decodeOut << ", options, b => " << qualified(*m.trailerDef) << ".Decode(b));\n\n";
                else
                    out << "        public RpcStream<" << item << "> " << cs << "(" << params << "CallOptions? options = null) =>\n            _rpc.Stream<"
                        << item << ">(" << methodRef << ", " << encodeRequest << ", " << decodeOut << ", options);\n\n";
            } else if (m.streaming == "client") {
                const std::string inType = m.inputWrapped ? paramType(m.input.type) : qualified(*m.inputDef) + "Init";
                const std::string encodeItem = m.inputWrapped ? "v => " + qualified(*m.inputDef) + ".Encode(new " + qualified(*m.inputDef) + "Init { Item = v })"
                                                              : qualified(*m.inputDef) + ".Encode";
                const std::string result = m.isVoid ? "object" : itemOut;
                const std::string decodeResult = m.isVoid ? "b => { " + resp + ".Decode(b); return new object(); }" : decodeOut;
                out << "        public RpcUpload<" << inType << ", " << result << "> " << cs << "(" << params << "CallOptions? options = null) =>\n"
                    << "            new RpcUpload<" << inType << ", " << result << ">(_rpc.Stream<object, " << result << ">(" << methodRef << ", " << encodeRequest
                    << ", b => throw new InvalidOperationException(\"no items\"), options, " << decodeResult << "), " << encodeItem << ");\n\n";
            } else {  // bidi
                const std::string inType = m.inputWrapped ? paramType(m.input.type) : qualified(*m.inputDef) + "Init";
                const std::string encodeItem = m.inputWrapped ? "v => " + qualified(*m.inputDef) + ".Encode(new " + qualified(*m.inputDef) + "Init { Item = v })"
                                                              : qualified(*m.inputDef) + ".Encode";
                out << "        public RpcDuplex<" << inType << ", " << itemOut << "> " << cs << "(" << params << "CallOptions? options = null) =>\n"
                    << "            new RpcDuplex<" << inType << ", " << itemOut << ">(_rpc.Stream<" << itemOut << ">(" << methodRef << ", " << encodeRequest
                    << ", " << decodeOut << ", options), " << encodeItem << ");\n\n";
            }
            (void)rawIn;
        }
        out << "    }\n\n";
    }

    std::string itemInput(const bufsc::RpcMethod& m) { return paramType(m.input.type); }

    // Table-form methods: the request and response tables are the caller's own.
    void emitTableMethod(std::ostream& out, const bufsc::ServiceDef& service, const bufsc::RpcMethod& m) {
        doc(out, m.doc, "        ");
        const std::string req = qualified(*m.requestDef);
        const std::string resp = qualified(*m.responseDef);
        const std::string cs = pascal(m.name) + "Async";
        const std::string methodRef = service.name + "Methods." + m.name;
        if (m.streaming == "server")
            out << "        public RpcStream<" << resp << "> " << cs << "(" << req << "Init request, CallOptions? options = null) =>\n            _rpc.Stream<" << resp
                << ">(" << methodRef << ", " << req << ".Encode(request), b => " << resp << ".Decode(b), options);\n\n";
        else
            out << "        public Task<" << resp << "> " << cs << "(" << req << "Init request, CallOptions? options = null) =>\n            _rpc.CallAsync(" << methodRef
                << ", " << req << ".Encode(request), b => " << resp << ".Decode(b), options);\n\n";
    }


};

} // namespace

std::string generateCSharp(const bufsc::Schema& schema, const Options& options) {
    bufsc::GenOptions gen;
    gen.outDir = options.outDir;
    gen.generator = "rpcc";
    const std::string path = options.outDir + "/" + stemOf(schema.mainFile) + ".bufs.cs";
    writeFile(path, RpcCsGenerator(schema, gen).run());
    return path;
}

} // namespace rpcc

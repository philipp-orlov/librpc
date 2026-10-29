// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// <stem>.rpc.hpp: for every rpc_service a method table, a server base class and a typed client, all inline.

#include <sstream>

#include "rpcc.hpp"

namespace rpcc {

namespace {

void docComment(std::ostream& out, const std::vector<std::string>& doc, const char* indent) {
    for (const std::string& line : doc)
        out << indent << "///" << (line.empty() ? "" : " ") << line << "\n";
}

std::string idLiteral(const std::string& name) {
    uint32_t hash = 2166136261u;
    for (char c : name) {
        hash ^= static_cast<uint8_t>(c);
        hash *= 16777619u;
    }
    char text[32];
    std::snprintf(text, sizeof text, "0x%08xu", hash);
    return text;
}


using bufsc::Base;
using bufsc::Field;
using bufsc::Type;

// ---- natural-form methods: Name(a:double, s:string):result ---------------------------------------------------
// The synthesized request table has one field per parameter, the response a field `result` (or is the result
// table itself). Types seen by the application:
//   server parameter   what the Reader accessor returns: scalars by value, std::string_view, const Struct&,
//                      bufs::Vector<E> / BoolVector / StringVector, Table::Reader -- views into the request
//   server result      scalars and structs by value, std::string_view, rpc::Span<const E> -- copied into the
//                      response before the handler's caller returns, so they may point into the request,
//                      into the service object or into static data
//   client parameter   the same write-side types; a table is passed as a function that fills it in
//   client result      scalars and structs by value; strings, vectors and tables as an rpc::Response that
//                      reads the received message in place

std::string elem(const Type& t) {
    switch (t.base) {
    case Base::Enum: return cppQualified(*t.enumDef);
    case Base::Struct: return cppQualified(*t.structDef);
    case Base::Table: return cppQualified(*t.tableDef);
    case Base::String: return "std::string_view";
    default: return bufsc::cppElementType(t);
    }
}

bool plain(const Type& t) { return !t.vector && t.base != Base::String && t.base != Base::Table; }  // scalar, enum, struct

std::string readerType(const Type& t) {
    if (t.vector) {
        if (t.base == Base::Bool)
            return "::bufs::BoolVector";
        if (t.base == Base::String)
            return "::bufs::StringVector";
        return "::bufs::Vector<" + elem(t) + ">";
    }
    if (t.base == Base::Table)
        return elem(t) + "::Reader";
    return elem(t);
}

// The write side, as a value (no const &).
std::string valueType(const Type& t) {
    if (t.vector)
        return "::rpc::Span<const " + elem(t) + ">";
    return elem(t);
}

std::string serverParam(const Type& t) { return t.base == Base::Struct && !t.vector ? "const " + elem(t) + "&" : readerType(t); }

std::string clientParam(const Type& t) {
    if (t.base == Base::Table && !t.vector)
        return "const ::rpc::Fill<" + elem(t) + ">&";
    if (t.base == Base::Struct && !t.vector)
        return "const " + elem(t) + "&";
    return valueType(t);
}

// Statement(s) storing `value` into field `field` of the table builder `builder`.
std::string store(const std::string& builder, const std::string& field, const Type& t, const std::string& value) {
    const std::string set = bufsc::cppPrefixedName("set", field);
    const std::string init = bufsc::cppPrefixedName("init", field);
    if (t.vector) {
        if (t.base == Base::Bool || t.base == Base::String)
            return "{ auto rpcList = " + builder + "." + init + "(" + value + ".size()); for (size_t rpcI = 0; rpcI < " + value +
                   ".size(); ++rpcI) rpcList.set(rpcI, " + value + "[rpcI]); }";
        return builder + "." + set + "(" + value + ".data(), " + value + ".size());";
    }
    if (t.base == Base::Table)
        return "{ auto rpcTable = " + builder + "." + init + "(); " + value + "(rpcTable); }";
    return builder + "." + set + "(" + value + ");";
}

std::string argName(const Field& f) { return bufsc::cppAccessorName(f.name); }

// "int step = 1" defaults: only a trailing run of scalar, bool and enum parameters can have them.
std::string defaultArg(const Field& f) {
    if (f.defaultText.empty() || !plain(f.type) || f.type.base == Base::Struct)
        return {};
    if (f.type.base == Base::Enum)
        return cppQualified(*f.type.enumDef) + "::" + f.defaultText;
    if (f.type.base == Base::Bool)
        return f.defaultText == "true" || f.defaultText == "1" ? "true" : "false";
    if (f.defaultText == "nan" || f.defaultText == "inf" || f.defaultText == "-inf")
        return {};
    return f.defaultText;
}

std::string paramsDecl(const bufsc::RpcMethod& m, bool server) {
    size_t firstDefault = m.params.size();
    if (!server)
        while (firstDefault > 0 && !defaultArg(m.params[firstDefault - 1]).empty())
            --firstDefault;
    std::string out;
    for (size_t i = 0; i < m.params.size(); ++i) {
        const Field& f = m.params[i];
        out += ", " + (server ? serverParam(f.type) : clientParam(f.type)) + " " + argName(f);
        if (!server && i >= firstDefault)
            out += " = " + defaultArg(f);
    }
    return out;
}

bool resultIsTable(const bufsc::RpcMethod& m) { return !m.isVoid && !m.resultWrapped; }

// The extra last parameter of a server handler (where a table result or the stream items go), or "".
bool inputStreams(const bufsc::RpcMethod& m) { return m.inputStream; }
bool outputStreams(const bufsc::RpcMethod& m) { return m.streaming == "server" || m.streaming == "bidi"; }

// The server handler's parameter that reads the client's stream.
std::string readerParam(const bufsc::RpcMethod& m) {
    const std::string item = cppQualified(*m.inputDef);
    if (!m.inputWrapped)
        return ", ::rpc::ServerReader<" + item + ">& rpcReader";
    return ", ::rpc::ValueReader<" + item + ", " + readerType(m.input.type) + ">& rpcReader";
}

// The server handler's trailing parameters: the client's stream, then where the result or the items go.
std::string serverOut(const bufsc::RpcMethod& m) {
    const std::string resp = cppQualified(*m.responseDef);
    std::string out = inputStreams(m) ? readerParam(m) : std::string();
    if (outputStreams(m)) {
        const std::string trailer = m.trailerDef ? ", " + cppQualified(*m.trailerDef) + "::Builder& rpcTrailer" : "";
        if (resultIsTable(m))
            return out + ", ::rpc::ServerWriter<" + resp + ">& rpcWriter" + trailer;
        return out + ", ::rpc::Writer<" + valueType(m.result) + ">& rpcWriter" + trailer;
    }
    return out + (resultIsTable(m) ? ", " + resp + "::Builder& rpcResult" : "");
}

std::string serverReturn(const bufsc::RpcMethod& m) {
    if (outputStreams(m) || m.isVoid || resultIsTable(m))
        return "void";
    return valueType(m.result);
}

std::string clientReturn(const bufsc::RpcMethod& m) {
    const std::string resp = cppQualified(*m.responseDef);
    if (m.streaming == "client")
        return "::rpc::ClientUpload<" + cppQualified(*m.inputDef) + ", " + resp + ">";
    if (m.streaming == "bidi")
        return "::rpc::ClientDuplex<" + cppQualified(*m.inputDef) + ", " + resp + ">";
    if (m.streaming == "server") {
        if (resultIsTable(m) && m.trailerDef)
            return "::rpc::TrailedStream<" + resp + ", " + cppQualified(*m.trailerDef) + ">";
        if (resultIsTable(m))
            return "::rpc::ClientStream<" + resp + ">";
        return "::rpc::ValueStream<" + resp + ", " + readerType(m.result) + ">";
    }
    if (m.isVoid)
        return "void";
    if (m.resultWrapped && plain(m.result))
        return elem(m.result);
    return "::rpc::Response<" + resp + ">";
}

std::string readArgs(const bufsc::RpcMethod& m) {
    std::string out;
    for (const Field& f : m.params)
        out += ", rpcRequest." + bufsc::cppAccessorName(f.name) + "()";
    return out;
}

void naturalServerDecl(std::ostream& out, const std::string& qualified, const bufsc::RpcMethod& m) {
    std::string unused = "(void)rpcContext;";
    for (const Field& f : m.params)
        unused += " (void)" + argName(f) + ";";
    if (inputStreams(m))
        unused += " (void)rpcReader;";
    if (outputStreams(m))
        unused += " (void)rpcWriter;";
    else if (resultIsTable(m))
        unused += " (void)rpcResult;";
    if (m.trailerDef)
        unused += " (void)rpcTrailer;";
    out << "    virtual " << serverReturn(m) << " " << m.name << "(::rpc::ServerContext& rpcContext" << paramsDecl(m, true)
        << serverOut(m) << ") {\n        " << unused << "\n        throw ::rpc::Error(::rpc::Code::Unimplemented, \"" << qualified << "."
        << m.name << " is not implemented\");\n    }\n";
}

// "rpcWriter" for the items of a stream going out: the table's own writer, or a value writer over the wrapper.
void writerSetup(std::ostream& out, const bufsc::RpcMethod& m, const std::string& resp) {
    if (resultIsTable(m)) {
        out << "                ::rpc::ServerWriter<" << resp << "> rpcWriter(rpcSink);\n";
    } else {
        out << "                ::rpc::ServerWriter<" << resp << "> rpcInner(rpcSink);\n"
            << "                ::rpc::Writer<" << valueType(m.result) << "> rpcWriter(\n"
            << "                    [](void* rpcContext, const " << valueType(m.result) << "& rpcValue) {\n"
            << "                        return static_cast<::rpc::ServerWriter<" << resp << ">*>(rpcContext)->write([&](" << resp
            << "::Builder& rpcB) {\n                            " << store("rpcB", "result", m.result, "rpcValue")
            << "\n                        });\n                    },\n                    &rpcInner);\n";
    }
}

void readerSetup(std::ostream& out, const bufsc::RpcMethod& m) {
    const std::string item = cppQualified(*m.inputDef);
    if (!m.inputWrapped)
        out << "                ::rpc::ServerReader<" << item << "> rpcReader(rpcIn);\n";
    else
        out << "                ::rpc::ValueReader<" << item << ", " << readerType(m.input.type) << "> rpcReader(rpcIn, [](const " << item
            << "::Reader& rpcR) -> " << readerType(m.input.type) << " { return rpcR.item(); });\n";
}

void naturalBind(std::ostream& out, const bufsc::RpcMethod& m) {
    const std::string req = cppQualified(*m.requestDef);
    const std::string resp = cppQualified(*m.responseDef);
    const std::string args = readArgs(m);
    if (m.streaming == "bidi") {
        out << "            method.bidi = [this](::rpc::ServerContext& rpcContext, const uint8_t* rpcData, size_t,\n"
            << "                                 ::rpc::StreamReader& rpcIn, ::rpc::StreamWriter& rpcSink) {\n"
            << "                " << req << "::Reader rpcRequest = ::bufs::root<" << req << ">(rpcData);\n"
            << "                (void)rpcRequest;\n";
        readerSetup(out, m);
        writerSetup(out, m, resp);
        out << "                this->" << m.name << "(rpcContext" << args << ", rpcReader, rpcWriter);\n"
            << "                return ::rpc::Status::ok();\n            };\n";
        return;
    }
    if (m.streaming == "client") {
        out << "            method.clientStream = [this](::rpc::ServerContext& rpcContext, const uint8_t* rpcData, size_t,\n"
            << "                                         ::rpc::StreamReader& rpcIn, ::bufs::MessageBuilder& rpcBuilder) {\n"
            << "                " << req << "::Reader rpcRequest = ::bufs::root<" << req << ">(rpcData);\n"
            << "                (void)rpcRequest;\n";
        readerSetup(out, m);
        out << "                " << resp << "::Builder rpcResult = rpcBuilder.initRoot<" << resp << ">();\n"
            << "                (void)rpcResult;\n";
        if (m.isVoid)
            out << "                this->" << m.name << "(rpcContext" << args << ", rpcReader);\n";
        else if (resultIsTable(m))
            out << "                this->" << m.name << "(rpcContext" << args << ", rpcReader, rpcResult);\n";
        else
            out << "                const " << (m.result.vector ? valueType(m.result) : elem(m.result)) << " rpcValue = this->" << m.name
                << "(rpcContext" << args << ", rpcReader);\n                " << store("rpcResult", "result", m.result, "rpcValue") << "\n";
        out << "                return ::rpc::Status::ok();\n            };\n";
        return;
    }
    if (m.streaming == "server") {
        out << "            method.stream = [this](::rpc::ServerContext& rpcContext, const uint8_t* rpcData, size_t,\n"
            << "                                   ::rpc::StreamWriter& rpcSink) {\n"
            << "                " << req << "::Reader rpcRequest = ::bufs::root<" << req << ">(rpcData);\n"
            << "                (void)rpcRequest;\n";
        if (resultIsTable(m)) {
            out << "                ::rpc::ServerWriter<" << resp << "> rpcWriter(rpcSink);\n";
        } else {
            out << "                ::rpc::ServerWriter<" << resp << "> rpcInner(rpcSink);\n"
                << "                ::rpc::Writer<" << valueType(m.result) << "> rpcWriter(\n"
                << "                    [](void* rpcContext, const " << valueType(m.result) << "& rpcValue) {\n"
                << "                        return static_cast<::rpc::ServerWriter<" << resp << ">*>(rpcContext)->write([&](" << resp
                << "::Builder& rpcB) {\n                            " << store("rpcB", "result", m.result, "rpcValue")
                << "\n                        });\n                    },\n                    &rpcInner);\n";
        }
        if (m.trailerDef) {
            const std::string trailer = cppQualified(*m.trailerDef);
            out << "                ::bufs::MessageBuilder rpcTrailerMessage;\n"
                << "                " << trailer << "::Builder rpcTrailer = rpcTrailerMessage.initRoot<" << trailer << ">();\n"
                << "                this->" << m.name << "(rpcContext" << args << ", rpcWriter, rpcTrailer);\n"
                << "                const auto rpcSealed = rpcTrailerMessage.finish();\n"
                << "                rpcSink.setTrailer(rpcSealed.first, rpcSealed.second);\n";
        } else {
            out << "                this->" << m.name << "(rpcContext" << args << ", rpcWriter);\n";
        }
        out << "                return ::rpc::Status::ok();\n            };\n";
        return;
    }
    out << "            method.unary = [this](::rpc::ServerContext& rpcContext, const uint8_t* rpcData, size_t,\n"
        << "                                  ::bufs::MessageBuilder& rpcBuilder) {\n"
        << "                " << req << "::Reader rpcRequest = ::bufs::root<" << req << ">(rpcData);\n"
        << "                (void)rpcRequest;\n"
        << "                " << resp << "::Builder rpcResult = rpcBuilder.initRoot<" << resp << ">();\n"
        << "                (void)rpcResult;\n";
    if (m.isVoid)
        out << "                this->" << m.name << "(rpcContext" << args << ");\n";
    else if (resultIsTable(m))
        out << "                this->" << m.name << "(rpcContext" << args << ", rpcResult);\n";
    else
        out << "                const " << (m.result.vector ? valueType(m.result) : elem(m.result)) << " rpcValue = this->" << m.name
            << "(rpcContext" << args << ");\n                " << store("rpcResult", "result", m.result, "rpcValue") << "\n";
    out << "                return ::rpc::Status::ok();\n            };\n";
}

void naturalClient(std::ostream& out, const std::string& svc, const bufsc::RpcMethod& m) {
    const std::string req = cppQualified(*m.requestDef);
    const std::string resp = cppQualified(*m.responseDef);
    const std::string ret = clientReturn(m);
    out << "    " << ret << " " << m.name << "(" << (m.params.empty() ? "" : paramsDecl(m, false).substr(2)) << (m.params.empty() ? "" : ", ")
        << "const ::rpc::CallOptions& rpcOptions = {}) {\n";
    std::string fill = "[&](" + req + "::Builder& rpcB) { (void)rpcB;";
    for (const Field& f : m.params)
        fill += " " + store("rpcB", f.name, f.type, argName(f));
    fill += " }";
    if (m.streaming == "client" || m.streaming == "bidi") {
        out << "        return " << ret << "(client_.template openCall<" << req << ">(" << svc << "Methods::" << m.name << ", " << fill
            << ", rpcOptions));\n    }\n\n";
        return;
    }
    if (m.streaming == "server") {
        const std::string call = "client_.template stream<" + req + ", " + resp + ">(" + svc + "Methods::" + m.name + ", " + fill +
                                 ", rpcOptions)";
        if (resultIsTable(m) && m.trailerDef)
            out << "        return " << ret << "(" << call << ");\n";
        else if (resultIsTable(m))
            out << "        return " << call << ";\n";
        else
            out << "        return " << ret << "(" << call << ", [](const " << resp << "::Reader& rpcR) -> " << readerType(m.result)
                << " { return rpcR.result(); });\n";
    } else {
        out << "        auto rpcResponse = client_.template unary<" << req << ", " << resp << ">(" << svc << "Methods::" << m.name << ", "
            << fill << ", rpcOptions);\n";
        if (m.isVoid)
            out << "        (void)rpcResponse;\n";
        else if (m.resultWrapped && plain(m.result))
            out << "        return rpcResponse->result();\n";
        else
            out << "        return rpcResponse;\n";
    }
    out << "    }\n\n";
}

} // namespace

std::string generateCppStubs(const bufsc::Schema& schema, const Options& options) {
    const std::string stem = stemOf(schema.mainFile);
    std::ostringstream out;
    out << "// Generated by rpcc from " << stem << ".bufs. Do not edit.\n"
        << "//\n"
        << "// Header only. Define RPC_NO_SERVER or RPC_NO_CLIENT before including it to leave a side out.\n"
        << "#pragma once\n\n"
        << "#include <cstdint>\n#include <string_view>\n#include <type_traits>\n#include <utility>\n\n"
        << "#include \"" << stem << ".bufs.hpp\"\n"
        << "#include \"rpc/status.hpp\"\n"
        << "#ifndef RPC_NO_SERVER\n#include \"rpc/server.hpp\"\n#endif\n"
        << "#ifndef RPC_NO_CLIENT\n#include \"rpc/client.hpp\"\n#endif\n\n";

    for (const auto& service : schema.services) {
        if (!inMainFile(schema, *service))
            continue;
        const std::string ns = cppNamespace(service->ns);
        if (!ns.empty())
            out << "namespace " << ns << " {\n\n";

        const std::string name = service->name;
        docComment(out, service->doc, "");
        out << "/// Method ids of " << service->qualified() << ".\n"
            << "struct " << name << "Methods {\n"
            << "    static constexpr const char* service = \"" << service->qualified() << "\";\n";
        for (const bufsc::RpcMethod& m : service->methods)
            out << "    static constexpr uint32_t " << m.name << " = " << idLiteral(methodName(*service, m)) << ";\n";
        out << "};\n\n";

        // ---- server --------------------------------------------------------------------------------------
        out << "#ifndef RPC_NO_SERVER\n"
            << "/// Server side of " << service->qualified() << ": derive and override the methods you implement; the\n"
            << "/// others answer Unimplemented. Pass the object to rpc::Listener::add or rpc::Endpoint::add.\n"
            << "class " << name << "Service : public ::rpc::Service {\npublic:\n"
            << "    std::string_view serviceName() const override { return " << name << "Methods::service; }\n\n";
        for (const bufsc::RpcMethod& m : service->methods) {
            if (m.natural) {
                docComment(out, m.doc, "    ");
                naturalServerDecl(out, service->qualified(), m);
                continue;
            }
            const std::string req = cppQualified(*m.requestDef);
            const std::string resp = cppQualified(*m.responseDef);
            const bool stream = m.streaming == "server";
            docComment(out, m.doc, "    ");
            out << "    virtual ::rpc::Status " << m.name << "(::rpc::ServerContext& context, const " << req
                << "::Reader& request, " << (stream ? "::rpc::ServerWriter<" + resp + ">&" : resp + "::Builder&")
                << " " << (stream ? "writer" : "response") << ") {\n"
                << "        (void)context; (void)request; (void)" << (stream ? "writer" : "response") << ";\n"
                << "        return ::rpc::Status(::rpc::Code::Unimplemented, \"" << service->qualified() << "." << m.name
                << " is not implemented\");\n    }\n";
        }
        out << "\n    void bind(::rpc::Registry& registry) override {\n";
        for (const bufsc::RpcMethod& m : service->methods) {
            const std::string req = cppQualified(*m.requestDef);
            const std::string resp = cppQualified(*m.responseDef);
            const bool stream = m.streaming == "server";
            out << "        {\n"
                << "            ::rpc::Method method;\n"
                << "            method.id = " << name << "Methods::" << m.name << ";\n"
                << "            method.name = \"" << methodName(*service, m) << "\";\n"
                << "            method.streaming = ::rpc::Streaming::" << (m.streaming == "server" ? "Server" : m.streaming == "client" ? "Client" : m.streaming == "bidi" ? "Bidi" : "None") << ";\n"
                << (m.attributes.count("inline") ? "            method.inlineHint = true;\n" : "")
                << "            method.requestType = " << req << "::typeId;\n"
                << "            method.verifyRequest = [](const void* data, size_t size) { return ::bufs::verify<" << req
                << ">(data, size); };\n";
            if (m.natural) {
                naturalBind(out, m);
            } else if (stream) {
                out << "            method.stream = [this](::rpc::ServerContext& context, const uint8_t* data, size_t,\n"
                    << "                                   ::rpc::StreamWriter& sink) {\n"
                    << "                " << req << "::Reader request = ::bufs::root<" << req << ">(data);\n"
                    << "                ::rpc::ServerWriter<" << resp << "> writer(sink);\n"
                    << "                return this->" << m.name << "(context, request, writer);\n"
                    << "            };\n";
            } else {
                out << "            method.unary = [this](::rpc::ServerContext& context, const uint8_t* data, size_t,\n"
                    << "                                  ::bufs::MessageBuilder& builder) {\n"
                    << "                " << req << "::Reader request = ::bufs::root<" << req << ">(data);\n"
                    << "                " << resp << "::Builder response = builder.initRoot<" << resp << ">();\n"
                    << "                return this->" << m.name << "(context, request, response);\n"
                    << "            };\n";
            }
            out << "            registry.add(std::move(method));\n        }\n";
        }
        out << "    }\n};\n#endif // RPC_NO_SERVER\n\n";

        // ---- client --------------------------------------------------------------------------------------
        out << "#ifndef RPC_NO_CLIENT\n"
            << "/// Client side of " << service->qualified() << " over an rpc::Client (a cheap, shareable handle).\n"
            << "/// Every call throws rpc::Error when it does not end in Ok.\n"
            << "class " << name << "Client {\npublic:\n"
            << "    explicit " << name << "Client(::rpc::Client client) : client_(std::move(client)) {}\n"
            << "    ::rpc::Client& connection() { return client_; }\n\n";
        for (const bufsc::RpcMethod& m : service->methods) {
            if (m.natural) {
                docComment(out, m.doc, "    ");
                naturalClient(out, name, m);
                continue;
            }
            const std::string req = cppQualified(*m.requestDef);
            const std::string resp = cppQualified(*m.responseDef);
            const bool stream = m.streaming == "server";
            const std::string result = stream ? "::rpc::ClientStream<" + resp + ">" : "::rpc::Response<" + resp + ">";
            const std::string call = stream ? "stream" : "unary";
            docComment(out, m.doc, "    ");
            out << "    /// Builds the request with `fill(" << req << "::Builder&)`.\n"
                << "    template <class Fill, class = std::enable_if_t<std::is_invocable_v<Fill&, " << req
                << "::Builder&>>>\n"
                << "    " << result << " " << m.name << "(Fill&& fill, const ::rpc::CallOptions& options = {}) {\n"
                << "        return client_.template " << call << "<" << req << ", " << resp << ">(" << name
                << "Methods::" << m.name << ", std::forward<Fill>(fill), options);\n    }\n"
                << "    /// Sends a request message built elsewhere.\n"
                << "    " << result << " " << m.name << "(const ::bufs::Message& request, const ::rpc::CallOptions& options = {}) {\n"
                << "        return client_.template " << call << "<" << resp << ">(" << name << "Methods::" << m.name
                << ", request, options);\n    }\n\n";
        }
        out << "private:\n    ::rpc::Client client_;\n};\n#endif // RPC_NO_CLIENT\n";
        if (!ns.empty())
            out << "\n} // namespace " << ns << "\n";
        out << "\n";
    }
    const std::string path = options.outDir + "/" + stem + ".rpc.hpp";
    writeFile(path, out.str());
    return path;
}

std::string generateSkeletons(const bufsc::Schema& schema, const Options& options) {
    const std::string stem = stemOf(schema.mainFile);
    std::ostringstream server, client;
    server << "// Generated by rpcc from " << stem << ".bufs: a server to start from. Fill in the TODOs.\n"
           << "// Build it with rpc_generate(... SKELETONS) or compile it against librpc, libhttp and libbufs.\n"
           << "#include <csignal>\n#include <cstdio>\n#include <cstdlib>\n\n"
           << "#include \"" << stem << ".rpc.hpp\"\n\n"
           << "namespace {\nvolatile std::sig_atomic_t g_stop = 0;\nvoid onSignal(int) { g_stop = 1; }\n}\n\n";
    client << "// Generated by rpcc from " << stem << ".bufs: a client to start from. Fill in the TODOs.\n"
           << "#include <cstdio>\n\n#include \"" << stem << ".rpc.hpp\"\n\n"
           << "int main(int argc, char** argv) {\n"
           << "    const char* url = argc > 1 ? argv[1] : \"ws://127.0.0.1:8080/rpc\";\n"
           << "    try {\n        rpc::Client connection = rpc::Client::connect(url);\n";
    std::string registrations;
    for (const auto& service : schema.services) {
        if (!inMainFile(schema, *service))
            continue;
        const std::string ns = cppNamespace(service->ns);
        const std::string qualified = (ns.empty() ? "" : "::" + ns) + "::" + service->name;
        if (!ns.empty())
            server << "namespace " << ns << " {\n";
        server << "class " << service->name << "Impl : public " << qualified << "Service {\npublic:\n";
        for (const bufsc::RpcMethod& m : service->methods) {
            if (m.natural) {
                std::string unused = "(void)rpcContext;";
                for (const Field& f : m.params)
                    unused += " (void)" + argName(f) + ";";
                if (!serverOut(m).empty())
                    unused += resultIsTable(m) && m.streaming != "server" ? " (void)rpcResult;" : " (void)rpcWriter;";
                const std::string ret = serverReturn(m);
                server << "    " << ret << " " << m.name << "(::rpc::ServerContext& rpcContext" << paramsDecl(m, true) << serverOut(m)
                       << ") override {\n        " << unused << "\n        // TODO: implement " << service->qualified() << "." << m.name
                       << "\n" << (ret == "void" ? "" : "        return {};\n") << "    }\n";
                client << "        // TODO: call " << service->name << "Stub." << m.name << "(...): " << (m.isVoid ? "void" : "result") << "\n";
                continue;
            }
            const std::string req = cppQualified(*m.requestDef);
            const std::string resp = cppQualified(*m.responseDef);
            const bool stream = m.streaming == "server";
            server << "    ::rpc::Status " << m.name << "(::rpc::ServerContext& context, const " << req
                   << "::Reader& request, " << (stream ? "::rpc::ServerWriter<" + resp + ">& writer" : resp + "::Builder& response")
                   << ") override {\n        (void)context; (void)request;\n"
                   << "        // TODO: implement " << service->qualified() << "." << m.name << "\n"
                   << (stream ? "        (void)writer;\n" : "        (void)response;\n")
                   << "        return ::rpc::Status::ok();\n    }\n";
            client << "        {\n            " << qualified << "Client " << service->name << "Stub(connection);\n"
                   << "            // TODO: fill the request of " << service->qualified() << "." << m.name << "\n";
            if (stream)
                client << "            auto stream = " << service->name << "Stub." << m.name << "([&](" << req
                       << "::Builder&) {});\n            while (stream.next()) {\n                // TODO: use stream.reader()\n            }\n";
            else
                client << "            auto response = " << service->name << "Stub." << m.name << "([&](" << req
                       << "::Builder&) {});\n            (void)response;  // TODO: use response->...\n";
            client << "        }\n";
        }
        server << "};\n";
        if (!ns.empty())
            server << "} // namespace " << ns << "\n";
        server << "\n";
        registrations += "    " + qualified.substr(0) + "Impl " + service->name + "Impl;\n    listener.add(" +
                         service->name + "Impl);\n";
    }
    server << "int main(int argc, char** argv) {\n"
           << "    http::ServerConfig config;\n    config.host = \"0.0.0.0\";\n    config.port = argc > 1 ? static_cast<uint16_t>(std::atoi(argv[1])) : 8080;\n\n"
           << "    rpc::Listener listener(config);  // serves ws://host:port/rpc and GET /healthz\n"
           << registrations << "\n"
           << "    std::signal(SIGINT, onSignal);\n    std::signal(SIGTERM, onSignal);\n"
           << "    std::printf(\"listening on port %u\\n\", static_cast<unsigned>(config.port));\n"
           << "    listener.run([] { return g_stop != 0; });\n    return 0;\n}\n";
    client << "    } catch (const rpc::Error& error) {\n        std::fprintf(stderr, \"%s\\n\", error.what());\n"
           << "        return 1;\n    }\n    return 0;\n}\n";
    const std::string serverPath = options.outDir + "/" + stem + ".server.cpp";
    writeFile(serverPath, server.str());
    writeFile(options.outDir + "/" + stem + ".client.cpp", client.str());
    return serverPath;
}

} // namespace rpcc

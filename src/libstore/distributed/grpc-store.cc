#include "nix/store/config.hh"

#if NIX_WITH_GRPC

#  include "grpc-common.hh"
#  include "nix/store/store-registration.hh"
#  include "nix/util/callback.hh"

#  include "nix-store.grpc.pb.h"

#  include <grpcpp/grpcpp.h>

namespace nix {

struct GrpcStoreConfig : std::enable_shared_from_this<GrpcStoreConfig>, virtual StoreConfig
{
    GrpcStoreConfig(const Params & params)
        : StoreConfig(params, FilePathType::Unix)
    {
    }

    GrpcStoreConfig(std::string_view scheme, std::string_view authority, const Params & params)
        : StoreConfig(params, FilePathType::Unix)
        , target(authority)
    {
    }

    std::string target; // host:port of the gRPC server

    Setting<std::string> apiKey{
        this, "", "api-key", "App/API key sent as `authorization: ApiKey <key>` for authentication."};

    static const std::string name()
    {
        return "Distributed Store (gRPC client)";
    }

    static std::string doc()
    {
        return "A client of a distributed Nix store reached over gRPC, e.g. `grpc://builder.example.com:5570`.";
    }

    static StringSet uriSchemes()
    {
        return {"grpc"};
    }

    ref<Store> openStore() const override;

    StoreReference getReference() const override
    {
        return {
            .variant = StoreReference::Specified{.scheme = *uriSchemes().begin(), .authority = target},
            .params = getQueryParams(),
        };
    }

private:
    void anchor() override;
};

namespace grpc_transport {

struct GrpcStore : virtual Store
{
    using Config = GrpcStoreConfig;

    ref<const Config> config;

    GrpcStore(ref<const Config> config)
        : Store{*config}
        , config{config}
        , stub(pb::NixStore::NewStub(
              grpc::CreateChannel(std::string(config->target), grpc::InsecureChannelCredentials())))
    {
    }

    void anchor() override {}

    /* Attach the API key (if any) to a fresh client context. */
    void auth(grpc::ClientContext & ctx)
    {
        if (!config->apiKey.get().empty())
            ctx.AddMetadata("authorization", "ApiKey " + config->apiKey.get());
    }

    [[noreturn]] static void fail(const grpc::Status & status)
    {
        throw Error("gRPC store error: %s", status.error_message());
    }

    bool isValidPathUncached(const StorePath & path) override
    {
        grpc::ClientContext ctx;
        auth(ctx);
        pb::StorePathRequest req;
        req.set_path(printStorePath(path));
        pb::BoolReply resp;
        auto s = stub->IsValidPath(&ctx, req, &resp);
        if (!s.ok())
            fail(s);
        return resp.value();
    }

    StorePathSet queryValidPaths(const StorePathSet & paths, SubstituteFlag maybeSubstitute = NoSubstitute) override
    {
        grpc::ClientContext ctx;
        auth(ctx);
        pb::StorePathsRequest req;
        for (auto & p : paths)
            req.add_paths(printStorePath(p));
        req.set_substitute(maybeSubstitute == Substitute);
        pb::StorePathsReply resp;
        auto s = stub->QueryValidPaths(&ctx, req, &resp);
        if (!s.ok())
            fail(s);
        StorePathSet res;
        for (auto & p : resp.paths())
            res.insert(parseStorePath(p));
        return res;
    }

    void queryPathInfoUncached(
        const StorePath & path, Callback<std::shared_ptr<const ValidPathInfo>> callback) noexcept override
    {
        try {
            grpc::ClientContext ctx;
            auth(ctx);
            pb::StorePathRequest req;
            req.set_path(printStorePath(path));
            pb::PathInfoReply resp;
            auto s = stub->QueryPathInfo(&ctx, req, &resp);
            if (!s.ok())
                fail(s);
            if (resp.has_info())
                callback(std::make_shared<ValidPathInfo>(fromProto(*this, resp.info())));
            else
                callback(nullptr);
        } catch (...) {
            callback.rethrow();
        }
    }

    std::optional<StorePath> queryPathFromHashPart(const std::string & hashPart) override
    {
        grpc::ClientContext ctx;
        auth(ctx);
        pb::HashPartRequest req;
        req.set_hash_part(hashPart);
        pb::OptionalStorePathReply resp;
        auto s = stub->QueryPathFromHashPart(&ctx, req, &resp);
        if (!s.ok())
            fail(s);
        if (resp.has_path())
            return parseStorePath(resp.path());
        return std::nullopt;
    }

    void addToStore(const ValidPathInfo & info, Source & source, RepairFlag, CheckSigsFlag) override
    {
        grpc::ClientContext ctx;
        auth(ctx);
        pb::PathInfoReply resp;
        auto writer = stub->AddToStore(&ctx, &resp);

        /* First message: the PathInfo. */
        pb::AddToStoreChunk head;
        toProto(*this, info, *head.mutable_info());
        if (!writer->Write(head))
            throw Error("gRPC AddToStore: failed to send path info");

        /* Then stream the NAR as chunks. */
        ChunkSink<grpc::ClientWriter<pb::AddToStoreChunk>, pb::AddToStoreChunk> sink(*writer, [](std::string_view d) {
            pb::AddToStoreChunk c;
            c.set_nar(std::string(d));
            return c;
        });
        source.drainInto(sink);
        writer->WritesDone();
        auto s = writer->Finish();
        if (!s.ok())
            fail(s);
    }

    void narFromPath(const StorePath & path, Sink & sink) override
    {
        grpc::ClientContext ctx;
        auth(ctx);
        pb::StorePathRequest req;
        req.set_path(printStorePath(path));
        auto reader = stub->NarFromPath(&ctx, req);
        pb::NarChunk chunk;
        while (reader->Read(&chunk))
            sink(chunk.nar());
        auto s = reader->Finish();
        if (!s.ok())
            fail(s);
    }

    /* --- not yet bridged over gRPC --- */
    StorePath addToStoreFromDump(
        Source &, std::string_view, FileSerialisationMethod, ContentAddressMethod, HashAlgorithm, const StorePathSet &,
        RepairFlag) override
    {
        throw Error("'addToStoreFromDump' is not yet supported over the gRPC store");
    }

    void registerDrvOutput(const Realisation &) override
    {
        throw Error("'registerDrvOutput' is not yet supported over the gRPC store");
    }

    void queryRealisationUncached(
        const DrvOutput &, Callback<std::shared_ptr<const UnkeyedRealisation>> callback) noexcept override
    {
        callback(nullptr);
    }

    ref<SourceAccessor> getFSAccessor(bool) override
    {
        throw Error("'getFSAccessor' is not yet supported over the gRPC store");
    }

    std::shared_ptr<SourceAccessor> getFSAccessor(const StorePath &, bool) override
    {
        throw Error("'getFSAccessor' is not yet supported over the gRPC store");
    }

    std::optional<TrustedFlag> isTrustedClient() override
    {
        return Trusted;
    }

private:
    std::unique_ptr<pb::NixStore::Stub> stub;
};

} // namespace grpc_transport

void GrpcStoreConfig::anchor() {}

ref<Store> GrpcStoreConfig::openStore() const
{
    return make_ref<grpc_transport::GrpcStore>(ref<const GrpcStoreConfig>(shared_from_this()));
}

static RegisterStoreImplementation<GrpcStoreConfig> regGrpcStore;

} // namespace nix

#endif // NIX_WITH_GRPC

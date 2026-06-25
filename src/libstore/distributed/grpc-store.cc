#include "nix/store/config.hh"

#if NIX_WITH_GRPC

#  include "grpc-common.hh"
#  include "nix/store/store-registration.hh"
#  include "nix/store/gc-store.hh"
#  include "nix/store/derived-path.hh"
#  include "nix/store/derivations.hh"
#  include "nix/util/callback.hh"

#  include "nix-store.grpc.pb.h"

#  include <grpcpp/grpcpp.h>
#  include <nlohmann/json.hpp>

#  include <limits>

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

    Setting<std::string> authToken{
        this, "", "auth-token", "Preshared token sent in the `auth-token` header for authentication."};

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

struct GrpcStore : virtual Store, virtual GcStore
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

    /* Attach the preshared token (if any) to a fresh client context. */
    void auth(grpc::ClientContext & ctx)
    {
        if (!config->authToken.get().empty())
            ctx.AddMetadata("auth-token", config->authToken.get());
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

    void queryReferrers(const StorePath & path, StorePathSet & referrers) override
    {
        grpc::ClientContext ctx;
        auth(ctx);
        pb::StorePathRequest req;
        req.set_path(printStorePath(path));
        pb::StorePathsReply resp;
        auto s = stub->QueryReferrers(&ctx, req, &resp);
        if (!s.ok())
            fail(s);
        for (auto & p : resp.paths())
            referrers.insert(parseStorePath(p));
    }

    StorePathSet queryValidDerivers(const StorePath & path) override
    {
        grpc::ClientContext ctx;
        auth(ctx);
        pb::StorePathRequest req;
        req.set_path(printStorePath(path));
        pb::StorePathsReply resp;
        auto s = stub->QueryValidDerivers(&ctx, req, &resp);
        if (!s.ok())
            fail(s);
        StorePathSet res;
        for (auto & p : resp.paths())
            res.insert(parseStorePath(p));
        return res;
    }

    void addSignatures(const StorePath & storePath, const std::set<Signature> & sigs) override
    {
        grpc::ClientContext ctx;
        auth(ctx);
        pb::AddSignaturesRequest req;
        req.set_path(printStorePath(storePath));
        for (auto & sig : sigs)
            req.add_sigs(sig.to_string());
        pb::Empty resp;
        auto s = stub->AddSignatures(&ctx, req, &resp);
        if (!s.ok())
            fail(s);
    }

    void registerDrvOutput(const Realisation & info) override
    {
        grpc::ClientContext ctx;
        auth(ctx);
        pb::Realisation req;
        toProto(*this, info.id, info, req);
        pb::Empty resp;
        auto s = stub->RegisterDrvOutput(&ctx, req, &resp);
        if (!s.ok())
            fail(s);
    }

    void queryRealisationUncached(
        const DrvOutput & id, Callback<std::shared_ptr<const UnkeyedRealisation>> callback) noexcept override
    {
        try {
            grpc::ClientContext ctx;
            auth(ctx);
            pb::DrvOutputRequest req;
            req.set_drv_output(id.to_string());
            pb::OptionalRealisationReply resp;
            auto s = stub->QueryRealisation(&ctx, req, &resp);
            if (!s.ok())
                fail(s);
            if (resp.has_realisation())
                callback(std::make_shared<const UnkeyedRealisation>(fromProto(*this, resp.realisation())));
            else
                callback(nullptr);
        } catch (...) {
            callback.rethrow();
        }
    }

    void addTempRoot(const StorePath & path) override
    {
        grpc::ClientContext ctx;
        auth(ctx);
        pb::StorePathRequest req;
        req.set_path(printStorePath(path));
        pb::Empty resp;
        auto s = stub->AddTempRoot(&ctx, req, &resp);
        if (!s.ok())
            fail(s);
    }

    void buildPaths(const std::vector<DerivedPath> & paths, BuildMode mode, std::shared_ptr<Store> evalStore) override
    {
        if (evalStore && evalStore.get() != this)
            throw Error("a separate evaluation store is not supported over the gRPC store");
        grpc::ClientContext ctx;
        auth(ctx);
        pb::BuildPathsRequest req;
        for (auto & p : paths)
            req.add_drvd_paths(p.to_string(*this));
        req.set_mode(grpc_transport::toProtoMode(mode));
        auto reader = stub->BuildPaths(&ctx, req);
        pb::BuildEvent ev;
        bool ok = true;
        std::string err;
        while (reader->Read(&ev)) {
            if (ev.has_result()) {
                ok = ev.result().success();
                err = ev.result().error();
            }
            // TODO: forward ev.log_line()/ev.activity() to the local logger.
        }
        auto s = reader->Finish();
        if (!s.ok())
            fail(s);
        if (!ok)
            throw Error("build failed on the remote gRPC store: %s", err);
    }

    BuildResult buildDerivation(const StorePath & drvPath, const BasicDerivation & drv, BuildMode mode) override
    {
        grpc::ClientContext ctx;
        auth(ctx);
        pb::BuildDerivationRequest req;
        req.set_drv_path(printStorePath(drvPath));
        /* Send the derivation as a JSON blob. */
        nlohmann::json j = drv;
        req.set_drv(j.dump());
        req.set_mode(grpc_transport::toProtoMode(mode));

        auto reader = stub->BuildDerivation(&ctx, req);
        pb::BuildEvent ev;
        std::optional<BuildResult> result;
        while (reader->Read(&ev)) {
            if (!ev.has_result())
                continue; // TODO: forward log/activity events
            auto & pr = ev.result();
            BuildResult br;
            if (pr.success()) {
                BuildResult::Success success;
                success.status = BuildResult::Success::Built;
                for (auto & [outputName, rm] : pr.built_outputs()) {
                    UnkeyedRealisation realisation{.outPath = parseStorePath(rm.out_path())};
                    for (auto & sig : rm.signatures())
                        realisation.signatures.insert(Signature::parse(sig));
                    success.builtOutputs.insert_or_assign(outputName, realisation);
                }
                br.inner = std::move(success);
            } else
                br.inner = BuildError(BuildResultFailureStatus::MiscFailure, "%s", pr.error());
            result = std::move(br);
        }
        auto s = reader->Finish();
        if (!s.ok())
            fail(s);
        if (!result)
            throw Error("no build result returned from the remote gRPC store");
        return *result;
    }

    Roots findRoots(bool censor) override
    {
        grpc::ClientContext ctx;
        auth(ctx);
        pb::Empty req;
        pb::RootsReply resp;
        auto s = stub->FindRoots(&ctx, req, &resp);
        if (!s.ok())
            fail(s);
        Roots roots;
        for (auto & [path, links] : resp.roots()) {
            auto & set = roots[parseStorePath(path)];
            for (auto & link : links.paths())
                set.insert(link);
        }
        return roots;
    }

    void collectGarbage(const GCOptions & options, GCResults & results) override
    {
        grpc::ClientContext ctx;
        auth(ctx);
        pb::GCRequest req;
        if (options.action == GCOptions::gcReturnLive)
            req.set_action(pb::GCRequest::RETURN_LIVE);
        else if (options.action == GCOptions::gcReturnDead)
            req.set_action(pb::GCRequest::RETURN_DEAD);
        else if (options.action == GCOptions::gcDeleteSpecific) {
            req.set_action(pb::GCRequest::DELETE_SPECIFIC);
            for (auto & p : std::get<GCOptions::SpecificPaths>(options.pathsToDelete).paths)
                req.add_paths_to_delete(printStorePath(p));
        } else
            req.set_action(pb::GCRequest::DELETE_DEAD);
        req.set_ignore_liveness(options.ignoreLiveness);
        if (options.maxFreed != std::numeric_limits<uint64_t>::max())
            req.set_max_freed(options.maxFreed);
        pb::GCReply resp;
        auto s = stub->CollectGarbage(&ctx, req, &resp);
        if (!s.ok())
            fail(s);
        for (auto & p : resp.paths())
            results.paths.insert(p);
        results.bytesFreed = resp.bytes_freed();
    }

    /* --- not yet bridged over gRPC --- */
    StorePath addToStoreFromDump(
        Source &, std::string_view, FileSerialisationMethod, ContentAddressMethod, HashAlgorithm, const StorePathSet &,
        RepairFlag) override
    {
        throw Error("'addToStoreFromDump' is not yet supported over the gRPC store");
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

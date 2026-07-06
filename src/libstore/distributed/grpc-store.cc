#include "nix/store/config.hh"

#if NIX_WITH_GRPC

#  include "grpc-common.hh"
#  include "nix/store/store-registration.hh"
#  include "nix/store/gc-store.hh"
#  include "nix/store/derived-path.hh"
#  include "nix/store/derivations.hh"
#  include "nix/store/content-address.hh"
#  include "nix/store/remote-fs-accessor.hh"
#  include "nix/util/file-content-address.hh"
#  include "nix/util/file-system.hh"
#  include "nix/util/serialise.hh"
#  include "nix/util/callback.hh"
#  include "nix/util/strings.hh"
#  include "nix/util/logging.hh"
#  include "nix/util/util.hh"

#  include "nix-store.grpc.pb.h"

#  include <grpcpp/grpcpp.h>
#  include <nlohmann/json.hpp>

#  include <atomic>
#  include <fcntl.h>
#  include <filesystem>
#  include <limits>
#  include <set>
#  include <unistd.h>
#  include <vector>

namespace nix {

namespace {

/* Replays streamed `BuildEvent`s from the server into the local logger, so a
   remote build renders exactly like a local one (activity tree, levels,
   progress, `-L` build-log lines). The server's activity ids are mapped onto
   fresh local `Activity` objects; RAII stops any still-open activities when
   the player goes out of scope (e.g. a mid-stream failover retry). */
struct BuildEventPlayer
{
    std::map<uint64_t, std::unique_ptr<Activity>> activities;

    void operator()(const grpc_transport::pb::BuildEvent & ev)
    {
        if (ev.has_log())
            logger->log((Verbosity) ev.log().level(), ev.log().text());
        else if (ev.has_start()) {
            auto & st = ev.start();
            auto parent = activities.find(st.parent());
            activities.emplace(
                st.id(),
                std::make_unique<Activity>(
                    *logger,
                    (Verbosity) st.level(),
                    (ActivityType) st.type(),
                    st.text(),
                    grpc_transport::fromProto(st.fields()),
                    parent != activities.end() ? parent->second->id : getCurActivity()));
        } else if (ev.has_stop())
            activities.erase(ev.stop().id());
        else if (ev.has_act_result()) {
            auto it = activities.find(ev.act_result().id());
            if (it != activities.end())
                it->second->result(
                    (ResultType) ev.act_result().type(), grpc_transport::fromProto(ev.act_result().fields()));
        }
    }
};

} // namespace

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

    Setting<std::string> nodes{
        this,
        "",
        "nodes",
        "Comma-separated list of cluster nodes (`host:port`) to fail over across. Defaults to the URI authority."};

    Setting<std::string> registry{
        this,
        "",
        "registry",
        "Path to the cluster's node registry directory (the shared store's "
        "`var/replicas`, where every node registers its address). The failover "
        "node list is read from it, so no static list is needed: "
        "`grpc://?registry=/cluster/var/replicas`. Requires the share to be "
        "reachable; an unreadable or empty registry is a fatal error (there is "
        "deliberately no fallback discovery path). Mutually exclusive with "
        "`nodes`."};

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

/* Buffers a Source to a temp file so the bytes can be replayed (re-read from
   the start) for each failover attempt. */
struct TempBuffer
{
    std::pair<AutoCloseFD, std::filesystem::path> tmp;
    AutoDelete del;

    TempBuffer(Source & source)
        : tmp(createTempFile("nix-grpc-upload"))
        , del(tmp.second)
    {
        FdSink sink(tmp.first.get());
        source.drainInto(sink);
        sink.flush();
    }

    /* A fresh Source reading from the start of the buffer. */
    FdSource replay()
    {
        if (lseek(tmp.first.get(), 0, SEEK_SET) == (off_t) -1)
            throw SysError("rewinding gRPC upload buffer");
        return FdSource(tmp.first.get());
    }
};

struct GrpcStore : virtual Store, virtual GcStore
{
    using Config = GrpcStoreConfig;

    ref<const Config> config;

    GrpcStore(ref<const Config> config)
        : Store{*config}
        , config{config}
    {
        /* The cluster nodes to fail over across: the share's node registry if
           `registry` is given, else the `nodes` parameter if given, otherwise
           the single node in the URI authority. */
        std::vector<std::string> list;
        if (auto regDir = config->registry.get(); !regDir.empty()) {
            if (!config->nodes.get().empty())
                throw UsageError("the 'registry' and 'nodes' parameters of a 'grpc://' store are mutually exclusive");
            /* Discovery from the registry the nodes maintain on the share
               (`var/replicas/<node>`, one advertised address per file). A
               client that cannot read the registry must fail: there is
               deliberately no fallback discovery path. The URI authority, if
               any, is tried first. */
            if (!config->target.empty())
                list.push_back(config->target);
            std::set<std::filesystem::path> entries; // sorted -> deterministic node order
            try {
                for (auto & entry : std::filesystem::directory_iterator(regDir))
                    if (entry.is_regular_file() && !entry.path().filename().string().starts_with("."))
                        entries.insert(entry.path());
            } catch (std::filesystem::filesystem_error & e) {
                throw Error("cannot read the cluster node registry '%s': %s", regDir, e.what());
            }
            for (auto & path : entries) {
                auto entry = grpc_transport::readReplicaEntry(path);
                if (entry.addr.empty())
                    continue;
                /* An entry older than its TTL belongs to a dead node (its
                   heartbeat stopped); see readReplicaEntry. */
                if (entry.stale) {
                    debug(
                        "ignoring stale replica registration '%s' (older than its %d s TTL)",
                        path.string(),
                        entry.ttl);
                    continue;
                }
                list.push_back(entry.addr);
            }
            if (list.empty())
                throw Error("the cluster node registry '%s' names no live nodes", regDir);
        } else
            list = config->nodes.get().empty()
                       ? std::vector<std::string>{std::string(config->target)}
                       : tokenizeString<std::vector<std::string>>(config->nodes.get(), ",");
        StringSet seen;
        for (auto & target : list) {
            if (target.empty() || !seen.insert(target).second)
                continue;
            targets.push_back(target);
            stubs.push_back(pb::NixStore::NewStub(grpc::CreateChannel(target, grpc::InsecureChannelCredentials())));
        }
        if (stubs.empty())
            throw UsageError(
                "a 'grpc://' store must name at least one node (in the authority or the 'nodes'/'registry' parameters)");
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

    /* A status that means "this node is unreachable/unhealthy" (worth trying
       another node), as opposed to the operation genuinely failing the same
       way everywhere. */
    static bool isNodeFailure(const grpc::Status & s)
    {
        auto c = s.error_code();
        return c == grpc::StatusCode::UNAVAILABLE || c == grpc::StatusCode::DEADLINE_EXCEEDED;
    }

    /* Run `fn` against the cluster, starting at the last good node and
       advancing to the next whenever a node is unreachable, cycling through
       every node before giving up. A genuine operation error is thrown
       immediately (it would fail the same on every node). `fn` MUST be safe to
       run from scratch more than once: unary calls and builds are naturally
       so, and the streaming operations buffer their payload (see below) so
       they too can simply restart on another node. Throws only once every node
       has been tried and found unreachable. */
    template<typename Fn>
    void withFailover(Fn fn)
    {
        size_t n = stubs.size();
        size_t start = currentNode.load();
        std::string lastError;
        for (size_t i = 0; i < n; ++i) {
            size_t idx = (start + i) % n;
            grpc::Status s = fn(*stubs[idx]);
            if (s.ok()) {
                currentNode.store(idx);
                return;
            }
            if (!isNodeFailure(s))
                fail(s);
            lastError = fmt("node '%s': %s", targets[idx], s.error_message());
            debug("gRPC store: %s; trying next node", lastError);
        }
        throw Error("all %d gRPC cluster nodes are unreachable (last: %s)", n, lastError);
    }

    bool isValidPathUncached(const StorePath & path) override
    {
        pb::StorePathRequest req;
        req.set_path(printStorePath(path));
        pb::BoolReply resp;
        withFailover([&](pb::NixStore::Stub & stub) {
            grpc::ClientContext ctx;
            auth(ctx);
            return stub.IsValidPath(&ctx, req, &resp);
        });
        return resp.value();
    }

    StorePathSet queryValidPaths(const StorePathSet & paths, SubstituteFlag maybeSubstitute = NoSubstitute) override
    {
        pb::StorePathsRequest req;
        for (auto & p : paths)
            req.add_paths(printStorePath(p));
        req.set_substitute(maybeSubstitute == Substitute);
        pb::StorePathsReply resp;
        withFailover([&](pb::NixStore::Stub & stub) {
            grpc::ClientContext ctx;
            auth(ctx);
            return stub.QueryValidPaths(&ctx, req, &resp);
        });
        StorePathSet res;
        for (auto & p : resp.paths())
            res.insert(parseStorePath(p));
        return res;
    }

    void queryPathInfoUncached(
        const StorePath & path, Callback<std::shared_ptr<const ValidPathInfo>> callback) noexcept override
    {
        try {
            pb::StorePathRequest req;
            req.set_path(printStorePath(path));
            pb::PathInfoReply resp;
            withFailover([&](pb::NixStore::Stub & stub) {
                grpc::ClientContext ctx;
                auth(ctx);
                return stub.QueryPathInfo(&ctx, req, &resp);
            });
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
        pb::HashPartRequest req;
        req.set_hash_part(hashPart);
        pb::OptionalStorePathReply resp;
        withFailover([&](pb::NixStore::Stub & stub) {
            grpc::ClientContext ctx;
            auth(ctx);
            return stub.QueryPathFromHashPart(&ctx, req, &resp);
        });
        if (resp.has_path())
            return parseStorePath(resp.path());
        return std::nullopt;
    }

    void addToStore(const ValidPathInfo & info, Source & source, RepairFlag, CheckSigsFlag) override
    {
        pb::AddToStoreChunk head;
        toProto(*this, info, *head.mutable_info());
        /* Buffer the NAR so the upload can be replayed verbatim on any node. */
        TempBuffer buffer(source);
        withFailover([&](pb::NixStore::Stub & stub) -> grpc::Status {
            grpc::ClientContext ctx;
            auth(ctx);
            pb::PathInfoReply resp;
            auto writer = stub.AddToStore(&ctx, &resp);
            if (!writer->Write(head))
                return writer->Finish();
            ChunkSink<grpc::ClientWriter<pb::AddToStoreChunk>, pb::AddToStoreChunk> sink(*writer, [](std::string_view d) {
                pb::AddToStoreChunk c;
                c.set_nar(std::string(d));
                return c;
            });
            buffer.replay().drainInto(sink);
            writer->WritesDone();
            return writer->Finish();
        });
    }

    void narFromPath(const StorePath & path, Sink & sink) override
    {
        pb::StorePathRequest req;
        req.set_path(printStorePath(path));
        /* Fetch the whole NAR into a temp file; only on a fully successful
           fetch do we write it to `sink`, so a mid-stream node failure just
           discards the buffer and restarts on another node. */
        auto [fd, tmpPath] = createTempFile("nix-grpc-nar");
        AutoDelete del(tmpPath);
        withFailover([&](pb::NixStore::Stub & stub) -> grpc::Status {
            grpc::ClientContext ctx;
            auth(ctx);
            if (ftruncate(fd.get(), 0) != 0 || lseek(fd.get(), 0, SEEK_SET) == (off_t) -1)
                throw SysError("resetting gRPC NAR buffer");
            FdSink fileSink(fd.get());
            auto reader = stub.NarFromPath(&ctx, req);
            pb::NarChunk chunk;
            while (reader->Read(&chunk))
                fileSink(chunk.nar());
            fileSink.flush();
            return reader->Finish();
        });
        if (lseek(fd.get(), 0, SEEK_SET) == (off_t) -1)
            throw SysError("rewinding gRPC NAR buffer");
        FdSource fileSource(fd.get());
        fileSource.drainInto(sink);
    }

    void queryReferrers(const StorePath & path, StorePathSet & referrers) override
    {
        pb::StorePathRequest req;
        req.set_path(printStorePath(path));
        pb::StorePathsReply resp;
        withFailover([&](pb::NixStore::Stub & stub) {
            grpc::ClientContext ctx;
            auth(ctx);
            return stub.QueryReferrers(&ctx, req, &resp);
        });
        for (auto & p : resp.paths())
            referrers.insert(parseStorePath(p));
    }

    StorePathSet queryValidDerivers(const StorePath & path) override
    {
        pb::StorePathRequest req;
        req.set_path(printStorePath(path));
        pb::StorePathsReply resp;
        withFailover([&](pb::NixStore::Stub & stub) {
            grpc::ClientContext ctx;
            auth(ctx);
            return stub.QueryValidDerivers(&ctx, req, &resp);
        });
        StorePathSet res;
        for (auto & p : resp.paths())
            res.insert(parseStorePath(p));
        return res;
    }

    void addSignatures(const StorePath & storePath, const std::set<Signature> & sigs) override
    {
        pb::AddSignaturesRequest req;
        req.set_path(printStorePath(storePath));
        for (auto & sig : sigs)
            req.add_sigs(sig.to_string());
        pb::Empty resp;
        withFailover([&](pb::NixStore::Stub & stub) {
            grpc::ClientContext ctx;
            auth(ctx);
            return stub.AddSignatures(&ctx, req, &resp);
        });
    }

    void registerDrvOutput(const Realisation & info) override
    {
        pb::Realisation req;
        toProto(*this, info.id, info, req);
        pb::Empty resp;
        withFailover([&](pb::NixStore::Stub & stub) {
            grpc::ClientContext ctx;
            auth(ctx);
            return stub.RegisterDrvOutput(&ctx, req, &resp);
        });
    }

    void queryRealisationUncached(
        const DrvOutput & id, Callback<std::shared_ptr<const UnkeyedRealisation>> callback) noexcept override
    {
        try {
            pb::DrvOutputRequest req;
            req.set_drv_output(id.render(*this));
            pb::OptionalRealisationReply resp;
            withFailover([&](pb::NixStore::Stub & stub) {
                grpc::ClientContext ctx;
                auth(ctx);
                return stub.QueryRealisation(&ctx, req, &resp);
            });
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
        pb::StorePathRequest req;
        req.set_path(printStorePath(path));
        pb::Empty resp;
        withFailover([&](pb::NixStore::Stub & stub) {
            grpc::ClientContext ctx;
            auth(ctx);
            return stub.AddTempRoot(&ctx, req, &resp);
        });
    }

    void buildPaths(const std::vector<DerivedPath> & paths, BuildMode mode, std::shared_ptr<Store> evalStore) override
    {
        if (evalStore && evalStore.get() != this)
            throw Error("a separate evaluation store is not supported over the gRPC store");
        pb::BuildPathsRequest req;
        for (auto & p : paths)
            req.add_drvd_paths(p.to_string(*this));
        req.set_mode(grpc_transport::toProtoMode(mode));
        req.set_verbosity(verbosity);
        /* A build is re-run from scratch on failover (already-valid outputs
           short-circuit), so the whole stream read is retriable. */
        bool ok = true;
        std::string err;
        withFailover([&](pb::NixStore::Stub & stub) -> grpc::Status {
            grpc::ClientContext ctx;
            auth(ctx);
            ok = true;
            err.clear();
            auto reader = stub.BuildPaths(&ctx, req);
            pb::BuildEvent ev;
            BuildEventPlayer replay;
            while (reader->Read(&ev)) {
                if (ev.has_result()) {
                    ok = ev.result().success();
                    err = ev.result().error();
                } else
                    replay(ev);
            }
            return reader->Finish();
        });
        if (!ok)
            throw Error("build failed on the remote gRPC store: %s", err);
    }

    std::vector<KeyedBuildResult>
    buildPathsWithResults(const std::vector<DerivedPath> & paths, BuildMode buildMode, std::shared_ptr<Store> evalStore)
        override
    {
        /* Forward the build (streams logs, throws on failure), then synthesise
           the per-path results -- mirroring RemoteStore's pre-1.34 fallback so
           that `nix build --store grpc://...` works (it uses this entry point,
           not buildPaths). */
        buildPaths(paths, buildMode, evalStore);

        Store & eval = evalStore ? *evalStore : *this;
        std::vector<KeyedBuildResult> results;
        for (auto & path : paths) {
            std::visit(
                overloaded{
                    [&](const DerivedPath::Opaque & bo) {
                        results.push_back(KeyedBuildResult{
                            {.inner{BuildResult::Success{.status = BuildResult::Success::Substituted}}}, bo});
                    },
                    [&](const DerivedPath::Built & bfd) {
                        BuildResult::Success success{.status = BuildResult::Success::Built};
                        auto drvPath = resolveDerivedPath(eval, *bfd.drvPath);
                        auto built = resolveDerivedPath(*this, bfd, evalStore.get());
                        for (auto & [output, outputPath] : built) {
                            auto outputId = DrvOutput{drvPath, output};
                            /* Prefer a registered realisation (it carries
                               signatures), but don't require one: a server
                               without the CA feature never registers any for
                               input-addressed builds. The output path was
                               resolved above either way (a genuinely unbuilt
                               CA derivation already threw there), so it is
                               safe to report. */
                            if (experimentalFeatureSettings.isEnabled(Xp::CaDerivations)) {
                                if (auto realisation = queryRealisation(outputId)) {
                                    success.builtOutputs.emplace(output, *realisation);
                                    continue;
                                }
                            }
                            success.builtOutputs.emplace(output, UnkeyedRealisation{.outPath = outputPath});
                        }
                        results.push_back(KeyedBuildResult{{.inner = std::move(success)}, bfd});
                    }},
                path.raw());
        }
        return results;
    }

    BuildResult buildDerivation(const StorePath & drvPath, const BasicDerivation & drv, BuildMode mode) override
    {
        pb::BuildDerivationRequest req;
        req.set_drv_path(printStorePath(drvPath));
        /* Send the derivation as a JSON blob. */
        nlohmann::json j = drv;
        req.set_drv(j.dump());
        req.set_mode(grpc_transport::toProtoMode(mode));
        req.set_verbosity(verbosity);

        std::optional<BuildResult> result;
        withFailover([&](pb::NixStore::Stub & stub) -> grpc::Status {
            grpc::ClientContext ctx;
            auth(ctx);
            result.reset();
            auto reader = stub.BuildDerivation(&ctx, req);
            pb::BuildEvent ev;
            BuildEventPlayer replay;
            while (reader->Read(&ev)) {
                if (!ev.has_result()) {
                    replay(ev);
                    continue;
                }
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
            return reader->Finish();
        });
        if (!result)
            throw Error("no build result returned from the remote gRPC store");
        return *result;
    }

    Roots findRoots(bool censor) override
    {
        pb::Empty req;
        pb::RootsReply resp;
        withFailover([&](pb::NixStore::Stub & stub) {
            grpc::ClientContext ctx;
            auth(ctx);
            return stub.FindRoots(&ctx, req, &resp);
        });
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
        withFailover([&](pb::NixStore::Stub & stub) {
            grpc::ClientContext ctx;
            auth(ctx);
            return stub.CollectGarbage(&ctx, req, &resp);
        });
        for (auto & p : resp.paths())
            results.paths.insert(p);
        results.bytesFreed = resp.bytes_freed();
    }

    StorePath addToStoreFromDump(
        Source & dump,
        std::string_view name,
        FileSerialisationMethod dumpMethod,
        ContentAddressMethod hashMethod,
        HashAlgorithm hashAlgo,
        const StorePathSet & references,
        RepairFlag repair) override
    {
        pb::AddDumpChunk head;
        auto * h = head.mutable_header();
        h->set_name(std::string(name));
        h->set_dump_method(std::string(renderFileSerialisationMethod(dumpMethod)));
        h->set_ca_method_with_algo(hashMethod.renderWithAlgo(hashAlgo));
        for (auto & r : references)
            h->add_references(printStorePath(r));
        h->set_repair(repair == Repair);

        /* Buffer the dump so it can be replayed on any node. */
        TempBuffer buffer(dump);
        pb::OptionalStorePathReply resp;
        withFailover([&](pb::NixStore::Stub & stub) -> grpc::Status {
            grpc::ClientContext ctx;
            auth(ctx);
            auto writer = stub.AddToStoreFromDump(&ctx, &resp);
            if (!writer->Write(head))
                return writer->Finish();
            ChunkSink<grpc::ClientWriter<pb::AddDumpChunk>, pb::AddDumpChunk> sink(*writer, [](std::string_view d) {
                pb::AddDumpChunk c;
                c.set_dump(std::string(d));
                return c;
            });
            buffer.replay().drainInto(sink);
            writer->WritesDone();
            return writer->Finish();
        });
        if (!resp.has_path())
            throw Error("gRPC AddToStoreFromDump: no store path returned");
        return parseStorePath(resp.path());
    }

    MissingPaths queryMissing(const std::vector<DerivedPath> & targets) override
    {
        pb::StorePathsRequest req;
        for (auto & t : targets)
            req.add_paths(t.to_string(*this));
        pb::QueryMissingReply resp;
        withFailover([&](pb::NixStore::Stub & stub) {
            grpc::ClientContext ctx;
            auth(ctx);
            return stub.QueryMissing(&ctx, req, &resp);
        });
        MissingPaths missing;
        for (auto & p : resp.will_build())
            missing.willBuild.insert(parseStorePath(p));
        for (auto & p : resp.will_substitute())
            missing.willSubstitute.insert(parseStorePath(p));
        for (auto & p : resp.unknown())
            missing.unknown.insert(parseStorePath(p));
        missing.downloadSize = resp.download_size();
        missing.narSize = resp.nar_size();
        return missing;
    }

    ref<SourceAccessor> getFSAccessor(bool requireValidPath) override
    {
        return make_ref<RemoteFSAccessor>(ref<Store>(shared_from_this()), requireValidPath);
    }

    std::shared_ptr<SourceAccessor> getFSAccessor(const StorePath & path, bool requireValidPath) override
    {
        return make_ref<RemoteFSAccessor>(ref<Store>(shared_from_this()), requireValidPath)->accessObject(path);
    }

    std::optional<TrustedFlag> isTrustedClient() override
    {
        return Trusted;
    }

private:
    std::vector<std::string> targets;
    std::vector<std::unique_ptr<pb::NixStore::Stub>> stubs;
    std::atomic<size_t> currentNode{0};
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

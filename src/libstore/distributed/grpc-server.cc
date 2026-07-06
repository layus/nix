#include "nix/store/config.hh"

#if NIX_WITH_GRPC

#  include "grpc-common.hh"
#  include "nix/util/logging.hh"
#  include "nix/util/error.hh"
#  include "nix/util/finally.hh"
#  include "nix/store/store-cast.hh"
#  include "nix/store/gc-store.hh"
#  include "nix/store/local-fs-store.hh"
#  include "nix/store/derived-path.hh"
#  include "nix/store/derivations.hh"
#  include "nix/store/content-address.hh"
#  include "nix/util/file-content-address.hh"
#  include "nix/util/file-system.hh"

#  include "nix-store.grpc.pb.h"

#  include <nlohmann/json.hpp>

#  include <csignal>
#  include <filesystem>
#  include <mutex>
#  include <sstream>
#  include <thread>
#  include <unistd.h>

#  include <grpcpp/grpcpp.h>
#  include <grpcpp/server_builder.h>

namespace nix::grpc_transport {

namespace {

using grpc::ServerContext;
using grpc::Status;
using grpc::StatusCode;

/* A Logger that forwards the server's log/activity stream to the client as
   structured `BuildEvent`s — the gRPC analogue of the daemon's TunnelLogger.
   Installed as the global logger for the duration of a build; its writes are
   serialised against the build thread / activity callbacks.

   Activities, stops, and results are forwarded unconditionally (they carry
   the levels; the client's own logger decides what to display). Plain log
   messages are gated on the verbosity the client sent with the request. */
struct GrpcLogger : Logger
{
    grpc::ServerWriter<pb::BuildEvent> * writer;
    Verbosity clientVerbosity;
    std::mutex mutex;

    GrpcLogger(grpc::ServerWriter<pb::BuildEvent> * writer, Verbosity clientVerbosity)
        : writer(writer)
        , clientVerbosity(clientVerbosity)
    {
    }

    void write(const pb::BuildEvent & ev)
    {
        std::lock_guard<std::mutex> lock(mutex);
        writer->Write(ev);
    }

    void logMsg(Verbosity lvl, std::string text)
    {
        pb::BuildEvent ev;
        auto & l = *ev.mutable_log();
        l.set_level(lvl);
        l.set_text(std::move(text));
        write(ev);
    }

    void log(Verbosity lvl, std::string_view s) override
    {
        if (lvl <= clientVerbosity)
            logMsg(lvl, std::string(s));
    }

    void logEI(const ErrorInfo & ei) override
    {
        if (ei.level > clientVerbosity)
            return;
        std::ostringstream oss;
        showErrorInfo(oss, ei, false);
        logMsg(ei.level, oss.str());
    }

    void startActivity(
        ActivityId act, Verbosity lvl, ActivityType type, const std::string & s, const Fields & fields, ActivityId parent)
        override
    {
        pb::BuildEvent ev;
        auto & st = *ev.mutable_start();
        st.set_id(act);
        st.set_level(lvl);
        st.set_type(type);
        st.set_text(s);
        toProto(fields, *st.mutable_fields());
        st.set_parent(parent);
        write(ev);
    }

    void stopActivity(ActivityId act) override
    {
        pb::BuildEvent ev;
        ev.mutable_stop()->set_id(act);
        write(ev);
    }

    void result(ActivityId act, ResultType type, const Fields & fields) override
    {
        pb::BuildEvent ev;
        auto & r = *ev.mutable_act_result();
        r.set_id(act);
        r.set_type(type);
        toProto(fields, *r.mutable_fields());
        write(ev);
    }
};

/* Run `body`, translating Nix exceptions into a gRPC error status. */
template<typename F>
Status guarded(F body)
{
    try {
        body();
        return Status::OK;
    } catch (Error & e) {
        return Status(StatusCode::INTERNAL, e.message());
    } catch (std::exception & e) {
        return Status(StatusCode::INTERNAL, e.what());
    }
}

struct NixStoreServiceImpl : pb::NixStore::Service
{
    ref<Store> store;
    std::string token;

    /* Serialises builds so that only one build's forwarding logger is the
       global logger at a time (the logger is process-global). */
    std::mutex buildMutex;

    NixStoreServiceImpl(ref<Store> store, std::string token)
        : store(store)
        , token(std::move(token))
    {
    }

    Status IsValidPath(ServerContext * ctx, const pb::StorePathRequest * req, pb::BoolReply * resp) override
    {
        if (auto s = checkAuth(*ctx, token); !s.ok())
            return s;
        return guarded([&]() { resp->set_value(store->isValidPath(store->parseStorePath(req->path()))); });
    }

    Status QueryValidPaths(ServerContext * ctx, const pb::StorePathsRequest * req, pb::StorePathsReply * resp) override
    {
        if (auto s = checkAuth(*ctx, token); !s.ok())
            return s;
        return guarded([&]() {
            StorePathSet paths;
            for (auto & p : req->paths())
                paths.insert(store->parseStorePath(p));
            for (auto & p : store->queryValidPaths(paths, req->substitute() ? Substitute : NoSubstitute))
                resp->add_paths(store->printStorePath(p));
        });
    }

    Status QueryPathInfo(ServerContext * ctx, const pb::StorePathRequest * req, pb::PathInfoReply * resp) override
    {
        if (auto s = checkAuth(*ctx, token); !s.ok())
            return s;
        return guarded([&]() {
            auto path = store->parseStorePath(req->path());
            if (store->isValidPath(path))
                toProto(*store, *store->queryPathInfo(path), *resp->mutable_info());
        });
    }

    Status QueryPathFromHashPart(
        ServerContext * ctx, const pb::HashPartRequest * req, pb::OptionalStorePathReply * resp) override
    {
        if (auto s = checkAuth(*ctx, token); !s.ok())
            return s;
        return guarded([&]() {
            if (auto p = store->queryPathFromHashPart(req->hash_part()))
                resp->set_path(store->printStorePath(*p));
        });
    }

    Status AddToStore(ServerContext * ctx, grpc::ServerReader<pb::AddToStoreChunk> * reader, pb::PathInfoReply * resp)
        override
    {
        if (auto s = checkAuth(*ctx, token); !s.ok())
            return s;
        return guarded([&]() {
            /* The first message carries the PathInfo; the rest are NAR bytes. */
            pb::AddToStoreChunk first;
            if (!reader->Read(&first) || !first.has_info())
                throw Error("gRPC AddToStore: missing leading PathInfo message");
            auto info = fromProto(*store, first.info());

            ChunkSource<grpc::ServerReader<pb::AddToStoreChunk>, pb::AddToStoreChunk> source(
                *reader, [](const pb::AddToStoreChunk & c) -> const std::string & { return c.nar(); });

            store->addToStore(info, source, NoRepair, NoCheckSigs);
            toProto(*store, info, *resp->mutable_info());
        });
    }

    Status NarFromPath(ServerContext * ctx, const pb::StorePathRequest * req, grpc::ServerWriter<pb::NarChunk> * writer)
        override
    {
        if (auto s = checkAuth(*ctx, token); !s.ok())
            return s;
        return guarded([&]() {
            ChunkSink<grpc::ServerWriter<pb::NarChunk>, pb::NarChunk> sink(*writer, [](std::string_view data) {
                pb::NarChunk c;
                c.set_nar(std::string(data));
                return c;
            });
            store->narFromPath(store->parseStorePath(req->path()), sink);
        });
    }

    Status QueryReferrers(ServerContext * ctx, const pb::StorePathRequest * req, pb::StorePathsReply * resp) override
    {
        if (auto s = checkAuth(*ctx, token); !s.ok())
            return s;
        return guarded([&]() {
            StorePathSet referrers;
            store->queryReferrers(store->parseStorePath(req->path()), referrers);
            for (auto & p : referrers)
                resp->add_paths(store->printStorePath(p));
        });
    }

    Status QueryValidDerivers(ServerContext * ctx, const pb::StorePathRequest * req, pb::StorePathsReply * resp) override
    {
        if (auto s = checkAuth(*ctx, token); !s.ok())
            return s;
        return guarded([&]() {
            for (auto & p : store->queryValidDerivers(store->parseStorePath(req->path())))
                resp->add_paths(store->printStorePath(p));
        });
    }

    Status AddSignatures(ServerContext * ctx, const pb::AddSignaturesRequest * req, pb::Empty *) override
    {
        if (auto s = checkAuth(*ctx, token); !s.ok())
            return s;
        return guarded([&]() {
            std::set<Signature> sigs;
            for (auto & sig : req->sigs())
                sigs.insert(Signature::parse(sig));
            store->addSignatures(store->parseStorePath(req->path()), sigs);
        });
    }

    Status RegisterDrvOutput(ServerContext * ctx, const pb::Realisation * req, pb::Empty *) override
    {
        if (auto s = checkAuth(*ctx, token); !s.ok())
            return s;
        return guarded([&]() { store->registerDrvOutput(fromProto(*store, *req)); });
    }

    Status
    QueryRealisation(ServerContext * ctx, const pb::DrvOutputRequest * req, pb::OptionalRealisationReply * resp) override
    {
        if (auto s = checkAuth(*ctx, token); !s.ok())
            return s;
        return guarded([&]() {
            auto id = DrvOutput::parse(*store, req->drv_output());
            if (auto r = store->queryRealisation(id))
                toProto(*store, id, *r, *resp->mutable_realisation());
        });
    }

    Status AddTempRoot(ServerContext * ctx, const pb::StorePathRequest * req, pb::Empty *) override
    {
        if (auto s = checkAuth(*ctx, token); !s.ok())
            return s;
        return guarded([&]() { store->addTempRoot(store->parseStorePath(req->path())); });
    }

    Status AddPermRoot(ServerContext * ctx, const pb::AddPermRootRequest * req, pb::StringReply * resp) override
    {
        if (auto s = checkAuth(*ctx, token); !s.ok())
            return s;
        return guarded([&]() {
            auto root =
                require<LocalFSStore>(*store).addPermRoot(store->parseStorePath(req->store_path()), req->gc_root());
            resp->set_value(root.string());
        });
    }

    Status FindRoots(ServerContext * ctx, const pb::Empty *, pb::RootsReply * resp) override
    {
        if (auto s = checkAuth(*ctx, token); !s.ok())
            return s;
        return guarded([&]() {
            for (auto & [path, links] : require<GcStore>(*store).findRoots(false)) {
                pb::StorePathsReply linksMsg;
                for (auto & link : links)
                    linksMsg.add_paths(link);
                (*resp->mutable_roots())[store->printStorePath(path)] = linksMsg;
            }
        });
    }

    Status CollectGarbage(ServerContext * ctx, const pb::GCRequest * req, pb::GCReply * resp) override
    {
        if (auto s = checkAuth(*ctx, token); !s.ok())
            return s;
        return guarded([&]() {
            GCOptions opts;
            auto action = req->action();
            if (action == pb::GCRequest::RETURN_LIVE)
                opts.action = GCOptions::gcReturnLive;
            else if (action == pb::GCRequest::RETURN_DEAD)
                opts.action = GCOptions::gcReturnDead;
            else if (action == pb::GCRequest::DELETE_SPECIFIC) {
                opts.action = GCOptions::gcDeleteSpecific;
                GCOptions::SpecificPaths specific;
                for (auto & p : req->paths_to_delete())
                    specific.paths.insert(store->parseStorePath(p));
                opts.pathsToDelete = specific;
            } else
                opts.action = GCOptions::gcDeleteDead;
            opts.ignoreLiveness = req->ignore_liveness();
            if (req->max_freed() > 0)
                opts.maxFreed = req->max_freed();

            GCResults results;
            require<GcStore>(*store).collectGarbage(opts, results);
            for (auto & p : results.paths)
                resp->add_paths(p);
            resp->set_bytes_freed(results.bytesFreed);
        });
    }

    Status BuildPaths(ServerContext * ctx, const pb::BuildPathsRequest * req, grpc::ServerWriter<pb::BuildEvent> * writer)
        override
    {
        if (auto s = checkAuth(*ctx, token); !s.ok())
            return s;
        return guarded([&]() {
            std::vector<DerivedPath> paths;
            for (auto & s : req->drvd_paths())
                paths.push_back(DerivedPath::parse(*store, s));

            pb::BuildEvent ev;
            {
                /* Forward the build's logs/progress to the client while it runs. */
                std::lock_guard<std::mutex> buildLock(buildMutex);
                GrpcLogger grpcLogger(writer, (Verbosity) req->verbosity());
                auto * prevLogger = logger;
                logger = &grpcLogger;
                Finally restoreLogger([&]() { logger = prevLogger; });
                try {
                    store->buildPaths(paths, fromProto(req->mode()));
                    ev.mutable_result()->set_success(true);
                } catch (Error & e) {
                    auto * r = ev.mutable_result();
                    r->set_success(false);
                    r->set_error(e.message());
                }
            }
            writer->Write(ev);
        });
    }

    Status BuildDerivation(
        ServerContext * ctx, const pb::BuildDerivationRequest * req, grpc::ServerWriter<pb::BuildEvent> * writer)
        override
    {
        if (auto s = checkAuth(*ctx, token); !s.ok())
            return s;
        return guarded([&]() {
            auto drvPath = store->parseStorePath(req->drv_path());
            /* The derivation travels as a JSON blob in `drv`. It is a
               `BasicDerivation` (what `buildDerivation` needs), serialised and
               parsed with the same `adl_serializer` on both ends. */
            BasicDerivation drv = static_cast<BasicDerivation>(nlohmann::json::parse(req->drv()));

            BuildResult result;
            {
                std::lock_guard<std::mutex> buildLock(buildMutex);
                GrpcLogger grpcLogger(writer, (Verbosity) req->verbosity());
                auto * prevLogger = logger;
                logger = &grpcLogger;
                Finally restoreLogger([&]() { logger = prevLogger; });
                result = store->buildDerivation(drvPath, drv, fromProto(req->mode()));
            }

            pb::BuildEvent ev;
            auto * r = ev.mutable_result();
            if (auto * success = result.tryGetSuccess()) {
                r->set_success(true);
                for (auto & [outputName, realisation] : success->builtOutputs) {
                    pb::Realisation rm;
                    rm.set_out_path(store->printStorePath(realisation.outPath));
                    for (auto & sig : realisation.signatures)
                        rm.add_signatures(sig.to_string());
                    (*r->mutable_built_outputs())[outputName] = rm;
                }
            } else if (auto * failure = result.tryGetFailure()) {
                r->set_success(false);
                r->set_error(failure->message());
            }
            writer->Write(ev);
        });
    }

    Status QueryMissing(ServerContext * ctx, const pb::StorePathsRequest * req, pb::QueryMissingReply * resp) override
    {
        if (auto s = checkAuth(*ctx, token); !s.ok())
            return s;
        return guarded([&]() {
            std::vector<DerivedPath> targets;
            for (auto & s : req->paths())
                targets.push_back(DerivedPath::parse(*store, s));
            auto missing = store->queryMissing(targets);
            for (auto & p : missing.willBuild)
                resp->add_will_build(store->printStorePath(p));
            for (auto & p : missing.willSubstitute)
                resp->add_will_substitute(store->printStorePath(p));
            for (auto & p : missing.unknown)
                resp->add_unknown(store->printStorePath(p));
            resp->set_download_size(missing.downloadSize);
            resp->set_nar_size(missing.narSize);
        });
    }

    Status AddToStoreFromDump(
        ServerContext * ctx, grpc::ServerReader<pb::AddDumpChunk> * reader, pb::OptionalStorePathReply * resp) override
    {
        if (auto s = checkAuth(*ctx, token); !s.ok())
            return s;
        return guarded([&]() {
            pb::AddDumpChunk first;
            if (!reader->Read(&first) || !first.has_header())
                throw Error("gRPC AddToStoreFromDump: missing leading header message");
            auto & h = first.header();

            auto dumpMethod = parseFileSerialisationMethod(h.dump_method());
            auto [hashMethod, hashAlgo] = ContentAddressMethod::parseWithAlgo(h.ca_method_with_algo());
            StorePathSet references;
            for (auto & r : h.references())
                references.insert(store->parseStorePath(r));

            ChunkSource<grpc::ServerReader<pb::AddDumpChunk>, pb::AddDumpChunk> source(
                *reader, [](const pb::AddDumpChunk & c) -> const std::string & { return c.dump(); });

            auto path = store->addToStoreFromDump(
                source, h.name(), dumpMethod, hashMethod, hashAlgo, references, h.repair() ? Repair : NoRepair);
            resp->set_path(store->printStorePath(path));
        });
    }
};

} // namespace

/* Register this node in the share's well-known registry directory
   (`<share>/var/replicas/<hostname>`, alongside `<share>/store`), advertising
   `advertise` as this node's address, so that clients can discover the whole
   cluster from the share alone (see the `registry` parameter of `grpc://`).
   Requires the backing store to expose its real filesystem (a
   `distributed://` store): a node asked to register but unable to is
   mis-configured or mis-mounted and must not serve, so failures are fatal.
   Returns the registration file, to be removed on shutdown. */
static std::filesystem::path registerReplica(Store & store, const std::string & advertise)
{
    auto * fsStore = dynamic_cast<LocalFSStore *>(&store);
    if (!fsStore)
        throw Error("cannot register replica '%s': the backing store exposes no local filesystem", advertise);

    char host[256];
    if (gethostname(host, sizeof(host)) != 0)
        throw SysError("getting the hostname for replica registration");
    host[sizeof(host) - 1] = 0;

    auto regDir = fsStore->getRealStoreDir().parent_path() / "var" / "replicas";
    std::filesystem::create_directories(regDir);
    auto regFile = regDir / host;
    writeFile(regFile.string(), advertise + "\n");
    printInfo("registered replica '%s' at %s", advertise, regFile.string());
    return regFile;
}

/* Written by the signal handler, drained by the shutdown thread: the gRPC
   Shutdown() call is not async-signal-safe, so the handler only pokes a
   pipe. */
static int shutdownPipe[2] = {-1, -1};

void runServer(ref<Store> store, const std::string & listenAddr, const std::string & token, const std::string & advertise)
{
    NixStoreServiceImpl service(store, token);
    grpc::ServerBuilder builder;
    builder.AddListeningPort(listenAddr, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    auto server = builder.BuildAndStart();
    if (!server)
        throw Error("could not start gRPC server on '%s'", listenAddr);

    /* Registration is an explicit opt-in (an advertise address is given):
       daemon-backed stores can also be served, and those must not scribble a
       registry next to the host's real /nix/store. */
    std::optional<std::filesystem::path> regFile;
    if (!advertise.empty())
        regFile = registerReplica(*store, advertise);
    Finally unregister([&]() {
        if (regFile) {
            std::error_code ec;
            std::filesystem::remove(*regFile, ec);
        }
    });

    /* Graceful shutdown on SIGTERM/SIGINT, so the registration is removed. */
    if (pipe(shutdownPipe) != 0)
        throw SysError("creating the shutdown pipe");
    struct sigaction sa = {};
    sa.sa_handler = [](int) { [[maybe_unused]] auto _ = ::write(shutdownPipe[1], "x", 1); };
    sigaction(SIGTERM, &sa, nullptr);
    sigaction(SIGINT, &sa, nullptr);
    std::thread shutdownThread([&]() {
        char c;
        if (::read(shutdownPipe[0], &c, 1) == 1)
            server->Shutdown();
    });

    printInfo("nix distributed-store gRPC server listening on %s", listenAddr);
    server->Wait();

    /* Wake the shutdown thread if the server stopped for another reason. */
    [[maybe_unused]] auto _ = ::write(shutdownPipe[1], "x", 1);
    shutdownThread.join();
    printInfo("gRPC server shut down");
}

} // namespace nix::grpc_transport

#endif // NIX_WITH_GRPC

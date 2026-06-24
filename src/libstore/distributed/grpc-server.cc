#include "nix/store/config.hh"

#if NIX_WITH_GRPC

#  include "grpc-common.hh"
#  include "nix/util/logging.hh"
#  include "nix/store/store-cast.hh"
#  include "nix/store/gc-store.hh"
#  include "nix/store/local-fs-store.hh"
#  include "nix/store/derived-path.hh"

#  include "nix-store.grpc.pb.h"

#  include <grpcpp/grpcpp.h>
#  include <grpcpp/server_builder.h>

namespace nix::grpc_transport {

namespace {

using grpc::ServerContext;
using grpc::Status;
using grpc::StatusCode;

/* Run `body`, translating Nix exceptions into a gRPC error status. */
template<typename F>
Status guarded(F body)
{
    try {
        body();
        return Status::OK;
    } catch (Error & e) {
        return Status(StatusCode::INTERNAL, e.msg());
    } catch (std::exception & e) {
        return Status(StatusCode::INTERNAL, e.what());
    }
}

struct NixStoreServiceImpl : pb::NixStore::Service
{
    ref<Store> store;
    std::string token;

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
            try {
                store->buildPaths(paths, fromProto(req->mode()));
                ev.mutable_result()->set_success(true);
            } catch (Error & e) {
                auto * r = ev.mutable_result();
                r->set_success(false);
                r->set_error(e.msg());
            }
            writer->Write(ev);
        });
    }
};

} // namespace

void runServer(ref<Store> store, const std::string & listenAddr, const std::string & token)
{
    NixStoreServiceImpl service(store, token);
    grpc::ServerBuilder builder;
    builder.AddListeningPort(listenAddr, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    auto server = builder.BuildAndStart();
    if (!server)
        throw Error("could not start gRPC server on '%s'", listenAddr);
    printInfo("nix distributed-store gRPC server listening on %s", listenAddr);
    server->Wait();
}

} // namespace nix::grpc_transport

#endif // NIX_WITH_GRPC

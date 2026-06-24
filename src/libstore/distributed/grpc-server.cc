#include "nix/store/config.hh"

#if NIX_WITH_GRPC

#  include "grpc-common.hh"
#  include "nix/util/logging.hh"

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
    std::string apiKey;

    NixStoreServiceImpl(ref<Store> store, std::string apiKey)
        : store(store)
        , apiKey(std::move(apiKey))
    {
    }

    Status IsValidPath(ServerContext * ctx, const pb::StorePathRequest * req, pb::BoolReply * resp) override
    {
        if (auto s = checkAuth(*ctx, apiKey); !s.ok())
            return s;
        return guarded([&]() { resp->set_value(store->isValidPath(store->parseStorePath(req->path()))); });
    }

    Status QueryValidPaths(ServerContext * ctx, const pb::StorePathsRequest * req, pb::StorePathsReply * resp) override
    {
        if (auto s = checkAuth(*ctx, apiKey); !s.ok())
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
        if (auto s = checkAuth(*ctx, apiKey); !s.ok())
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
        if (auto s = checkAuth(*ctx, apiKey); !s.ok())
            return s;
        return guarded([&]() {
            if (auto p = store->queryPathFromHashPart(req->hash_part()))
                resp->set_path(store->printStorePath(*p));
        });
    }

    Status AddToStore(ServerContext * ctx, grpc::ServerReader<pb::AddToStoreChunk> * reader, pb::PathInfoReply * resp)
        override
    {
        if (auto s = checkAuth(*ctx, apiKey); !s.ok())
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
        if (auto s = checkAuth(*ctx, apiKey); !s.ok())
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
};

} // namespace

void runServer(ref<Store> store, const std::string & listenAddr, const std::string & apiKey)
{
    NixStoreServiceImpl service(store, apiKey);
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

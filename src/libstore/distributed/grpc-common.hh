#pragma once
///@file
// Shared helpers for the gRPC transport: conversions between the protobuf
// messages and Nix's domain types. Internal to the distributed-store sources;
// only compiled with the `grpc` feature.

#include "nix/store/config.hh"

#if NIX_WITH_GRPC

#  include "nix/store/store-api.hh"
#  include "nix/store/path-info.hh"
#  include "nix/util/hash.hh"

#  include "nix-store.pb.h"

#  include <grpcpp/grpcpp.h>

#  include <cstring>
#  include <functional>
#  include <string_view>

namespace nix::grpc_transport {

namespace pb = nix::distributed::v1;

/**
 * Validate the request's credentials. With a non-empty `apiKey`, the call must
 * carry `authorization: ApiKey <apiKey>` in its metadata. (mTLS and OAuth
 * providers plug in here later; see grpc.md.)
 */
grpc::Status checkAuth(grpc::ServerContext & ctx, const std::string & apiKey);

/**
 * Serve `store` over gRPC on `listenAddr` (e.g. "0.0.0.0:5570"), requiring
 * `apiKey` if non-empty. Blocks until the server is shut down.
 */
void runServer(ref<Store> store, const std::string & listenAddr, const std::string & apiKey);

/** Fill a `PathInfo` message from a `ValidPathInfo`. */
inline void toProto(const StoreDirConfig & store, const ValidPathInfo & info, pb::PathInfo & out)
{
    out.set_path(store.printStorePath(info.path));
    out.set_nar_hash(info.narHash.to_string(HashFormat::SRI, true));
    out.set_nar_size(info.narSize);
    for (auto & ref : info.references)
        out.add_references(store.printStorePath(ref));
    if (info.deriver)
        out.set_deriver(store.printStorePath(*info.deriver));
    for (auto & sig : info.sigs)
        out.add_sigs(sig.to_string());
    if (info.ca)
        out.set_ca(renderContentAddress(info.ca));
    out.set_ultimate(info.ultimate);
    out.set_registration_time(info.registrationTime);
}

/** Build a `ValidPathInfo` from a `PathInfo` message. */
inline ValidPathInfo fromProto(const StoreDirConfig & store, const pb::PathInfo & in)
{
    ValidPathInfo info{
        store.parseStorePath(in.path()),
        UnkeyedValidPathInfo{store, Hash::parseAny(in.nar_hash(), std::nullopt)},
    };
    info.narSize = in.nar_size();
    if (in.has_deriver())
        info.deriver = store.parseStorePath(in.deriver());
    for (auto & ref : in.references())
        info.references.insert(store.parseStorePath(ref));
    for (auto & sig : in.sigs())
        info.sigs.insert(Signature::parse(sig));
    if (in.has_ca())
        info.ca = ContentAddress::parseOpt(in.ca());
    info.ultimate = in.ultimate();
    info.registrationTime = in.registration_time();
    return info;
}

/**
 * A `Sink` that writes each chunk of data as one stream message (via a gRPC
 * reader-writer or writer). `wrap` builds the message from a byte range.
 */
template<typename Stream, typename Msg>
struct ChunkSink : Sink
{
    Stream & stream;
    std::function<Msg(std::string_view)> wrap;

    ChunkSink(Stream & stream, std::function<Msg(std::string_view)> wrap)
        : stream(stream)
        , wrap(std::move(wrap))
    {
    }

    void operator()(std::string_view data) override
    {
        if (data.empty())
            return;
        if (!stream.Write(wrap(data)))
            throw Error("gRPC stream write failed");
    }
};

/**
 * A `Source` that yields the bytes carried by a stream of messages. `unwrap`
 * extracts the byte field from each message.
 */
template<typename Stream, typename Msg>
struct ChunkSource : Source
{
    Stream & stream;
    std::function<const std::string &(const Msg &)> unwrap;
    Msg cur;
    std::string_view pending;
    bool eof = false;

    ChunkSource(Stream & stream, std::function<const std::string &(const Msg &)> unwrap)
        : stream(stream)
        , unwrap(std::move(unwrap))
    {
    }

    size_t read(char * data, size_t len) override
    {
        while (pending.empty()) {
            if (eof || !stream.Read(&cur)) {
                eof = true;
                throw EndOfFile("end of gRPC stream");
            }
            pending = unwrap(cur);
        }
        size_t n = std::min(len, pending.size());
        memcpy(data, pending.data(), n);
        pending.remove_prefix(n);
        return n;
    }
};

} // namespace nix::grpc_transport

#endif // NIX_WITH_GRPC

#pragma once
///@file
// Shared helpers for the gRPC transport: conversions between the protobuf
// messages and Nix's domain types. Internal to the distributed-store sources;
// only compiled with the `grpc` feature.

#include "nix/store/config.hh"

#if NIX_WITH_GRPC

#  include "nix/store/store-api.hh"
#  include "nix/store/path-info.hh"
#  include "nix/store/realisation.hh"
#  include "nix/store/gc-store.hh"
#  include "nix/store/build-result.hh"
#  include "nix/util/hash.hh"

#  include "nix-store.pb.h"

#  include <grpcpp/grpcpp.h>

#  include <cstring>
#  include <functional>
#  include <string_view>

namespace nix::grpc_transport {

namespace pb = nix::distributed::v1;

/**
 * Validate the request's preshared token: with a non-empty `token`, the call
 * must carry a matching `auth-token` metadata header. Empty `token` = no auth.
 */
grpc::Status checkAuth(grpc::ServerContext & ctx, const std::string & token);

/**
 * Serve `store` over gRPC on `listenAddr` (e.g. "0.0.0.0:5570"), requiring the
 * preshared `token` if non-empty. Blocks until the server is shut down.
 */
void runServer(ref<Store> store, const std::string & listenAddr, const std::string & token);

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

/** Fill a proto `Realisation` from `id` + an (unkeyed) realisation. */
inline void toProto(const StoreDirConfig & store, const DrvOutput & id, const UnkeyedRealisation & r, pb::Realisation & out)
{
    out.set_drv_output(id.to_string());
    out.set_out_path(store.printStorePath(r.outPath));
    for (auto & sig : r.signatures)
        out.add_signatures(sig.to_string());
}

/** Build a keyed `Realisation` from a proto message. */
inline Realisation fromProto(const StoreDirConfig & store, const pb::Realisation & in)
{
    Realisation r{
        UnkeyedRealisation{.outPath = store.parseStorePath(in.out_path())},
        DrvOutput::parse(store, in.drv_output()),
    };
    for (auto & sig : in.signatures())
        r.signatures.insert(Signature::parse(sig));
    return r;
}

inline BuildMode fromProto(pb::BuildMode m)
{
    if (m == pb::REPAIR)
        return bmRepair;
    if (m == pb::CHECK)
        return bmCheck;
    return bmNormal;
}

inline pb::BuildMode toProtoMode(BuildMode m)
{
    if (m == bmRepair)
        return pb::REPAIR;
    if (m == bmCheck)
        return pb::CHECK;
    return pb::NORMAL;
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

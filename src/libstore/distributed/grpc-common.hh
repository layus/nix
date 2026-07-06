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
#  include "nix/util/logging.hh"
#  include "nix/util/file-system.hh"
#  include "nix/util/strings.hh"
#  include "nix/util/util.hh"

#  include "nix-store.pb.h"

#  include <grpcpp/grpcpp.h>
#  include <nlohmann/json.hpp>

#  include <chrono>
#  include <cstring>
#  include <filesystem>
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
 * preshared `token` if non-empty. Blocks until the server is shut down
 * (gracefully on SIGTERM/SIGINT).
 *
 * If `advertise` is non-empty, the node registers that address in the share's
 * `var/replicas/<hostname>` registry (requires the backing store to expose
 * its real filesystem, i.e. a `distributed://` store; fatal otherwise) and
 * unregisters on shutdown.
 */
void runServer(
    ref<Store> store,
    const std::string & listenAddr,
    const std::string & token,
    const std::string & advertise = "");

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
 * Structured Nix error → JSON, carried in the gRPC status `error_details`
 * (and mirrored in the wire `BuildResult` for build failures), so the client
 * can rethrow with the message, traces, and CLI exit status intact instead
 * of a flat string.
 */
inline nlohmann::json errorToJson(const BaseError & e)
{
    auto traces = nlohmann::json::array();
    for (auto & t : e.info().traces)
        traces.push_back(t.hint.str());
    return {{"msg", e.message()}, {"status", e.info().status}, {"traces", std::move(traces)}};
}

/**
 * Apply `errorToJson` data onto a freshly built client-side error.
 */
inline void errorFromJson(BaseError & e, const nlohmann::json & j)
{
    e.withExitStatus(j.value("status", 1u));
    auto & traces = j.at("traces");
    /* addTrace prepends, so re-add in reverse to restore the order. */
    for (auto it = traces.rbegin(); it != traces.rend(); ++it)
        e.addTrace({}, "%s", it->get<std::string>());
}

/**
 * Record a build failure in the wire `BuildResult`, faithfully enough for
 * `fromProtoBuildFailure` to rethrow it.
 */
inline void toProto(const BuildError & e, pb::BuildResult & out)
{
    out.set_success(false);
    out.set_error(e.message());
    out.set_failure_status((uint32_t) e.status);
    out.set_is_non_deterministic(e.isNonDeterministic);
    out.set_exit_status(e.info().status);
    for (auto & t : e.info().traces)
        out.add_traces(t.hint.str());
}

/**
 * Rebuild the `BuildError` a failed build reported, so a remote failure
 * throws (and exits) exactly like a local one.
 */
inline BuildError fromProtoBuildFailure(const pb::BuildResult & in)
{
    BuildError e(BuildError::Status(in.failure_status()), "%s", in.error());
    e.isNonDeterministic = in.is_non_deterministic();
    if (in.exit_status())
        e.withExitStatus(in.exit_status());
    auto & traces = in.traces();
    for (auto it = traces.rbegin(); it != traces.rend(); ++it)
        e.addTrace({}, "%s", *it);
    return e;
}

/**
 * Freshness of `var/replicas` registrations, TempRoots-style: a
 * registration's optional second line declares the TTL (seconds) within
 * which its node promises to refresh the file's mtime (heartbeat); an entry
 * older than its TTL belongs to a dead node — clients skip it and live
 * nodes' heartbeats sweep it away. Reasonably synchronised clocks (NTP) are
 * assumed, with the heartbeat running at TTL/3 for slack.
 */
constexpr uint64_t defaultReplicaTtl = 60;

struct ReplicaEntry
{
    std::string addr;
    uint64_t ttl = defaultReplicaTtl;
    bool stale = false;
};

/**
 * Parse a `var/replicas` registration: line 1 the advertised address,
 * optional line 2 the TTL. NB: the file is read BEFORE its mtime is checked
 * — on NFS the open() forces attribute revalidation (close-to-open
 * consistency), which keeps the staleness check honest despite attribute
 * caching.
 */
inline ReplicaEntry readReplicaEntry(const std::filesystem::path & path)
{
    ReplicaEntry res;
    auto lines = tokenizeString<std::vector<std::string>>(readFile(path.string()), "\n");
    if (!lines.empty())
        res.addr = trim(lines[0]);
    if (lines.size() > 1)
        if (auto t = string2Int<uint64_t>(trim(lines[1])))
            res.ttl = *t;
    std::error_code ec;
    auto mtime = std::filesystem::last_write_time(path, ec);
    if (!ec)
        res.stale = std::chrono::file_clock::now() - mtime > std::chrono::seconds(res.ttl);
    return res;
}

/** Logger fields → proto, for build-event forwarding. */
inline void toProto(const Logger::Fields & fields, google::protobuf::RepeatedPtrField<pb::LogField> & out)
{
    for (auto & f : fields) {
        auto & pf = *out.Add();
        if (f.type == Logger::Field::tInt)
            pf.set_num(f.i);
        else
            pf.set_str(f.s);
    }
}

/** Logger fields ← proto, for build-event replay. */
inline Logger::Fields fromProto(const google::protobuf::RepeatedPtrField<pb::LogField> & fields)
{
    Logger::Fields res;
    for (auto & pf : fields)
        res.push_back(pf.has_num() ? Logger::Field(pf.num()) : Logger::Field(pf.str()));
    return res;
}

/** Fill a proto `Realisation` from `id` + an (unkeyed) realisation. */
inline void toProto(const StoreDirConfig & store, const DrvOutput & id, const UnkeyedRealisation & r, pb::Realisation & out)
{
    out.set_drv_output(id.render(store));
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

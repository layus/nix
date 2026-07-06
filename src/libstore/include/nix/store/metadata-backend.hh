#pragma once
///@file

#include "nix/store/path.hh"
#include "nix/store/path-info.hh"
#include "nix/store/realisation.hh"
#include "nix/util/signature/local-keys.hh"

#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <string_view>

namespace nix {

/**
 * Abstract interface to the store's *metadata* database.
 *
 * This is the seam that decouples the relational metadata model (which
 * paths are valid, the reference graph, derivation outputs, and
 * realisations) from its storage engine. Today the only implementation is
 * SQLite (see `LocalStore`); the purpose of this interface is to allow a
 * second, *distributed* implementation (e.g. a Raft-replicated SQL store,
 * a distributed SQL database, or a transactional key-value store) to back
 * a multi-node, shared-store Nix cluster.
 *
 * Design notes:
 *
 *  - Operations are keyed on `StorePath`, never on a storage-engine row id.
 *    The current SQLite schema uses an integer `AUTOINCREMENT` rowid as a
 *    join key, but that id is never cached in C++ across calls (the C++
 *    caches key on `StorePath`); it is purely an internal join key within a
 *    single transaction. A distributed backend is free to key on the store
 *    path text (which is already `UNIQUE`) or on its own surrogate key.
 *
 *  - The *content* of store paths is NOT this interface's concern. Content
 *    lives on the (shared) filesystem and is immutable/content-addressed.
 *    This interface only governs metadata.
 *
 *  - Each method that bundles several writes (notably `registerValidPaths`
 *    and the `invalidate*` family) MUST be atomic: it either applies in
 *    full or not at all, and is serializable against concurrent writers.
 *    The current SQLite implementation relies on a single coarse database
 *    lock; a distributed backend must obtain the equivalent guarantee from
 *    its transaction model. The atomic boundaries are called out per method.
 *
 *  - Referential integrity that SQLite expresses declaratively today
 *    (`ON DELETE CASCADE`/`RESTRICT` on `Refs`, the `DeleteSelfRefs`
 *    trigger) is part of the *semantics* of these operations, not an
 *    implementation detail. A backend without triggers/foreign keys must
 *    enforce it inside the relevant transaction.
 */
struct MetadataBackend
{
    virtual ~MetadataBackend() = default;

    /* ------------------------------------------------------------------ *
     * Validity and path information (read)
     * ------------------------------------------------------------------ */

    /**
     * Is `path` registered as valid?
     */
    virtual bool isValidPath(const StorePath & path) = 0;

    /**
     * Look up the full metadata for `path`, including its set of
     * references. Returns null if the path is not valid.
     */
    virtual std::shared_ptr<const ValidPathInfo> queryPathInfo(const StorePath & path) = 0;

    /**
     * The set of all valid paths. Potentially large; primarily used by
     * verification and GC.
     */
    virtual StorePathSet queryAllValidPaths() = 0;

    /**
     * Find the unique valid path whose hash part equals `hashPart` (the
     * 32-character base-32 prefix of a store path name), if any.
     */
    virtual std::optional<StorePath> queryPathFromHashPart(std::string_view hashPart) = 0;

    /* ------------------------------------------------------------------ *
     * Reference graph (read)
     * ------------------------------------------------------------------ */

    /**
     * Append to `referrers` every valid path that directly references
     * `path` (the reverse edges of the reference graph).
     */
    virtual void queryReferrers(const StorePath & path, StorePathSet & referrers) = 0;

    /**
     * The valid derivations that have `path` as one of their outputs.
     */
    virtual StorePathSet queryValidDerivers(const StorePath & path) = 0;

    /**
     * For a derivation `deriver`, the statically-known mapping from output
     * name to output path (entries may be absent for not-yet-built CA
     * outputs).
     */
    virtual std::map<std::string, std::optional<StorePath>> queryDerivationOutputMap(const StorePath & deriver) = 0;

    /* ------------------------------------------------------------------ *
     * Mutations
     * ------------------------------------------------------------------ */

    /**
     * Atomically register the validity of a batch of paths.
     *
     * For every entry this records the path's metadata, its outgoing
     * references, and — for derivations — its output mapping. The whole
     * batch is one atomic unit: a reference cycle among the registered
     * paths, or a conflict with a concurrent writer, MUST roll the entire
     * batch back.
     *
     * This is the single most important atomic operation in the store; it
     * is the write path for both local builds and `nix copy`.
     */
    virtual void registerValidPaths(const ValidPathInfos & infos) = 0;

    /**
     * Record the output mapping (output name -> output path) of the derivation
     * at `deriver`. Called by the store after registering a `.drv`'s validity,
     * since extracting the outputs requires parsing the derivation content
     * (which lives on the shared filesystem, not in the database). Idempotent.
     */
    virtual void registerDerivationOutputs(
        const StorePath & deriver, const std::map<std::string, StorePath> & outputs) = 0;

    /**
     * Replace the signatures attached to `path`. Atomic with respect to
     * concurrent readers/writers of that path's row.
     */
    virtual void addSignatures(const StorePath & path, const std::set<Signature> & sigs) = 0;

    /**
     * Unconditionally remove `path` from the set of valid paths, together
     * with its outgoing references. Used by the garbage collector, which
     * has already established (under GC coordination) that the path is
     * dead. Atomic.
     */
    virtual void invalidatePath(const StorePath & path) = 0;

    /**
     * Like `invalidatePath`, but first verify within the same transaction
     * that `path` has no remaining referrers; throw `PathInUse` otherwise.
     * This is the check-then-delete used by user-facing deletion, and the
     * check and delete MUST be atomic to be correct under concurrency.
     */
    virtual void invalidatePathChecked(const StorePath & path) = 0;

    /* ------------------------------------------------------------------ *
     * Realisations (content-addressed derivation outputs)
     * ------------------------------------------------------------------ */

    /**
     * Register (or, if already present, merge signatures into) the
     * realisation of a derivation output. Upsert; atomic.
     */
    virtual void registerDrvOutput(const Realisation & info) = 0;

    /**
     * Look up the realisation of derivation output `id`, if known.
     */
    virtual std::optional<UnkeyedRealisation> queryRealisation(const DrvOutput & id) = 0;

    /* ------------------------------------------------------------------ *
     * Garbage collection (cluster-coordinated)
     * ------------------------------------------------------------------ */

    /**
     * Register `path` as rooted by `link` (a cluster-unique root identifier).
     * Upsert: re-registering the same link repoints it and refreshes its
     * registration time — roots behave as leases (see
     * `removeRootsOlderThan`).
     */
    virtual void addRoot(const std::string & link, const StorePath & path) = 0;

    /**
     * All garbage-collector roots across the whole cluster, as a map from
     * rooted store path to the set of root identifiers rooting it.
     */
    virtual std::map<StorePath, std::set<std::string>> queryRoots() = 0;

    /**
     * Delete every GC root whose last (re-)registration is older than
     * `olderThan` (epoch seconds), returning how many were removed. Roots
     * are leases: remote clients may disappear without ever cleaning up
     * their roots, so the collector calls this (with now minus the store's
     * `gc-root-lifetime`) before marking.
     */
    virtual uint64_t removeRootsOlderThan(int64_t olderThan) = 0;

    /**
     * Atomically remove a set of paths (and all their references) from the
     * metadata. The set MUST be closed under referrers — i.e. no path outside
     * the set references a path inside it — which holds for the dead set
     * computed by a mark-and-sweep. Used by the collector.
     */
    virtual void removeValidPaths(const StorePathSet & paths) = 0;

    /**
     * Try to acquire the single cluster-wide GC lease for `holder`, valid for
     * `ttlSeconds`. Returns false if another holder's lease is still valid.
     * Acquiring renews/extends the lease if `holder` already holds it.
     */
    virtual bool acquireGCLease(const std::string & holder, uint64_t ttlSeconds) = 0;

    /**
     * Release the GC lease if held by `holder` (no-op otherwise).
     */
    virtual void releaseGCLease(const std::string & holder) = 0;

    /**
     * Register `path` as a temporary root held by node `node`, valid for
     * `ttlSeconds`. Upsert: re-adding pushes the expiry forward.
     */
    virtual void addTempRoot(const std::string & node, const StorePath & path, uint64_t ttlSeconds) = 0;

    /**
     * The set of all non-expired temporary roots across the whole cluster.
     */
    virtual StorePathSet queryLiveTempRoots() = 0;

    /**
     * Remove all of `node`'s temporary roots. Called on clean shutdown so a
     * node's temp roots do not outlive it by their TTL (the TTL only bounds
     * a crashed node).
     */
    virtual void removeTempRoots(const std::string & node) = 0;

    /**
     * Try to acquire the cluster-wide exclusive build lock for derivation
     * `drvPath` on behalf of `holder`, valid for `ttlSeconds`. Returns false if
     * another holder's lease on that derivation is still valid. Acquiring
     * renews the lease if `holder` already holds it.
     */
    virtual bool acquireBuildLock(const std::string & drvPath, const std::string & holder, uint64_t ttlSeconds) = 0;

    /**
     * Release the build lock on `drvPath` if held by `holder` (no-op
     * otherwise).
     */
    virtual void releaseBuildLock(const std::string & drvPath, const std::string & holder) = 0;

    /**
     * Push the expiry of every lock whose holder starts with
     * `holderPrefix#` forward (the heartbeat), so a long build does not
     * lose its lock. Holders are unique per acquisition —
     * `<nodeId>#<token>` — so a node renews all of its own holders at once.
     */
    virtual void renewBuildLocks(const std::string & holderPrefix, uint64_t ttlSeconds) = 0;

    /**
     * Advertise `drvPath` as ready to build (all of its inputs are valid),
     * so that an idle node may steal and build it; valid for `ttlSeconds`.
     * Upsert: re-advertising pushes the expiry forward (the advertiser's
     * heartbeat).
     */
    virtual void advertiseBuild(const std::string & node, const std::string & drvPath, uint64_t ttlSeconds) = 0;

    /**
     * Withdraw an advertisement (no-op if absent).
     */
    virtual void unadvertiseBuild(const std::string & drvPath) = 0;

    /**
     * Up to `limit` stealable builds: non-expired advertisements from nodes
     * OTHER than `node` whose derivation has no live build lock (nobody is
     * actually building it right now), oldest first.
     */
    virtual std::vector<StorePath> queryStealableBuilds(const std::string & node, unsigned limit) = 0;
};

} // namespace nix

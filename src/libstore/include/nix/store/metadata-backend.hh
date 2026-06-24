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
};

} // namespace nix

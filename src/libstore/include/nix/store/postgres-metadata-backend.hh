#pragma once
///@file

#include "nix/store/config.hh"

#if NIX_WITH_POSTGRES

#  include "nix/store/metadata-backend.hh"
#  include "nix/store/store-dir-config.hh"

#  include <map>
#  include <memory>
#  include <mutex>
#  include <optional>
#  include <string>
#  include <vector>

// Forward declarations so this header does not pull in <libpq-fe.h>.
struct pg_conn;
struct pg_result;

namespace nix {

/**
 * A `MetadataBackend` backed by a PostgreSQL-wire-compatible database.
 *
 * The intended deployment target is **YugabyteDB** (whose YSQL layer speaks
 * the PostgreSQL wire protocol), giving the distributed store a replicated,
 * transactional metadata plane shared by every node. The same backend runs
 * against plain PostgreSQL, which is used for local development and testing.
 *
 * Store paths are stored as their full textual form (as produced by
 * `StoreDirConfig::printStorePath`), so the backend holds a reference to the
 * store directory configuration for (de)serialisation. The relational model
 * is the one in `src/libstore/distributed/schema.sql`.
 *
 * Concurrency: a single `pg_conn` is not thread-safe, so calls are
 * serialised by an internal mutex. This mirrors the process-local
 * serialisation that `LocalStore` gets from its `Sync<State>`; cross-node
 * serialisation is provided by the database's own transactions.
 *
 * This is experimental, in-development code (built only with the `postgres`
 * feature) and is not yet wired into a store.
 */
struct PostgresMetadataBackend : MetadataBackend
{
    /**
     * @param store      store directory configuration, for path (de)serialisation.
     * @param conninfo   a libpq connection string (e.g. from PGHOST/PGDATABASE
     *                   or "host=... dbname=... user=...").
     */
    PostgresMetadataBackend(const StoreDirConfig & store, const std::string & conninfo);
    ~PostgresMetadataBackend() override;

    PostgresMetadataBackend(const PostgresMetadataBackend &) = delete;
    PostgresMetadataBackend & operator=(const PostgresMetadataBackend &) = delete;

    /**
     * Create the schema (idempotent) if it does not already exist.
     */
    void initSchema();

    bool isValidPath(const StorePath & path) override;
    std::shared_ptr<const ValidPathInfo> queryPathInfo(const StorePath & path) override;
    StorePathSet queryAllValidPaths() override;
    std::optional<StorePath> queryPathFromHashPart(std::string_view hashPart) override;

    void queryReferrers(const StorePath & path, StorePathSet & referrers) override;
    StorePathSet queryValidDerivers(const StorePath & path) override;
    std::map<std::string, std::optional<StorePath>> queryDerivationOutputMap(const StorePath & deriver) override;

    void registerValidPaths(const ValidPathInfos & infos) override;
    void addSignatures(const StorePath & path, const std::set<Signature> & sigs) override;
    void invalidatePath(const StorePath & path) override;
    void invalidatePathChecked(const StorePath & path) override;

    void registerDrvOutput(const Realisation & info) override;
    std::optional<UnkeyedRealisation> queryRealisation(const DrvOutput & id) override;

    void addRoot(const std::string & link, const StorePath & path) override;
    std::map<StorePath, std::set<std::string>> queryRoots() override;
    void removeValidPaths(const StorePathSet & paths) override;
    bool acquireGCLease(const std::string & holder, uint64_t ttlSeconds) override;
    void releaseGCLease(const std::string & holder) override;
    void addTempRoot(const std::string & node, const StorePath & path, uint64_t ttlSeconds) override;
    void renewTempRoots(const std::string & node, uint64_t ttlSeconds) override;
    StorePathSet queryLiveTempRoots() override;
    bool acquireBuildLock(const std::string & drvPath, const std::string & holder, uint64_t ttlSeconds) override;
    void releaseBuildLock(const std::string & drvPath, const std::string & holder) override;
    void renewBuildLocks(const std::string & holder, uint64_t ttlSeconds) override;

    /**
     * Record the static output mapping of a derivation.
     *
     * `registerValidPaths` records path validity and the reference graph,
     * but the derivation→output mapping is computed by the store layer
     * (which must read and validate the derivation); it is supplied here.
     *
     * TODO: thread the precomputed outputs through `registerValidPaths` so
     * that a derivation's validity and its output mapping are committed in a
     * single transaction, matching `LocalStore`'s behaviour.
     */
    void
    registerDerivationOutputs(const StorePath & deriver, const std::map<std::string, StorePath> & outputs) override;

private:
    const StoreDirConfig & store;
    pg_conn * conn;
    std::mutex mutex;

    struct Txn;

    /** An optional text parameter for a parameterised statement (nullopt = SQL NULL). */
    using Param = std::optional<std::string>;

    /** Execute a statement with no result handling; throws on error. */
    void exec(const std::string & sql);

    /** Execute a parameterised statement; returns the result (caller owns it). */
    pg_result * execParams(const char * sql, const std::vector<Param> & params);
};

} // namespace nix

#endif // NIX_WITH_POSTGRES

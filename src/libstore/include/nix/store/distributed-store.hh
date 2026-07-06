#pragma once
///@file

#include "nix/store/config.hh"

#if NIX_WITH_POSTGRES

#  include "nix/store/local-store.hh"
#  include "nix/store/metadata-backend.hh"

#  include <condition_variable>
#  include <memory>
#  include <mutex>
#  include <thread>

namespace nix {

struct DistributedStore;

/**
 * Configuration for a distributed store: store *content* lives on a shared
 * filesystem (as for any `LocalFSStore`), while *metadata* lives in a shared
 * distributed database addressed by `metadata-db-url`. Every node in the
 * cluster opens the same store; the database provides the cross-node
 * atomicity and replication.
 */
struct DistributedStoreConfig : virtual LocalStoreConfig
{
    DistributedStoreConfig(const Params & params)
        : StoreConfig(params, FilePathType::Native)
        , LocalFSStoreConfig(params)
        , LocalStoreConfig(params)
    {
    }

    DistributedStoreConfig(std::string_view scheme, std::string_view authority, const Params & params);

    Setting<std::string> metadataDbUrl{
        this,
        "",
        "metadata-db-url",
        R"(
          The libpq connection string for the shared metadata database
          (PostgreSQL, or a PostgreSQL-wire-compatible distributed database
          such as YugabyteDB). For example
          `host=db.example.com dbname=nix user=nix`.
        )"};

    Setting<uint64_t> gcRootLifetime{
        this,
        7 * 24 * 3600,
        "gc-root-lifetime",
        R"(
          How long (in seconds) a registered GC root stays alive without
          being re-registered — one week by default. In a distributed setup
          roots are registered remotely by clients that may disappear without
          ever cleaning them up, so roots behave as *leases*: adding a root
          that already exists refreshes it, and the garbage collector deletes
          roots that have not been refreshed within this lifetime, reclaiming
          the paths they held in the same run.
        )"};

    static const std::string name()
    {
        return "Distributed Store";
    }

    static std::string doc();

    static StringSet uriSchemes()
    {
        return {"distributed"};
    }

    ref<Store> openStore() const override;

    StoreReference getReference() const override;

private:
    void anchor() override;
};

/**
 * A store whose content is served from a shared filesystem and whose metadata
 * is served from a shared `MetadataBackend`. It is a full `LocalStore`, so it
 * can build derivations locally (writing outputs into the shared `real=`
 * filesystem); the metadata virtuals are overridden so the cluster-wide
 * database — not the node-local SQLite that `LocalStore` opens — is the source
 * of truth. The node-local SQLite is therefore vestigial; the `state=` dir must
 * be node-local and distinct from any system store.
 *
 * Cross-node build coordination uses a per-derivation lock in the database (see
 * `tryLockBuild`), so two nodes never build the same derivation at once.
 *
 * Experimental and in development (built only with the `postgres` feature).
 */
struct DistributedStore : virtual LocalStore
{
    using Config = DistributedStoreConfig;

    ref<const Config> config;

    DistributedStore(ref<const Config>);
    ~DistributedStore() override;

    /* --- metadata: routed to the backend --- */
    bool isValidPathUncached(const StorePath & path) override;
    void queryPathInfoUncached(
        const StorePath & path, Callback<std::shared_ptr<const ValidPathInfo>> callback) noexcept override;
    void queryReferrers(const StorePath & path, StorePathSet & referrers) override;
    StorePathSet queryValidDerivers(const StorePath & path) override;
    std::map<std::string, std::optional<StorePath>>
    queryStaticPartialDerivationOutputMap(const StorePath & path) override;
    std::optional<StorePath> queryPathFromHashPart(const std::string & hashPart) override;
    StorePathSet queryAllValidPaths() override;
    void addSignatures(const StorePath & storePath, const std::set<Signature> & sigs) override;
    void registerDrvOutput(const Realisation & info) override;
    void queryRealisationUncached(
        const DrvOutput & id, Callback<std::shared_ptr<const UnkeyedRealisation>> callback) noexcept override;

    /* Route build-output (and other) registration to the shared database
       instead of the node-local SQLite. The single-path `registerValidPath`
       inherited from LocalStore delegates to this. */
    void registerValidPaths(const ValidPathInfos & infos) override;

    /* --- content mutation --- */
    void addToStore(const ValidPathInfo & info, Source & source, RepairFlag repair, CheckSigsFlag checkSigs) override;
    StorePath addToStoreFromDump(
        Source & dump,
        std::string_view name,
        FileSerialisationMethod dumpMethod,
        ContentAddressMethod hashMethod,
        HashAlgorithm hashAlgo,
        const StorePathSet & references,
        RepairFlag repair) override;

    /* --- garbage collection: DB-coordinated (lease + roots + temp roots) --- */
    Roots findRoots(bool censor) override;
    void collectGarbage(const GCOptions & options, GCResults & results) override;
    /* `addPermRoot` is `final` in IndirectRootStore; it creates the user-facing
       symlink and calls this, which records the root in the shared database. */
    void addIndirectRoot(const std::filesystem::path & path) override;
    /* Remote clients register roots by name (no symlink anywhere on the
       shared filesystem); re-adding refreshes the root's lease (see
       `gc-root-lifetime`). */
    bool addNamedRoot(const std::string & name, const StorePath & storePath) override;

    /**
     * Verify the store against the shared database (NOT the vestigial
     * node-local SQLite that `LocalStore::verifyStore` would consult):
     * every DB-valid path must exist on the shared filesystem, its
     * references must be valid, and with `checkContents` its NAR hash must
     * match. Repair mode is not yet supported (it would have to coordinate
     * cluster-wide) and throws.
     */
    bool verifyStore(bool checkContents, RepairFlag repair) override;

    /* --- build logs: not yet implemented --- */
    std::optional<std::string> getBuildLogExact(const StorePath & path) override;
    void addBuildLog(const StorePath & path, std::string_view log) override;

    std::optional<TrustedFlag> isTrustedClient() override;

    /**
     * Register `path` as a temporary root in the shared database so that no
     * node's collector deletes it while it is in use here. Kept alive by the
     * heartbeat thread until this store is destroyed.
     */
    void addTempRoot(const StorePath & path) override;

    /**
     * Acquire the cluster-wide per-derivation build lock in the database, so no
     * two nodes build the same derivation at once. Returns a handle that
     * releases the lock when destroyed, or `nullptr` if another node currently
     * holds it. Held locks are kept alive by the heartbeat thread.
     */
    std::unique_ptr<BuildLock> tryLockBuild(const StorePath & drvPath) override;

private:
    void anchor() override;

    std::unique_ptr<MetadataBackend> backend;

    /** Identifies this process/node for the GC lease and temp roots. */
    std::string nodeId;

    /** Background heartbeat that keeps this node's temp roots alive. */
    std::thread heartbeatThread;
    std::mutex heartbeatMutex;
    std::condition_variable heartbeatCv;
    bool heartbeatStop = false;
};

} // namespace nix

#endif // NIX_WITH_POSTGRES

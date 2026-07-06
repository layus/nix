#pragma once
///@file

#include "nix/store/config.hh"

#if NIX_WITH_POSTGRES

#  include "nix/store/local-store.hh"
#  include "nix/store/metadata-backend.hh"

#  include <atomic>
#  include <condition_variable>
#  include <map>
#  include <memory>
#  include <mutex>
#  include <optional>
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

    Setting<bool> workStealing{
        this,
        false,
        "work-stealing",
        R"(
          Whether this node steals advertised builds from busier nodes of the
          cluster when it is otherwise idle. Saturated nodes advertise their
          ready-to-build derivations; the per-derivation build lock still
          arbitrates execution, so duplicate builds are impossible. Enable on
          cluster nodes that run builds (e.g. under `nix-grpc-store-server`);
          leave off for pure clients.
        )"};

    Setting<uint64_t> workStealingInterval{
        this,
        5,
        "work-stealing-interval",
        R"(
          How often (in seconds) an idle node polls for stealable builds.
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

    /* --- build logs: recorded in the shared database, chunk by chunk, so
       every worker and client can read (and follow) them --- */
    /** Tees a build's raw log into the shared `BuildLogs` table. */
    std::shared_ptr<FinishSink> buildLogSink(const StorePath & drvPath) override;
    /** Streams another worker's in-progress log from `BuildLogs` (used
        while waiting on its build lock, so every requester sees the log). */
    std::unique_ptr<BuildLogFollower> followBuildLog(const StorePath & drvPath) override;
    /** Serves `nix log` from `BuildLogs`, cluster-wide. */
    std::optional<std::string> getBuildLogExact(const StorePath & path) override;
    void addBuildLog(const StorePath & path, std::string_view log) override;

    std::optional<TrustedFlag> isTrustedClient() override;

    /**
     * RAII pin keeping `path` protected from every node's collector while an
     * operation on this node uses it. Pins form a multiset (a map from path
     * to pin count): the FIRST pin for a path registers its temp root
     * synchronously — the path is protected cluster-wide by the time
     * `pinPath` returns — and while any pin for it is alive, the heartbeat
     * keeps the root renewed. When the last pin dies the path leaves the
     * set: its database row is no longer renewed and simply lapses after the
     * temp-root TTL, which therefore bounds both a crashed node's leftovers
     * and the post-operation reclaim latency (and keeps covering the window
     * between an operation finishing and the client registering a permanent
     * root, as LocalStore's connection-lifetime temp roots do).
     */
    class [[nodiscard]] TempRootPin
    {
        friend struct DistributedStore;

        DistributedStore * store = nullptr;
        std::optional<StorePath> path;

        TempRootPin(DistributedStore & store, const StorePath & path);

    public:
        TempRootPin() = default;
        TempRootPin(const TempRootPin &) = delete;
        TempRootPin & operator=(const TempRootPin &) = delete;

        TempRootPin(TempRootPin && other) noexcept
            : store(other.store)
            , path(std::move(other.path))
        {
            other.store = nullptr;
        }

        ~TempRootPin();
    };

    /**
     * Acquire a pin for `path` (see `TempRootPin`).
     */
    TempRootPin pinPath(const StorePath & path);

    /**
     * One-shot cluster-wide protection of `path` for the temp-root TTL, for
     * callers without an operation scope (the generic `Store` machinery).
     * The root is NOT renewed by the heartbeat; operations of this store
     * itself hold `TempRootPin`s instead.
     */
    void addTempRoot(const StorePath & path) override;

    /**
     * Acquire the cluster-wide per-derivation build lock in the database, so
     * no two workers — on any node, this one included (the holder is unique
     * per acquisition) — build the same derivation at once. Returns a handle
     * that releases the lock when destroyed, or `nullptr` if another worker
     * currently holds it. Held locks are kept alive by the heartbeat thread.
     */
    std::unique_ptr<BuildLock> tryLockBuild(const StorePath & drvPath) override;

    /**
     * Build exclusion comes entirely from `tryLockBuild`'s database entries;
     * the machine-local flock output locks are skipped (flock is unreliable
     * on the shared filesystem).
     */
    bool useFileSystemBuildLocks() override;

    /**
     * Advertise a ready-to-build derivation in the shared `BuildQueue`, so
     * an idle node may steal it (see the `work-stealing` setting). Handles
     * form a multiset like the temp-root pins: the first one publishes
     * synchronously, the heartbeat renews live advertisements, and the last
     * handle's destruction withdraws the row (a crashed node's ads lapse by
     * TTL).
     */
    std::unique_ptr<BuildAdvertisement> advertiseBuild(const StorePath & drvPath) override;

private:
    void anchor() override;

    std::unique_ptr<MetadataBackend> backend;

    /** Identifies this process/node for the GC lease and temp roots. */
    std::string nodeId;

    /** Background heartbeat that keeps pinned temp roots and held build
        locks alive. */
    std::thread heartbeatThread;
    std::mutex heartbeatMutex;
    std::condition_variable heartbeatCv;
    bool heartbeatStop = false;

    /** The pin multiset: paths currently in use by operations on this node,
        with their pin counts (see `TempRootPin`). Guarded by `pinsMutex`;
        the heartbeat renews exactly these paths' temp roots. */
    std::mutex pinsMutex;
    std::map<StorePath, unsigned> pinnedPaths;

    /** Advertised ready-to-build derivations, with handle counts (see
        `advertiseBuild`). Guarded by `pinsMutex`; renewed by the
        heartbeat. */
    std::map<StorePath, unsigned> advertisedBuilds;

    class BuildAd;

    /** Number of builds this node is currently running (live build-lock
        handles); the work stealer only runs at zero. */
    std::atomic<unsigned> activeLocalBuilds{0};

    /** Makes lock holders unique per acquisition (nodeId#token). */
    std::atomic<uint64_t> lockCounter{0};

    /** Cluster-wide exclusive lease on an arbitrary key, waiting until
        granted — the database replacement for flock PathLocks. */
    std::unique_ptr<BuildLock> lockClusterKey(std::string key);

    /** Background work stealer (only started with `work-stealing`). */
    std::thread stealerThread;
    void stealSome();

    /** Steals that failed, with when: skipped for a cooldown so a failing
        advertisement is not re-stolen every poll (only touched by the
        stealer thread). */
    std::map<StorePath, std::chrono::steady_clock::time_point> stealFailures;
};

} // namespace nix

#endif // NIX_WITH_POSTGRES

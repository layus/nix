#pragma once
///@file

#include "nix/store/config.hh"

#if NIX_WITH_POSTGRES

#  include "nix/store/local-fs-store.hh"
#  include "nix/store/metadata-backend.hh"

#  include <memory>

namespace nix {

struct DistributedStore;

/**
 * Configuration for a distributed store: store *content* lives on a shared
 * filesystem (as for any `LocalFSStore`), while *metadata* lives in a shared
 * distributed database addressed by `metadata-db-url`. Every node in the
 * cluster opens the same store; the database provides the cross-node
 * atomicity and replication.
 */
struct DistributedStoreConfig : std::enable_shared_from_this<DistributedStoreConfig>, virtual LocalFSStoreConfig
{
    DistributedStoreConfig(const Params & params)
        : StoreConfig(params, FilePathType::Native)
        , LocalFSStoreConfig(params)
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
 * is served from a shared `MetadataBackend`. Read/query operations and path
 * registration are routed to the backend; content reads come from the shared
 * filesystem via `LocalFSStore`.
 *
 * Experimental and in development (built only with the `postgres` feature).
 * Content-mutating operations (`addToStore`), garbage collection, and build
 * logs are not yet implemented; builds are expected to happen locally on each
 * node and their results copied/registered into the shared store.
 */
struct DistributedStore : virtual LocalFSStore
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

    /* --- content mutation: not yet implemented --- */
    void addToStore(const ValidPathInfo & info, Source & source, RepairFlag repair, CheckSigsFlag checkSigs) override;
    StorePath addToStoreFromDump(
        Source & dump,
        std::string_view name,
        FileSerialisationMethod dumpMethod,
        ContentAddressMethod hashMethod,
        HashAlgorithm hashAlgo,
        const StorePathSet & references,
        RepairFlag repair) override;

    /* --- garbage collection: not yet implemented (see the DB-coordinated GC
       design) --- */
    Roots findRoots(bool censor) override;
    void collectGarbage(const GCOptions & options, GCResults & results) override;
    std::filesystem::path addPermRoot(const StorePath & storePath, const std::filesystem::path & gcRoot) override;

    /* --- build logs: not yet implemented --- */
    std::optional<std::string> getBuildLogExact(const StorePath & path) override;
    void addBuildLog(const StorePath & path, std::string_view log) override;

    std::optional<TrustedFlag> isTrustedClient() override;

private:
    void anchor() override;

    std::unique_ptr<MetadataBackend> backend;

    /** Identifies this process/node when holding the cluster GC lease. */
    std::string nodeId;
};

} // namespace nix

#endif // NIX_WITH_POSTGRES

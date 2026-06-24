#include "nix/store/config.hh"

#if NIX_WITH_POSTGRES

#  include "nix/store/distributed-store.hh"
#  include "nix/store/postgres-metadata-backend.hh"
#  include "nix/store/store-registration.hh"
#  include "nix/store/pathlocks.hh"
#  include "nix/store/posix-fs-canonicalise.hh"
#  include "nix/util/callback.hh"
#  include "nix/util/error.hh"
#  include "nix/util/archive.hh"
#  include "nix/util/hash.hh"
#  include "nix/util/serialise.hh"
#  include "nix/util/file-system.hh"
#  include "nix/util/source-accessor.hh"
#  include "nix/util/file-content-address.hh"

namespace nix {

/* ------------------------------------------------------------------ *
 * Config
 * ------------------------------------------------------------------ */

DistributedStoreConfig::DistributedStoreConfig(
    std::string_view /* scheme */, std::string_view authority, const Params & params)
    : DistributedStoreConfig(params)
{
    if (!authority.empty())
        throw UsageError("a 'distributed://' store URI must not have an authority part ('%s')", authority);
}

void DistributedStoreConfig::anchor() {}

std::string DistributedStoreConfig::doc()
{
    return R"(
        A store whose content lives on a shared filesystem and whose metadata
        lives in a shared distributed database (PostgreSQL or a
        PostgreSQL-wire-compatible database such as YugabyteDB), so that
        multiple nodes can share one store. Set `metadata-db-url` to the
        database connection string.

        This is experimental and under development.
    )";
}

StoreReference DistributedStoreConfig::getReference() const
{
    return {
        .variant =
            StoreReference::Specified{
                .scheme = *uriSchemes().begin(),
            },
        .params = getQueryParams(),
    };
}

ref<Store> DistributedStoreConfig::openStore() const
{
    return make_ref<DistributedStore>(ref<const DistributedStoreConfig>(shared_from_this()));
}

/* ------------------------------------------------------------------ *
 * Store
 * ------------------------------------------------------------------ */

DistributedStore::DistributedStore(ref<const Config> config)
    : Store{*config}
    , LocalFSStore{*config}
    , config{config}
{
    if (config->metadataDbUrl.get().empty())
        throw UsageError("the 'metadata-db-url' setting is required for a distributed store");
    createDirs(config->realStoreDir.get());
    backend = std::make_unique<PostgresMetadataBackend>(*config, config->metadataDbUrl.get());
}

DistributedStore::~DistributedStore() = default;

void DistributedStore::anchor() {}

/* --- metadata: routed to the backend --- */

bool DistributedStore::isValidPathUncached(const StorePath & path)
{
    return backend->isValidPath(path);
}

void DistributedStore::queryPathInfoUncached(
    const StorePath & path, Callback<std::shared_ptr<const ValidPathInfo>> callback) noexcept
{
    try {
        callback(backend->queryPathInfo(path));
    } catch (...) {
        callback.rethrow();
    }
}

void DistributedStore::queryReferrers(const StorePath & path, StorePathSet & referrers)
{
    backend->queryReferrers(path, referrers);
}

StorePathSet DistributedStore::queryValidDerivers(const StorePath & path)
{
    return backend->queryValidDerivers(path);
}

std::map<std::string, std::optional<StorePath>>
DistributedStore::queryStaticPartialDerivationOutputMap(const StorePath & path)
{
    return backend->queryDerivationOutputMap(path);
}

std::optional<StorePath> DistributedStore::queryPathFromHashPart(const std::string & hashPart)
{
    return backend->queryPathFromHashPart(hashPart);
}

StorePathSet DistributedStore::queryAllValidPaths()
{
    return backend->queryAllValidPaths();
}

void DistributedStore::addSignatures(const StorePath & storePath, const std::set<Signature> & sigs)
{
    backend->addSignatures(storePath, sigs);
}

void DistributedStore::registerDrvOutput(const Realisation & info)
{
    backend->registerDrvOutput(info);
}

void DistributedStore::queryRealisationUncached(
    const DrvOutput & id, Callback<std::shared_ptr<const UnkeyedRealisation>> callback) noexcept
{
    try {
        auto r = backend->queryRealisation(id);
        callback(r ? std::make_shared<const UnkeyedRealisation>(*r) : nullptr);
    } catch (...) {
        callback.rethrow();
    }
}

/* --- not yet implemented ---
   These operations require content writing, GC coordination, or build-log
   storage that the distributed store does not yet provide. Builds are
   expected to run locally and their results be copied/registered in. */

void DistributedStore::addToStore(
    const ValidPathInfo & info, Source & source, RepairFlag repair, CheckSigsFlag checkSigs)
{
    if (checkSigs && pathInfoIsUntrusted(info))
        throw Error(
            "cannot add path '%s' because it lacks a signature by a trusted key", printStorePath(info.path));

    if (!repair && isValidPath(info.path))
        return;

    /* Lock the output path against concurrent writers on this node. (Content
       is content-addressed, so concurrent writers on other nodes converge.) */
    auto realPath = toRealPath(info.path);
    PathLocks outputLock;
    outputLock.lockPaths({realPath.string()});

    /* It may have become valid in the meantime. */
    if (!repair && isValidPathUncached(info.path))
        return;

    deletePath(realPath);

    /* Restore the NAR while computing its hash, then verify it matches. */
    HashSink hashSink(HashAlgorithm::SHA256);
    TeeSource wrapperSource{source, hashSink};
    restorePath(realPath, wrapperSource);
    auto hashResult = hashSink.finish();

    if (hashResult.hash != info.narHash)
        throw Error(
            "hash mismatch importing path '%s';\n  specified: %s\n  got:       %s",
            printStorePath(info.path),
            info.narHash.to_string(HashFormat::SRI, true),
            hashResult.hash.to_string(HashFormat::SRI, true));

    if (hashResult.numBytesDigested != info.narSize)
        throw Error(
            "size mismatch importing path '%s';\n  specified: %s\n  got:       %s",
            printStorePath(info.path),
            info.narSize,
            hashResult.numBytesDigested);

    /* TODO: for content-addressed paths (info.ca), re-verify the content
       address against the restored content, as LocalStore does. */

    static const StringSet emptyAcls;
    canonicalisePathMetaData(realPath, {NIX_WHEN_SUPPORT_ACLS(emptyAcls)});

    backend->registerValidPaths(ValidPathInfos{{info.path, info}});

    outputLock.setDeletion(true);
}

StorePath DistributedStore::addToStoreFromDump(
    Source & source0,
    std::string_view name,
    FileSerialisationMethod dumpMethod,
    ContentAddressMethod hashMethod,
    HashAlgorithm hashAlgo,
    const StorePathSet & references,
    RepairFlag repair)
{
    /* Hash the dump as it streams in (for computing the store path). */
    HashSink hashSink{hashAlgo};
    TeeSource source{source0, hashSink};

    /* Restore the dump into a temporary directory in the store. (Unlike
       LocalStore we always go via a temp path, skipping the in-memory
       fast path, for simplicity.) */
    auto tempDir = createTempDir(config->realStoreDir.get(), "nix-distributed-add");
    AutoDelete delTempDir(tempDir, true);
    auto tempPath = tempDir / "x";
    restorePath(tempPath, source, dumpMethod);

    auto [dumpHash, size] = hashSink.finish();

    bool methodsMatch = static_cast<FileIngestionMethod>(dumpMethod) == hashMethod.getFileIngestionMethod();

    auto desc = ContentAddressWithReferences::fromParts(
        hashMethod,
        methodsMatch
            ? dumpHash
            : hashPath(makeFSSourceAccessor(tempPath), hashMethod.getFileIngestionMethod(), hashAlgo).first,
        {
            .others = references,
            .self = false,
        });

    auto dstPath = makeFixedOutputPathFromCA(name, desc);

    if (!repair && isValidPath(dstPath))
        return dstPath;

    auto realPath = toRealPath(dstPath);
    PathLocks outputLock({realPath.string()});

    if (!repair && isValidPathUncached(dstPath))
        return dstPath;

    deletePath(realPath);
    moveFile(tempPath, realPath);

    /* Compute the NAR hash (the same as the dump hash only in recursive
       SHA-256 mode). */
    HashResult narHash = {dumpHash, size};
    if (dumpMethod != FileSerialisationMethod::NixArchive || hashAlgo != HashAlgorithm::SHA256) {
        HashSink narSink{HashAlgorithm::SHA256};
        dumpPath(realPath, narSink);
        narHash = narSink.finish();
    }

    static const StringSet emptyAcls;
    canonicalisePathMetaData(realPath, {NIX_WHEN_SUPPORT_ACLS(emptyAcls)});

    auto info = ValidPathInfo::makeFromCA(*this, name, std::move(desc), narHash.hash);
    info.narSize = narHash.numBytesDigested;
    backend->registerValidPaths(ValidPathInfos{{info.path, info}});

    outputLock.setDeletion(true);
    return dstPath;
}

Roots DistributedStore::findRoots(bool)
{
    throw Error("garbage collection is not yet supported on the distributed store");
}

void DistributedStore::collectGarbage(const GCOptions &, GCResults &)
{
    throw Error("garbage collection is not yet supported on the distributed store");
}

std::filesystem::path DistributedStore::addPermRoot(const StorePath &, const std::filesystem::path &)
{
    throw Error("'addPermRoot' is not yet supported on the distributed store");
}

std::optional<std::string> DistributedStore::getBuildLogExact(const StorePath &)
{
    return std::nullopt;
}

void DistributedStore::addBuildLog(const StorePath &, std::string_view)
{
    throw Error("'addBuildLog' is not yet supported on the distributed store");
}

std::optional<TrustedFlag> DistributedStore::isTrustedClient()
{
    return Trusted;
}

static RegisterStoreImplementation<DistributedStore::Config> regDistributedStore;

} // namespace nix

#endif // NIX_WITH_POSTGRES

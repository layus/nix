#include "nix/store/config.hh"

#if NIX_WITH_POSTGRES

#  include "nix/store/distributed-store.hh"
#  include "nix/store/postgres-metadata-backend.hh"
#  include "nix/store/store-registration.hh"
#  include "nix/store/derivations.hh"
#  include "nix/store/pathlocks.hh"
#  include "nix/store/posix-fs-canonicalise.hh"
#  include "nix/store/local-settings.hh"
#  include "nix/util/callback.hh"
#  include "nix/util/error.hh"
#  include "nix/util/archive.hh"
#  include "nix/util/hash.hh"
#  include "nix/util/serialise.hh"
#  include "nix/util/file-system.hh"
#  include "nix/util/source-accessor.hh"
#  include "nix/util/file-content-address.hh"
#  include "nix/util/finally.hh"
#  include "nix/util/signals.hh"

#  include <unistd.h>

namespace nix {

/* How long a temp root stays valid, and how often the heartbeat renews this
   node's temp roots. The interval must be comfortably below the TTL so a root
   never lapses while its node is alive. */
static constexpr uint64_t tempRootTtlSeconds = 600;
static constexpr unsigned tempRootHeartbeatSeconds = 200;

/* How long a per-derivation build lock stays valid without a heartbeat; the
   same heartbeat that renews temp roots renews held build locks, so a build
   longer than this never loses its lock, while a crashed builder's lock lapses. */
static constexpr uint64_t buildLockTtlSeconds = 600;

namespace {

/* RAII handle for a held cluster-wide build lock; releases it on destruction.
   Holds a raw backend pointer, which is safe because the owning DistributedStore
   (and thus the backend) outlives any build it is running. */
struct DistributedBuildLock : BuildLock
{
    MetadataBackend * backend;
    std::string drvPath;
    std::string holder;

    DistributedBuildLock(MetadataBackend * backend, std::string drvPath, std::string holder)
        : backend(backend)
        , drvPath(std::move(drvPath))
        , holder(std::move(holder))
    {
    }

    ~DistributedBuildLock() override
    {
        try {
            backend->releaseBuildLock(drvPath, holder);
        } catch (...) {
            ignoreExceptionInDestructor();
        }
    }
};

} // namespace

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
    return make_ref<DistributedStore>(ref{std::dynamic_pointer_cast<const DistributedStoreConfig>(shared_from_this())});
}

/* ------------------------------------------------------------------ *
 * Store
 * ------------------------------------------------------------------ */

DistributedStore::DistributedStore(ref<const Config> config)
    : Store{*config}
    , LocalFSStore{*config}
    , LocalStore{static_cast<ref<const LocalStore::Config>>(config)}
    , config{config}
{
    if (config->metadataDbUrl.get().empty())
        throw UsageError("the 'metadata-db-url' setting is required for a distributed store");
    createDirs(config->realStoreDir.get());
    backend = std::make_unique<PostgresMetadataBackend>(*config, config->metadataDbUrl.get());

    char host[256] = {0};
    gethostname(host, sizeof(host) - 1);
    nodeId = std::string(host) + ":" + std::to_string(getpid());

    /* Keep this node's temp roots alive for as long as the store is open. */
    heartbeatThread = std::thread([this]() {
        std::unique_lock<std::mutex> lk(heartbeatMutex);
        while (!heartbeatStop) {
            heartbeatCv.wait_for(lk, std::chrono::seconds(tempRootHeartbeatSeconds), [this]() {
                return heartbeatStop;
            });
            if (heartbeatStop)
                break;
            lk.unlock();
            try {
                backend->renewTempRoots(nodeId, tempRootTtlSeconds);
                backend->renewBuildLocks(nodeId, buildLockTtlSeconds);
            } catch (...) {
                /* Don't let a transient database error kill the heartbeat. */
            }
            lk.lock();
        }
    });
}

DistributedStore::~DistributedStore()
{
    {
        std::lock_guard<std::mutex> lk(heartbeatMutex);
        heartbeatStop = true;
    }
    heartbeatCv.notify_all();
    if (heartbeatThread.joinable())
        heartbeatThread.join();
}

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

void DistributedStore::registerValidPaths(const ValidPathInfos & infos)
{
    /* The build pipeline registers its outputs here (LocalStore would write
       SQLite); route to the shared database so every node sees them. */
    backend->registerValidPaths(infos);

    /* Mirror LocalStore::addValidPath: when a derivation is registered, record
       its output map (output name -> output path). The build relies on this
       (via queryDerivationOutputMap), so it must be populated for every .drv
       added to the store. Parsing requires the derivation content, which is on
       the shared filesystem, so we do it at the store layer. */
    for (auto & [_, info] : infos) {
        if (!info.path.isDerivation())
            continue;
        auto drv = readInvalidDerivation(info.path);
        drv.checkInvariants(*this, info.path);
        std::map<std::string, StorePath> outputs;
        for (auto & [name, output] : drv.outputsAndOptPaths(*this))
            if (output.second) // floating CA outputs have no path until built
                outputs.insert_or_assign(name, *output.second);
        if (!outputs.empty())
            backend->registerDerivationOutputs(info.path, outputs);
    }
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

    /* Protect the path from a concurrent collector on any node while we add it. */
    addTempRoot(info.path);

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

    /* Honour `ignored-acls` (as LocalStore does): some ACLs such as
       `system.nfs4_acl` cannot be removed even by root, and on shared NFS
       storage every file carries one, so passing an empty set here made every
       add fail with EINVAL. */
    canonicalisePathMetaData(realPath, {NIX_WHEN_SUPPORT_ACLS(config->getLocalSettings().ignoredAcls)});

    /* Route through our override so a copied-in .drv also gets its output map
       registered (the build needs it). */
    registerValidPaths(ValidPathInfos{{info.path, info}});

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

    /* Protect the path from a concurrent collector on any node. */
    addTempRoot(dstPath);

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

    /* Honour `ignored-acls`; see the note in addToStore above. */
    canonicalisePathMetaData(realPath, {NIX_WHEN_SUPPORT_ACLS(config->getLocalSettings().ignoredAcls)});

    auto info = ValidPathInfo::makeFromCA(*this, name, std::move(desc), narHash.hash);
    info.narSize = narHash.numBytesDigested;
    /* Route through our override so a derivation written via this path (e.g.
       `writeDerivation`, which uses addToStoreFromDump) also gets its output
       map registered. */
    registerValidPaths(ValidPathInfos{{info.path, info}});

    outputLock.setDeletion(true);
    return dstPath;
}

void DistributedStore::addIndirectRoot(const std::filesystem::path & gcRoot)
{
    /* IndirectRootStore::addPermRoot has already created the user-facing symlink
       (gcRoot -> store path); record the root in the shared database so every
       node's collector sees it. */
    auto storePath = parseStorePath(readLink(gcRoot).string());
    backend->addRoot(gcRoot.string(), storePath);
}

Roots DistributedStore::findRoots(bool censor)
{
    Roots roots;
    for (auto & [path, links] : backend->queryRoots()) {
        auto & set = roots[path];
        for (auto & link : links)
            set.insert(censor ? "{censored}" : link);
    }
    return roots;
}

void DistributedStore::collectGarbage(const GCOptions & options, GCResults & results)
{
    /* Cluster-wide mutual exclusion: only one node collects at a time. */
    if (!backend->acquireGCLease(nodeId, /*ttlSeconds=*/3600))
        throw Error("another node is currently running garbage collection on this store");
    Finally releaseLease([&]() { backend->releaseGCLease(nodeId); });

    /* Mark: the live set is the closure, over references, of all cluster
       roots *and* every non-expired temp root (paths in use on any node). */
    StorePathSet rootPaths;
    for (auto & [path, links] : backend->queryRoots())
        rootPaths.insert(path);
    for (auto & p : backend->queryLiveTempRoots())
        rootPaths.insert(p);

    StorePathSet live;
    if (!options.ignoreLiveness)
        computeFSClosure(rootPaths, live);

    if (options.action == GCOptions::gcReturnLive) {
        for (auto & p : live)
            results.paths.insert(printStorePath(p));
        return;
    }

    /* Sweep: dead = all valid paths not in the live set. */
    StorePathSet dead;
    for (auto & p : backend->queryAllValidPaths())
        if (!live.count(p))
            dead.insert(p);

    if (options.action == GCOptions::gcReturnDead) {
        for (auto & p : dead)
            results.paths.insert(printStorePath(p));
        return;
    }

    /* Decide what to delete. */
    StorePathSet candidates;
    if (options.action == GCOptions::gcDeleteSpecific) {
        auto & specific = std::get<GCOptions::SpecificPaths>(options.pathsToDelete);
        for (auto & p : specific.paths) {
            if (!options.ignoreLiveness && !dead.count(p))
                throw Error("cannot delete path '%s' since it is still live", printStorePath(p));
            candidates.insert(p);
        }
    } else
        candidates = dead;

    /* Delete content from the shared filesystem, honouring `maxFreed`, and
       remove exactly what we deleted from the metadata. `removeValidPaths`
       drops every reference edge touching the set first, so deleting an
       arbitrary subset never trips the foreign key. */
    StorePathSet deleted;
    for (auto & p : candidates) {
        if (results.bytesFreed >= options.maxFreed)
            break;
        uint64_t freed = 0;
        deletePath(toRealPath(p), freed);
        results.bytesFreed += freed;
        results.paths.insert(printStorePath(p));
        deleted.insert(p);
    }

    backend->removeValidPaths(deleted);
}

bool DistributedStore::verifyStore(bool checkContents, RepairFlag repair)
{
    /* LocalStore::verifyStore would verify (and in repair mode, mutate!) the
       vestigial node-local SQLite; here the shared database is the source of
       truth. Repairing would have to coordinate cluster-wide (other nodes may
       be using the path being rewritten), so it is refused for now. */
    if (repair)
        throw Unsupported("repairing the distributed store is not yet supported");

    printInfo("verifying against the shared database...");
    auto validPaths = queryAllValidPaths();

    bool errors = false;

    printInfo("checking path existence on the shared filesystem...");
    for (auto & path : validPaths) {
        checkInterrupt();
        if (!pathExists(toRealPath(path))) {
            printError(
                "path '%s' is valid in the database but missing from the shared filesystem", printStorePath(path));
            errors = true;
            continue;
        }
        try {
            auto info = queryPathInfo(path);
            for (auto & ref : info->references)
                if (!validPaths.count(ref)) {
                    printError(
                        "path '%s' refers to invalid path '%s'", printStorePath(path), printStorePath(ref));
                    errors = true;
                }
        } catch (Error & e) {
            logError(e.info());
            errors = true;
        }
    }

    if (checkContents) {
        printInfo("checking store hashes...");
        for (auto & path : validPaths) {
            checkInterrupt();
            try {
                auto info = queryPathInfo(path);
                printMsg(lvlTalkative, "checking contents of '%s'", printStorePath(path));
                auto hashSink = HashSink(info->narHash.algo);
                dumpPath(toRealPath(path), hashSink);
                auto current = hashSink.finish();
                if (info->narHash != current.hash) {
                    printError(
                        "path '%s' was modified! expected hash '%s', got '%s'",
                        printStorePath(path),
                        info->narHash.to_string(HashFormat::Nix32, true),
                        current.hash.to_string(HashFormat::Nix32, true));
                    errors = true;
                }
            } catch (Error & e) {
                /* The path may have been GC'ed by another node meanwhile. */
                if (isValidPath(path))
                    logError(e.info());
                else
                    logWarning(e.info());
                errors = true;
            }
        }
    }

    return errors;
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

void DistributedStore::addTempRoot(const StorePath & path)
{
    backend->addTempRoot(nodeId, path, tempRootTtlSeconds);
}

std::unique_ptr<BuildLock> DistributedStore::tryLockBuild(const StorePath & drvPath)
{
    auto drv = printStorePath(drvPath);
    if (!backend->acquireBuildLock(drv, nodeId, buildLockTtlSeconds))
        return nullptr; // another node is building this derivation
    return std::make_unique<DistributedBuildLock>(backend.get(), std::move(drv), nodeId);
}

static RegisterStoreImplementation<DistributedStore::Config> regDistributedStore;

} // namespace nix

#endif // NIX_WITH_POSTGRES

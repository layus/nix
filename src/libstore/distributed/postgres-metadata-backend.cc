#include "nix/store/config.hh"

#if NIX_WITH_POSTGRES

#  include "nix/store/postgres-metadata-backend.hh"
#  include "nix/util/error.hh"
#  include "nix/util/util.hh"
#  include "nix/util/signature/local-keys.hh"
#  include "nix/util/topo-sort.hh"
#  include "nix/util/hash.hh"
#  include "nix/store/content-address.hh"

#  include <libpq-fe.h>

#  include <ctime>
#  include <vector>

namespace nix {

/* ------------------------------------------------------------------ *
 * Small libpq helpers
 * ------------------------------------------------------------------ */

namespace {

/** RAII wrapper around a PGresult. */
struct Result
{
    PGresult * res;

    Result(PGresult * res)
        : res(res)
    {
    }
    Result(Result &&) = delete;
    Result(const Result &) = delete;
    ~Result()
    {
        if (res)
            PQclear(res);
    }

    int ntuples() const
    {
        return PQntuples(res);
    }

    bool isNull(int row, int col) const
    {
        return PQgetisnull(res, row, col);
    }

    std::string get(int row, int col) const
    {
        return std::string(PQgetvalue(res, row, col));
    }

    std::optional<std::string> getOpt(int row, int col) const
    {
        if (isNull(row, col))
            return std::nullopt;
        return get(row, col);
    }
};

} // namespace

/** A scoped transaction; rolls back unless committed. */
struct PostgresMetadataBackend::Txn
{
    PostgresMetadataBackend & backend;
    bool committed = false;

    Txn(PostgresMetadataBackend & backend)
        : backend(backend)
    {
        backend.exec("begin");
    }

    void commit()
    {
        backend.exec("commit");
        committed = true;
    }

    ~Txn()
    {
        if (!committed) {
            try {
                backend.exec("rollback");
            } catch (...) {
                ignoreExceptionInDestructor();
            }
        }
    }
};

/* ------------------------------------------------------------------ *
 * Connection lifecycle
 * ------------------------------------------------------------------ */

PostgresMetadataBackend::PostgresMetadataBackend(const StoreDirConfig & store, const std::string & conninfo)
    : store(store)
    , conn(PQconnectdb(conninfo.c_str()))
{
    if (!conn || PQstatus(conn) != CONNECTION_OK) {
        auto msg = conn ? std::string(PQerrorMessage(conn)) : "out of memory";
        if (conn)
            PQfinish(conn);
        conn = nullptr;
        throw Error("could not connect to the metadata database: %s", msg);
    }
    initSchema();
}

PostgresMetadataBackend::~PostgresMetadataBackend()
{
    if (conn)
        PQfinish(conn);
}

void PostgresMetadataBackend::exec(const std::string & sql)
{
    Result res(PQexec(conn, sql.c_str()));
    auto status = PQresultStatus(res.res);
    if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK)
        throw Error("metadata database error: %s", PQerrorMessage(conn));
}

/**
 * Run a parameterised statement. Parameters are passed as text; a
 * `std::nullopt` parameter becomes SQL NULL.
 */
PGresult * PostgresMetadataBackend::execParams(const char * sql, const std::vector<Param> & params)
{
    std::vector<const char *> values;
    values.reserve(params.size());
    for (auto & p : params)
        values.push_back(p ? p->c_str() : nullptr);

    PGresult * res = PQexecParams(
        conn, sql, (int) params.size(), nullptr, values.data(), nullptr, nullptr, 0 /* text results */);
    auto status = PQresultStatus(res);
    if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK) {
        auto msg = std::string(PQerrorMessage(conn));
        PQclear(res);
        throw Error("metadata database error: %s", msg);
    }
    return res;
}

void PostgresMetadataBackend::initSchema()
{
    // Keep this in sync with src/libstore/distributed/schema.sql, which is
    // the documented reference DDL.
    static const char * schema = R"sql(
        create table if not exists ValidPaths (
            path             text primary key,
            hash             text not null,
            registrationTime bigint not null,
            deriver          text,
            narSize          bigint,
            ultimate         boolean not null default false,
            sigs             text,
            ca               text
        );
        create table if not exists Refs (
            referrer  text not null,
            reference text not null,
            primary key (referrer, reference),
            foreign key (referrer)  references ValidPaths(path) on delete cascade,
            foreign key (reference) references ValidPaths(path) on delete restrict
        );
        create index if not exists IndexReference on Refs(reference);
        create table if not exists DerivationOutputs (
            drv  text not null,
            id   text not null,
            path text not null,
            primary key (drv, id),
            foreign key (drv) references ValidPaths(path) on delete cascade
        );
        create index if not exists IndexDerivationOutputs on DerivationOutputs(path);
        create table if not exists Realisations (
            drvPath    text not null,
            outputName text not null,
            outputPath text not null,
            signatures text,
            primary key (drvPath, outputName)
        );
        create table if not exists GCRoots (
            link text primary key,
            path text not null
        );
        create index if not exists IndexGCRootsPath on GCRoots(path);
        create table if not exists GCLease (
            id      integer primary key,
            holder  text,
            expires bigint not null
        );
        create table if not exists TempRoots (
            node    text not null,
            path    text not null,
            expires bigint not null,
            primary key (node, path)
        );
        create index if not exists IndexTempRootsExpires on TempRoots(expires);
        create table if not exists BuildLocks (
            drv_path text primary key,
            holder   text,
            expires  bigint not null
        );
        create index if not exists IndexBuildLocksHolder on BuildLocks(holder);
    )sql";
    exec(schema);
}

/* ------------------------------------------------------------------ *
 * (De)serialisation helpers
 * ------------------------------------------------------------------ */

static std::string serialiseSigs(const std::set<Signature> & sigs)
{
    return concatStringsSep(" ", Signature::toStrings(sigs));
}

static std::set<Signature> parseSigs(const std::string & s)
{
    return Signature::parseMany(tokenizeString<StringSet>(s, " "));
}

/* ------------------------------------------------------------------ *
 * Reads
 * ------------------------------------------------------------------ */

bool PostgresMetadataBackend::isValidPath(const StorePath & path)
{
    auto lock = std::scoped_lock(mutex);
    Result res(execParams("select 1 from ValidPaths where path = $1", {store.printStorePath(path)}));
    return res.ntuples() > 0;
}

std::shared_ptr<const ValidPathInfo> PostgresMetadataBackend::queryPathInfo(const StorePath & path)
{
    auto lock = std::scoped_lock(mutex);
    Result res(execParams(
        "select hash, registrationTime, deriver, narSize, ultimate, sigs, ca from ValidPaths where path = $1",
        {store.printStorePath(path)}));
    if (res.ntuples() == 0)
        return nullptr;

    auto narHash = Hash::parseAnyPrefixed(res.get(0, 0));
    auto info = std::make_shared<ValidPathInfo>(path, UnkeyedValidPathInfo(store, narHash));
    info->registrationTime = std::stoll(res.get(0, 1));
    if (auto s = res.getOpt(0, 2))
        info->deriver = store.parseStorePath(*s);
    if (auto s = res.getOpt(0, 3))
        info->narSize = std::stoull(*s);
    info->ultimate = res.get(0, 4) == "t";
    if (auto s = res.getOpt(0, 5))
        info->sigs = parseSigs(*s);
    if (auto s = res.getOpt(0, 6))
        info->ca = ContentAddress::parseOpt(*s);

    Result refs(execParams("select reference from Refs where referrer = $1", {store.printStorePath(path)}));
    for (int i = 0; i < refs.ntuples(); ++i)
        info->references.insert(store.parseStorePath(refs.get(i, 0)));

    return info;
}

StorePathSet PostgresMetadataBackend::queryAllValidPaths()
{
    auto lock = std::scoped_lock(mutex);
    Result res(execParams("select path from ValidPaths", {}));
    StorePathSet paths;
    for (int i = 0; i < res.ntuples(); ++i)
        paths.insert(store.parseStorePath(res.get(i, 0)));
    return paths;
}

std::optional<StorePath> PostgresMetadataBackend::queryPathFromHashPart(std::string_view hashPart)
{
    if (hashPart.size() != StorePath::HashLen)
        throw Error("invalid hash part");
    auto lock = std::scoped_lock(mutex);
    std::string prefix = store.storeDir + "/" + std::string(hashPart);
    Result res(execParams("select path from ValidPaths where path >= $1 order by path limit 1", {prefix}));
    if (res.ntuples() == 0)
        return std::nullopt;
    auto p = res.get(0, 0);
    if (p.compare(0, prefix.size(), prefix) == 0)
        return store.parseStorePath(p);
    return std::nullopt;
}

void PostgresMetadataBackend::queryReferrers(const StorePath & path, StorePathSet & referrers)
{
    auto lock = std::scoped_lock(mutex);
    Result res(execParams("select referrer from Refs where reference = $1", {store.printStorePath(path)}));
    for (int i = 0; i < res.ntuples(); ++i)
        referrers.insert(store.parseStorePath(res.get(i, 0)));
}

StorePathSet PostgresMetadataBackend::queryValidDerivers(const StorePath & path)
{
    auto lock = std::scoped_lock(mutex);
    Result res(execParams("select drv from DerivationOutputs where path = $1", {store.printStorePath(path)}));
    StorePathSet derivers;
    for (int i = 0; i < res.ntuples(); ++i)
        derivers.insert(store.parseStorePath(res.get(i, 0)));
    return derivers;
}

std::map<std::string, std::optional<StorePath>>
PostgresMetadataBackend::queryDerivationOutputMap(const StorePath & deriver)
{
    auto lock = std::scoped_lock(mutex);
    Result res(execParams("select id, path from DerivationOutputs where drv = $1", {store.printStorePath(deriver)}));
    std::map<std::string, std::optional<StorePath>> outputs;
    for (int i = 0; i < res.ntuples(); ++i)
        outputs.insert_or_assign(res.get(i, 0), store.parseStorePath(res.get(i, 1)));
    return outputs;
}

std::optional<UnkeyedRealisation> PostgresMetadataBackend::queryRealisation(const DrvOutput & id)
{
    auto lock = std::scoped_lock(mutex);
    std::string drvPath{id.drvPath.to_string()};
    Result res(execParams(
        "select outputPath, signatures from Realisations where drvPath = $1 and outputName = $2",
        {drvPath, id.outputName}));
    if (res.ntuples() == 0)
        return std::nullopt;
    return UnkeyedRealisation{
        .outPath = store.parseStorePath(res.get(0, 0)),
        .signatures = res.isNull(0, 1) ? std::set<Signature>{} : parseSigs(res.get(0, 1)),
    };
}

/* ------------------------------------------------------------------ *
 * Writes
 * ------------------------------------------------------------------ */

void PostgresMetadataBackend::registerValidPaths(const ValidPathInfos & infos)
{
    auto lock = std::scoped_lock(mutex);
    Txn txn(*this);

    for (auto & [_, info] : infos) {
        Param deriver = info.deriver ? Param(store.printStorePath(*info.deriver)) : std::nullopt;
        Param narSize = info.narSize != 0 ? Param(std::to_string(info.narSize)) : std::nullopt;
        Param sigs = info.sigs.empty() ? std::nullopt : Param(serialiseSigs(info.sigs));
        Param ca = info.ca ? Param(renderContentAddress(info.ca)) : std::nullopt;
        std::string regTime = std::to_string(info.registrationTime == 0 ? time(nullptr) : info.registrationTime);

        Result res(execParams(
            R"sql(
                insert into ValidPaths (path, hash, registrationTime, deriver, narSize, ultimate, sigs, ca)
                values ($1, $2, $3, $4, $5, $6, $7, $8)
                on conflict (path) do update set
                    hash = excluded.hash, registrationTime = excluded.registrationTime,
                    deriver = excluded.deriver, narSize = excluded.narSize,
                    ultimate = excluded.ultimate, sigs = excluded.sigs, ca = excluded.ca
            )sql",
            {store.printStorePath(info.path),
             info.narHash.to_string(HashFormat::Base16, true),
             regTime,
             deriver,
             narSize,
             info.ultimate ? "true" : "false",
             sigs,
             ca}));
    }

    /* Add references once all paths exist (so the foreign keys resolve). */
    for (auto & [_, info] : infos)
        for (auto & ref : info.references)
            Result(execParams(
                "insert into Refs (referrer, reference) values ($1, $2) on conflict do nothing",
                {store.printStorePath(info.path), store.printStorePath(ref)}));

    /* Detect reference cycles among the batch (only possible for multi-output
       derivations); throw and roll back if found. */
    StorePathSet paths;
    for (auto & [_, info] : infos)
        paths.insert(info.path);
    auto topo = topoSort(
        paths,
        [&](const StorePath & path) {
            auto i = infos.find(path);
            return i == infos.end() ? StorePathSet() : i->second.references;
        });
    std::visit(
        overloaded{
            [&](const Cycle<StorePath> & cycle) {
                throw Error(
                    "cycle detected in the references of '%s' from '%s'",
                    store.printStorePath(cycle.path),
                    store.printStorePath(cycle.parent));
            },
            [](auto &) {}},
        topo);

    txn.commit();
}

void PostgresMetadataBackend::registerDerivationOutputs(
    const StorePath & deriver, const std::map<std::string, StorePath> & outputs)
{
    auto lock = std::scoped_lock(mutex);
    Txn txn(*this);
    for (auto & [outputName, outputPath] : outputs)
        Result(execParams(
            R"sql(
                insert into DerivationOutputs (drv, id, path) values ($1, $2, $3)
                on conflict (drv, id) do update set path = excluded.path
            )sql",
            {store.printStorePath(deriver), outputName, store.printStorePath(outputPath)}));
    txn.commit();
}

void PostgresMetadataBackend::addSignatures(const StorePath & path, const std::set<Signature> & sigs)
{
    auto lock = std::scoped_lock(mutex);
    Txn txn(*this);

    /* Merge with any existing signatures, matching LocalStore semantics. */
    std::set<Signature> merged = sigs;
    Result cur(execParams("select sigs from ValidPaths where path = $1", {store.printStorePath(path)}));
    if (cur.ntuples() == 0)
        throw Error("cannot add signatures to non-valid path '%s'", store.printStorePath(path));
    if (auto s = cur.getOpt(0, 0)) {
        auto existing = parseSigs(*s);
        merged.insert(existing.begin(), existing.end());
    }

    Result(execParams(
        "update ValidPaths set sigs = $1 where path = $2", {serialiseSigs(merged), store.printStorePath(path)}));
    txn.commit();
}

void PostgresMetadataBackend::invalidatePath(const StorePath & path)
{
    auto lock = std::scoped_lock(mutex);
    Txn txn(*this);
    auto p = store.printStorePath(path);
    /* Remove the self-reference first (the `on delete restrict` on
       Refs.reference would otherwise block the delete). */
    Result(execParams("delete from Refs where referrer = $1 and reference = $1", {p}));
    Result(execParams("delete from ValidPaths where path = $1", {p}));
    txn.commit();
}

void PostgresMetadataBackend::invalidatePathChecked(const StorePath & path)
{
    auto lock = std::scoped_lock(mutex);
    Txn txn(*this);
    auto p = store.printStorePath(path);
    /* Verify there are no referrers other than the path itself. */
    Result refs(execParams("select referrer from Refs where reference = $1 and referrer != $1", {p}));
    if (refs.ntuples() > 0)
        throw Error("cannot delete path '%s' because it is still referenced", p);
    Result(execParams("delete from Refs where referrer = $1 and reference = $1", {p}));
    Result(execParams("delete from ValidPaths where path = $1", {p}));
    txn.commit();
}

void PostgresMetadataBackend::registerDrvOutput(const Realisation & info)
{
    auto lock = std::scoped_lock(mutex);
    Txn txn(*this);

    std::string drvPath{info.id.drvPath.to_string()};
    Result cur(execParams(
        "select outputPath, signatures from Realisations where drvPath = $1 and outputName = $2",
        {drvPath, info.id.outputName}));

    if (cur.ntuples() > 0) {
        auto oldOutPath = store.parseStorePath(cur.get(0, 0));
        if (oldOutPath != info.outPath)
            throw Error(
                "cannot register a realisation of '%s': a conflicting one is already registered",
                info.id.to_string());
        auto merged = info.signatures;
        if (auto s = cur.getOpt(0, 1)) {
            auto existing = parseSigs(*s);
            merged.insert(existing.begin(), existing.end());
        }
        Result(execParams(
            "update Realisations set signatures = $1 where drvPath = $2 and outputName = $3",
            {serialiseSigs(merged), drvPath, info.id.outputName}));
    } else {
        Result(execParams(
            R"sql(
                insert into Realisations (drvPath, outputName, outputPath, signatures)
                values ($1, $2, $3, $4)
            )sql",
            {drvPath, info.id.outputName, store.printStorePath(info.outPath), serialiseSigs(info.signatures)}));
    }
    txn.commit();
}

/* ------------------------------------------------------------------ *
 * Garbage collection
 * ------------------------------------------------------------------ */

void PostgresMetadataBackend::addRoot(const std::string & link, const StorePath & path)
{
    auto lock = std::scoped_lock(mutex);
    Result(execParams(
        "insert into GCRoots (link, path) values ($1, $2) on conflict (link) do update set path = excluded.path",
        {link, store.printStorePath(path)}));
}

std::map<StorePath, std::set<std::string>> PostgresMetadataBackend::queryRoots()
{
    auto lock = std::scoped_lock(mutex);
    Result res(execParams("select link, path from GCRoots", {}));
    std::map<StorePath, std::set<std::string>> roots;
    for (int i = 0; i < res.ntuples(); ++i)
        roots[store.parseStorePath(res.get(i, 1))].insert(res.get(i, 0));
    return roots;
}

void PostgresMetadataBackend::removeValidPaths(const StorePathSet & paths)
{
    auto lock = std::scoped_lock(mutex);
    Txn txn(*this);
    /* Remove every reference edge touching the set first, so the subsequent
       ValidPaths deletes cannot trip the `on delete restrict` foreign key.
       This is sound because the set is closed under referrers. */
    for (auto & p : paths) {
        auto ps = store.printStorePath(p);
        Result(execParams("delete from Refs where referrer = $1 or reference = $1", {ps}));
    }
    for (auto & p : paths)
        Result(execParams("delete from ValidPaths where path = $1", {store.printStorePath(p)}));
    txn.commit();
}

bool PostgresMetadataBackend::acquireGCLease(const std::string & holder, uint64_t ttlSeconds)
{
    auto lock = std::scoped_lock(mutex);
    Txn txn(*this);
    int64_t now = (int64_t) time(nullptr);
    int64_t expires = now + (int64_t) ttlSeconds;

    /* Ensure the single lease row exists, then lock it. */
    Result(execParams("insert into GCLease (id, holder, expires) values (1, NULL, 0) on conflict (id) do nothing", {}));
    Result cur(execParams("select holder, expires from GCLease where id = 1 for update", {}));

    bool heldByOther = cur.ntuples() > 0 && !cur.isNull(0, 0) && std::stoll(cur.get(0, 1)) > now
                       && cur.get(0, 0) != holder;
    if (heldByOther) {
        txn.commit();
        return false;
    }

    Result(execParams("update GCLease set holder = $1, expires = $2 where id = 1", {holder, std::to_string(expires)}));
    txn.commit();
    return true;
}

void PostgresMetadataBackend::releaseGCLease(const std::string & holder)
{
    auto lock = std::scoped_lock(mutex);
    Result(execParams("update GCLease set holder = NULL, expires = 0 where holder = $1", {holder}));
}

void PostgresMetadataBackend::addTempRoot(const std::string & node, const StorePath & path, uint64_t ttlSeconds)
{
    auto lock = std::scoped_lock(mutex);
    int64_t expires = (int64_t) time(nullptr) + (int64_t) ttlSeconds;
    Result(execParams(
        "insert into TempRoots (node, path, expires) values ($1, $2, $3) "
        "on conflict (node, path) do update set expires = excluded.expires",
        {node, store.printStorePath(path), std::to_string(expires)}));
}

void PostgresMetadataBackend::renewTempRoots(const std::string & node, uint64_t ttlSeconds)
{
    auto lock = std::scoped_lock(mutex);
    int64_t expires = (int64_t) time(nullptr) + (int64_t) ttlSeconds;
    Result(execParams("update TempRoots set expires = $1 where node = $2", {std::to_string(expires), node}));
}

StorePathSet PostgresMetadataBackend::queryLiveTempRoots()
{
    auto lock = std::scoped_lock(mutex);
    int64_t now = (int64_t) time(nullptr);
    Result res(execParams("select distinct path from TempRoots where expires > $1", {std::to_string(now)}));
    StorePathSet paths;
    for (int i = 0; i < res.ntuples(); ++i)
        paths.insert(store.parseStorePath(res.get(i, 0)));
    return paths;
}

bool PostgresMetadataBackend::acquireBuildLock(
    const std::string & drvPath, const std::string & holder, uint64_t ttlSeconds)
{
    auto lock = std::scoped_lock(mutex);
    int64_t now = (int64_t) time(nullptr);
    int64_t expires = now + (int64_t) ttlSeconds;

    /* Atomic conditional acquire as a single auto-committed statement: claim the
       row if it is absent, already ours, or its lease has expired. `INSERT … ON
       CONFLICT DO UPDATE … WHERE` is evaluated atomically by the database, so
       two nodes racing for the same derivation can't both win. (We avoid an
       explicit transaction with `SELECT … FOR UPDATE` here: holding a row lock
       across the surrounding store operations left the row invisible to other
       connections until the build finished, defeating cross-node exclusion.) */
    Result(execParams(
        "insert into BuildLocks (drv_path, holder, expires) values ($1, $2, $3) "
        "on conflict (drv_path) do update set holder = $2, expires = $3 "
        "where BuildLocks.holder is null or BuildLocks.holder = $2 or BuildLocks.expires <= $4",
        {drvPath, holder, std::to_string(expires), std::to_string(now)}));

    /* We hold it iff the row's holder is now us. */
    Result cur(execParams("select holder from BuildLocks where drv_path = $1", {drvPath}));
    return cur.ntuples() > 0 && !cur.isNull(0, 0) && cur.get(0, 0) == holder;
}

void PostgresMetadataBackend::releaseBuildLock(const std::string & drvPath, const std::string & holder)
{
    auto lock = std::scoped_lock(mutex);
    Result(execParams("delete from BuildLocks where drv_path = $1 and holder = $2", {drvPath, holder}));
}

void PostgresMetadataBackend::renewBuildLocks(const std::string & holder, uint64_t ttlSeconds)
{
    auto lock = std::scoped_lock(mutex);
    int64_t expires = (int64_t) time(nullptr) + (int64_t) ttlSeconds;
    Result(execParams("update BuildLocks set expires = $1 where holder = $2", {std::to_string(expires), holder}));
}

} // namespace nix

#endif // NIX_WITH_POSTGRES

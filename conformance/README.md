# conformance

The TypeScript SDK's (`@particledb/sdk`) SDK-conformance bundle. One command
runs the whole suite against this SDK from source:

```
PDB_BIN=/path/to/particledb ./run_conformance.sh [--mode default|strict] [--results FILE]
```

`PDB_BIN` defaults to `$PARTICLEDB_BIN`, else `particledb` on `PATH`. The
cases, runner library and spec (`SPEC.md`) are the shared SDK conformance
suite, identical in every ParticleDB SDK repository, so this runs standalone.

| entry | what it is for |
|---|---|
| `run_conformance.sh` | `npm ci` (if needed) and `npm run build` at the repository root, starts a fresh server (unless `PDB_CONF_PORT` + `PDB_CONF_CONTROL` name one), runs the mode's cases with `tsx`, validates the results file |
| `src/` | The adapter (`README.md` there) |
| `cases/`, `lib/` | The shared conformance cases and runner library (identical in every SDK repository) -- change every copy together, never one alone |

Constraints: script cases run through the driver-layer surface the ORM
adapters (`@particledb/drizzle`, `@particledb/prisma`) use -- `reserve()` for one session and
`queryArrays()` for positional rows -- so their results are the ORM adapters'
results too. The client is protocol 1: DEF-001/002/007 FAIL (the server
records `pdb-native-v1`), and the multiplexing cases (CONC-001/002/003/005),
cancel, bounded-memory streaming and binary / in-transaction KV are SKIPPED
with the missing surface named. Skips are statements of a missing surface,
never a way to hide a failure.

Last run (1.1.19-rc.16, `default` = shipped defaults, `strict` =
`--concurrency-control strict-optimistic --durability sync`, started by
`run_conformance.sh` with `PDB_CONF_DEFAULT_PORT=39183` because 7173 was in use):

| mode | pass | fail | skip | failing ids |
|---|---|---|---|---|
| default | 54 | 6 | 9 | DEF-001, DEF-002, DEF-007 (protocol 1), SQL-MULTI-001, SQL-MULTI-002 (multi-statement strings run only on the protocol 2 mux; the protocol 1 query request answers 42601), DEF-005 (only because the server was moved off its default port: the SDK default 7173 is the server default) |
| strict | 39 | 3 | 2 | DEF-001, DEF-002, SQL-MULTI-001 |

CONC-004 holds both transactions open until both have read: under the READ
COMMITTED session default a side that committed early became visible to the
other side's reads, which failed the case by timing alone.

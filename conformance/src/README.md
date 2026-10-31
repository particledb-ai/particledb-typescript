# conformance/src

| File | What it is for |
|------|----------------|
| `adapter.ts` | The whole adapter: env contract, a minimal RESP2 client (the independent KV side), canonical binding / rendering (the conformance specification, section 4; arrays arrive as PostgreSQL array text and are parsed), the `script` interpreter (one `reserve()`d connection per script `conn`; `isolation` = `SET SESSION CHARACTERISTICS`), every `custom` case by id, and the results file. Run by `../run_conformance.sh` with `tsx` from the repository root. |

# Workspace Context

Last updated: 2026-05-20

## Current Focus
- Project: incremental REL_ID -> REL_ID_REGIE_KLANT mapping tests in Spark notebooks.
- Main notebooks:
  - notebooks/siebel_id_update.ipynb
  - notebooks/siebel_id_test.ipynb

## Recent State
- Added a new simultaneous-update scenario in the test notebook (Case 9):
  - Inserts new rel_ids into both direct_bank and ggm_np in the same run.
  - Links both new rel_ids to an existing active REL_ID_REGIE_KLANT.
  - Includes idempotency rerun assertions.
- Fixed a typo in update logic:
  - withColumndvffdbs(...) -> withColumn(...)

## Validation Snapshot
- Full test notebook run completed successfully through baseline + Cases 1-9.
- Case 9 passed, including idempotency checks.

## Notes
- If notebook outputs need a clean state, clear code-cell outputs/execution counts before sharing/committing.
- Re-run from setup + hard reset cells for deterministic replay.

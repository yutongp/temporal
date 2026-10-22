# Temporal 1.22.7 → 1.29.7 on Render (Phase 1)

One minor at a time, highest patch of each, in the order Render needs:
Elasticsearch mapping (when the step has one), then frontend (its auto-setup
applies the Postgres schema), history, matching, worker. Rehearsed twice
locally with the Blueprint's Dockerfiles, render.yaml's env vars and the
Render plan limits; see [Rehearsal](#rehearsal) for what was checked.

| Step | Commit | Version | Postgres schema | ES fields to add first | Also |
|---|---|---|---|---|---|
| 1 | `bf52fc1` | 1.23.1 | 1.10 → 1.11 | `ParentWorkflowId`, `ParentRunId` (v6) | |
| 2 | `6cafb9c` | 1.24.3 | 1.11 → 1.12 | `RootWorkflowId`, `RootRunId` (v7) | `postgres` → `postgres12` plugin, `schema/postgresql/v96` → `v12` |
| 3 | `56fddf9` | 1.25.2 | 1.12 → 1.14 | none | carries `b869d5e`, the history cache cap |
| 4 | `0a91750` | 1.26.3 | 1.14 (no change) | `TemporalPauseInfo` (v8) | |
| 5 | `80efede` | 1.27.4 | 1.14 → 1.16 | `TemporalWorkerDeploymentVersion`, `TemporalWorkflowVersioningBehavior`, `TemporalWorkerDeployment` (v9) | skips 1.27.0 (broken Postgres schema upgrade) |
| 6 | `538b670` | 1.28.4 | 1.16 → 1.17 | none | |
| 7 | `f63f50d` | 1.29.7 | 1.17 → 1.18 | none | last release with auto-setup, tctl, dockerize |

Each commit contains every earlier one, so deploying step N's commit to a
service brings that service to step N.

## Before you start

1. **Back up temporal-db.** Temporal has no downgrade: once a frontend has
   applied a schema step, the way back is a restore. Note the time, or take a
   manual backup from the database's page in the Render dashboard.
2. **Read-only checks from the temporal-frontend Shell** (Render dashboard →
   temporal-frontend → Shell):

   ```bash
   temporal operator cluster system --address localhost:7233
   ```
   ServerVersion should be 1.22.7.

   ```bash
   curl -s -u "$ES_USER:$ES_PWD" "http://$ES_SEEDS:9200/temporal_visibility_v1_dev/_mapping" | grep -o '"[A-Za-z]*":{"type"' | cut -d'"' -f2 | sort | tr '\n' ' '
   ```
   The list must include `BuildIds HistorySizeBytes TemporalNamespaceDivision
   TemporalSchedulePaused TemporalScheduledById TemporalScheduledStartTime`
   (the v2–v5 fields 1.22 expects). The index is only created once, so if it
   was created by an older Temporal, some may be missing; add them the same
   way as the steps below (`date_nanos` for `TemporalScheduledStartTime`,
   `boolean` for `TemporalSchedulePaused`, `long` for `HistorySizeBytes`,
   `keyword` for the others) before step 1. The rehearsal index was created
   on 1.22.7, so this case was not exercised.
3. **Postgres schema is 1.10**: temporal-db → Connect → PSQL command, then
   `select curr_version from schema_version;` (read-only).
4. **Plan one sitting.** Locally each step's four deploys took 9–16 s with
   cached images. On Render every service rebuilds its image first, so
   budget roughly 10–20 min per step (estimate, not measured), about
   2 hours in all. Nothing is needed on the app side: temporalio 1.5.0 worked
   through every step and replays histories written by 1.29.7.

## Each step

Main is what the services build from and `autoDeploy` is off, so a step is:
move `main` to the step's commit, then deploy the services one at a time.

```bash
git fetch origin
git push origin <commit>:main      # fast-forward main to this step
```

(If the PR was merged with a merge commit or rebase instead, use Manual
Deploy → deploy a specific commit with the same SHAs. Do not squash-merge:
the per-step commits are the deploy units.)

1. **ES mapping** (steps 1, 2, 4, 5 only), from the **current**
   temporal-frontend Shell, before deploying anything:

   ```bash
   curl -sS --fail -u "$ES_USER:$ES_PWD" -X PUT "http://$ES_SEEDS:9200/temporal_visibility_v1_dev/_mapping" \
     -H 'Content-Type: application/json' -d '<MAPPING>'
   ```
   It prints `{"acknowledged":true}`. `<MAPPING>` per step:
   - step 1: `{"properties":{"ParentWorkflowId":{"type":"keyword"},"ParentRunId":{"type":"keyword"}}}`
   - step 2: `{"properties":{"RootWorkflowId":{"type":"keyword"},"RootRunId":{"type":"keyword"}}}`
   - step 4: `{"properties":{"TemporalPauseInfo":{"type":"keyword"}}}`
   - step 5: `{"properties":{"TemporalWorkerDeploymentVersion":{"type":"keyword"},"TemporalWorkflowVersioningBehavior":{"type":"keyword"},"TemporalWorkerDeployment":{"type":"keyword"}}}`

   These are the `new_mapping` bodies of Temporal's
   `schema/elasticsearch/visibility/versioned/vN/upgrade.sh` (v6 at 1.23, v7
   at 1.24, v8 at 1.26, v9 at 1.27). The scripts ship in the auto-setup image
   under `/etc/temporal/schema/...`, but they pipe through `jq`, which no
   image has (1.22.7 to 1.29.7), so run the PUT directly. It only adds
   fields: no reindex, and older servers ignore them.
2. **temporal-frontend → Manual Deploy → Deploy latest commit.** Wait for
   "Deploy live". Its log must show the schema step, then the server start:
   ```
   + temporal-sql-tool --plugin postgres12 ... update-schema -d /etc/temporal/schema/postgresql/v12/temporal/versioned
   Schema updated from 1.11 to 1.12
   UpdateSchemaTask done
   Temporal server started.
   ```
   (Steps 1 log `--plugin postgres` and `v96`; step 4 has no update.)
   From 1.24 the container exits if this part fails; see
   [If something goes wrong](#if-something-goes-wrong).
3. **temporal-history**, Deploy latest commit; wait for live.
4. **temporal-matching**, same.
5. **temporal-worker**, same.
6. **Restart our app's Temporal worker service** (the `srv-cm1r1sun7f5s73eo89ag`
   pods) once temporal-worker is live. After the Phase 0 deploys on
   2026-10-21, one pod's worker process (vfb5r) stayed connected but
   broken: Temporal handed it 4 system-job tasks at 23:51 that it never ran,
   and they failed at their timeouts until the process restarted at about
   23:52. The rehearsal's worker never hit this, so treat it as Render-only.
   Restarting right away closes the window.
7. **Check** from the new temporal-frontend Shell:
   ```bash
   temporal operator cluster system --address localhost:7233          # ServerVersion = this step
   curl -s -u "$ES_USER:$ES_PWD" "http://$ES_SEEDS:9200/temporal_visibility_v1_dev/_mapping" | grep -o '"<new field>"'
   temporal workflow list --address localhost:7233 --limit 20 \
     --query "(ExecutionStatus='Failed' OR ExecutionStatus='TimedOut') AND CloseTime > '<step start, UTC>'"
   temporal schedule describe --address localhost:7233 -s production-system-jobs-tick-schedule -o json | grep -iE 'actual_?time'
   ```
   and the same for `staging-system-jobs-tick-schedule`. The last 10 action
   times should be one per minute up to now, with no gap around the deploy. Also check
   `select curr_version from schema_version;` matches the table.
8. **Logs.** These ERROR lines are expected, all within about a second of the
   matching deploy (plus one `sticky worker unavailable` at the next
   schedule tick). Anything else is new:
   - matching: `GetTaskQueue operation failed ... Error: context canceled`,
     `Failed to lock task queue. Error: context canceled`,
     `pq: canceling statement due to user request`,
     `sticky worker unavailable, please use original task queue.`
   - history: `Failed to lock shard. Previous range ID ...`, `shard closed`,
     `Failed to update shard. Previous range ID ...`, `Shard is owned by ...
     but not by ...` (old and new history both briefly own shards).
   - frontend and history, from 1.26.3: `long poll to refresh Nexus endpoints
     returned error` / `Nexus endpoints table ownership lost`.

   Once in the rehearsal (1.24.3, run 2) the new history kept a stale shard
   from that overlap, dropped it 27 s later and re-acquired it at its
   once-a-minute shard check: workflows on that shard (1 of 4) paused for
   33 s. Nothing failed and no schedule run was missed. This comes from the
   overlapping deploys Render does, not from the upgrade, but every step can
   hit it: a schedule whose shard pauses for more than a minute skips that
   run, because the SDK's default catchup window is 1 minute.

After upgrade step 7, fast-forward `main` to the branch tip (this runbook; nothing to
deploy).

**region-eu** follows the same steps against the EU services, moving
`region-eu` one step at a time with `git merge --no-ff <commit>` (the step
commits don't touch render.yaml, so they merge cleanly).

## Memory (step 3 onwards)

From 1.25 the history service caches workflows per host: up to 128,000
entries, 1 h TTL, about 35 KiB each (measured). Uncapped, a 512 MB history
would run out of memory at roughly 12k workflows touched per hour. Commit
`b869d5e` caps it at 2048 entries (what 4 shards × 512 allowed before) and
ships with step 3. Watch temporal-history memory in Render's metrics after
step 3; it stayed under 200 MiB locally with 10,000 open workflows.

## If something goes wrong

- **Stop at the step that failed.** Don't deploy history/matching/worker
  ahead of the frontend: the frontend applies the schema the new versions
  need.
- **Frontend doesn't go live** (auto-setup error in its log): with
  zero-downtime deploys Render should keep the previous instance serving (not
  tested here). Fix forward or redeploy the previous step's
  commit to the frontend. The schema steps are additive and the older
  version runs against them; that's what the rehearsal did every step, with
  old history/matching/worker running for a few seconds against the new
  schema.
- **Going back more than one step** is not supported by Temporal: restore the
  backup from "Before you start".

## Rehearsal

`rehearsal/` runs the Blueprint locally (`docker-compose.yml`: same
Dockerfiles and env vars, Render CPU/memory limits as container limits),
deploys like Render (`deploy.sh`: new instance beside the old, then the old
one stops), and drives a temporalio 1.5.0 workload through each step
(`harness/harness.py`). `step.sh` is one step in the order above;
`verify.sh` is the check list.

```bash
cd rehearsal
docker compose --profile harness up -d --build      # 1.22.7 from the kit commit
docker compose --profile harness run --rm harness python /harness/harness.py seed
git checkout <step commit> && ./step.sh v1.23.1 '<MAPPING>' && ./verify.sh v1.23.1 <step start>
```

Workload: an every-minute schedule (SKIP overlap) running a one-activity
workflow; timers of 5 min to 4 h started before the first step and before
each step; a workflow with a query handler and `workflow.patched`, signalled
every step; a `continue_as_new` chain every 2 min; activities that fail 2 or
3 times under retry policies (one retrying across each deploy); and, from
1.25.2, 3000 open workflows. After every step: server version, schema
version, ES fields, schedule gaps and start delay, timer lateness, query
answers, chain progress, retries, visibility queries (WorkflowType,
ExecutionStatus, StartTime, STARTS_WITH), failed/timed-out/terminated
workflows, histories replayed with the 1.5.0 Replayer, ERROR lines per
service, and memory.

### Results

Run 2 is the final commits above, from a clean 1.22.7 cluster (02:23–02:53
UTC on 2026-10-22). Deploy is the four services, Render-style, with images
already pulled.

| Step | Deploy | Server | Schema | ES fields | Schedule runs, missed, max start delay | Timers late (max) | Failed/timed-out | History memory |
|---|---|---|---|---|---|---|---|---|
| baseline | | 1.22.7 | 1.10 | 29 | 5, 0, 0.1 s | | 0 | 42 MiB |
| 1 | 8 s | 1.23.1 | 1.11 | 31 | 8, 0, 0.1 s | | 0 | 88 MiB |
| 2 | 10 s | 1.24.3 | 1.12 | 33 | 12, 0, 1.9 s | 0.1 s | 0 | 85 MiB |
| 3 | 14 s | 1.25.2 | 1.14 | 33 | 15, 0, 1.9 s | 0.1 s | 0 | 58 MiB; 138 MiB with 3000 open workflows |
| 4 | 14 s | 1.26.3 | 1.14 | 34 | 20, 0, 1.9 s | 0.1 s | 0 | 70 MiB |
| 5 | 15 s | 1.27.4 | 1.16 | 37 | 23, 0, 1.9 s | 0.1 s | 0 | 70 MiB |
| 6 | 16 s | 1.28.4 | 1.17 | 37 | 26, 0, 1.9 s | 0.1 s | 0 | 57 MiB |
| 7 | 15 s | 1.29.7 | 1.18 | 37 | 30, 0, 1.9 s | 0.2 s | 0 | 59 MiB |

Every step also had: queries answered, every signal handled through the
`workflow.patched` branch, the `continue_as_new` chain advancing, all
retrying activities completing, visibility queries by WorkflowType,
ExecutionStatus and StartTime working (`STARTS_WITH` from 1.23.1; 1.22.7
rejects it), histories replaying on the 1.5.0 Replayer, and a 1 s frontend
probe that never failed. ERROR lines were only the handover ones listed in
step 7. A timer started on 1.22.7 at 02:23:04 fired on 1.29.7 at 02:53:04,
0.1 s late. The UI (2.21.3, unchanged) works against 1.29.7.

Run 1 (same steps, before the cap commit moved ahead of 1.25.2) passed the
same checks. Its timers started on 1.22.7 fired after steps 2, 4 and 7,
each at most 0.1 s late. It also gave the memory numbers: uncapped,
5000 cached workflows took history from 86 to 260 MiB; with the cap, 10000
took it to 187 MiB.

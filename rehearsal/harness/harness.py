"""Workload that rides through the server upgrade.

    harness.py worker          # long-running worker (the compose `harness` service)
    harness.py seed            # baseline: schedule, sleepers, query/patch, chain, retries
    harness.py step LABEL      # before a deploy: more sleepers/retries, one signal
    harness.py check LABEL     # after a deploy: everything the runbook checks, JSON in /out
    harness.py sleepers L S..  # extra sleepers of S seconds each
    harness.py load N S [I]    # N sleepers of S seconds (rh-load-*), to load the history cache
    harness.py probe           # 1 s frontend probe, logs failures (deploy downtime)
    harness.py finish          # end the long-running workflows
"""

import asyncio
import json
import os
import sys
from datetime import datetime, timedelta, timezone

from temporalio import activity, workflow
from temporalio.api.workflowservice.v1 import (
    DescribeNamespaceRequest,
    GetClusterInfoRequest,
)
from temporalio.client import (
    Client,
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleOverlapPolicy,
    SchedulePolicy,
    ScheduleSpec,
    WorkflowExecutionStatus,
)
from temporalio.common import RetryPolicy
from temporalio.worker import Replayer, Worker

ADDRESS = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
QUEUE = "rehearsal"
OUT = "/out"
SCHEDULE_ID = "rh-schedule"
# Sleepers started at seed; durations are chosen to land inside later steps.
SEED_SLEEPS = [600, 1200, 1800, 2700, 3600, 5400, 7200, 9000, 10800, 14400]
STEP_SLEEPS = [300, 900, 1800]
CHAIN_SLEEP = 120


def now() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------- activities


@activity.defn
async def tick(label: str) -> str:
    return f"{label} at {now().isoformat()}"


@activity.defn
async def flaky(fail_times: int) -> int:
    attempt = activity.info().attempt
    if attempt <= fail_times:
        raise RuntimeError(f"planned failure on attempt {attempt}")
    return attempt


# ----------------------------------------------------------------- workflows


@workflow.defn
class TickWorkflow:
    @workflow.run
    async def run(self) -> str:
        return await workflow.execute_activity(
            tick, "schedule", start_to_close_timeout=timedelta(seconds=30)
        )


@workflow.defn
class SleeperWorkflow:
    @workflow.run
    async def run(self, seconds: int) -> dict:
        started = workflow.now()
        await asyncio.sleep(seconds)
        fired = workflow.now()
        await workflow.execute_activity(
            tick, "sleeper", start_to_close_timeout=timedelta(seconds=30)
        )
        return {
            "planned_s": seconds,
            "late_s": (fired - started).total_seconds() - seconds,
            "due": (started + timedelta(seconds=seconds)).isoformat(),
            "fired": fired.isoformat(),
        }


@workflow.defn
class QueryPatchWorkflow:
    def __init__(self) -> None:
        self.bumps: list = []
        self.pending: list = []
        self.done = False

    @workflow.signal
    def bump(self, label: str) -> None:
        self.pending.append(label)

    @workflow.signal
    def finish(self) -> None:
        self.done = True

    @workflow.query
    def state(self) -> dict:
        return {"bumps": self.bumps, "pending": self.pending, "done": self.done}

    @workflow.run
    async def run(self) -> list:
        while True:
            await workflow.wait_condition(lambda: bool(self.pending) or self.done)
            while self.pending:
                label = self.pending.pop(0)
                if workflow.patched("rehearsal-echo"):
                    label = await workflow.execute_activity(
                        tick, label, start_to_close_timeout=timedelta(seconds=30)
                    )
                self.bumps.append(label)
            if self.done:
                return self.bumps


@workflow.defn
class ChainWorkflow:
    def __init__(self) -> None:
        self.n = 0

    @workflow.query
    def current(self) -> int:
        return self.n

    @workflow.run
    async def run(self, n: int, limit: int) -> int:
        self.n = n
        await asyncio.sleep(CHAIN_SLEEP)
        await workflow.execute_activity(
            tick, f"chain {n}", start_to_close_timeout=timedelta(seconds=30)
        )
        if n >= limit:
            return n
        workflow.continue_as_new(args=[n + 1, limit])


@workflow.defn
class RetryWorkflow:
    @workflow.run
    async def run(self, fail_times: int, initial_s: int) -> int:
        return await workflow.execute_activity(
            flaky,
            fail_times,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=initial_s),
                backoff_coefficient=2.0,
                maximum_attempts=fail_times + 3,
            ),
        )


WORKFLOWS = [TickWorkflow, SleeperWorkflow, QueryPatchWorkflow, ChainWorkflow, RetryWorkflow]


# ------------------------------------------------------------------ commands


async def connect() -> Client:
    while True:
        try:
            return await Client.connect(ADDRESS)
        except Exception as e:  # noqa: BLE001 - keep retrying while the frontend restarts
            print(f"{now().isoformat()} connect failed: {e}", flush=True)
            await asyncio.sleep(2)


async def cmd_worker() -> None:
    client = await connect()
    worker = Worker(client, task_queue=QUEUE, workflows=WORKFLOWS, activities=[tick, flaky])
    print(f"{now().isoformat()} worker running on {QUEUE}", flush=True)
    await worker.run()


async def start_sleepers(client: Client, label: str, durations: list) -> None:
    for s in durations:
        await client.start_workflow(
            SleeperWorkflow.run, s, id=f"rh-sleep-{label}-{s}", task_queue=QUEUE
        )


async def start_retries(client: Client, label: str) -> None:
    # fast: retries 2 s / 4 s apart; slow: 60 s / 120 s / 240 s apart, across the deploy.
    await client.start_workflow(
        RetryWorkflow.run, args=[2, 2], id=f"rh-retry-{label}-fast", task_queue=QUEUE
    )
    await client.start_workflow(
        RetryWorkflow.run, args=[3, 60], id=f"rh-retry-{label}-slow", task_queue=QUEUE
    )


async def cmd_seed() -> None:
    client = await connect()
    await client.create_schedule(
        SCHEDULE_ID,
        Schedule(
            action=ScheduleActionStartWorkflow(
                TickWorkflow.run, id="rh-tick", task_queue=QUEUE
            ),
            spec=ScheduleSpec(cron_expressions=["* * * * *"]),
            policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP),
        ),
    )
    await start_sleepers(client, "seed", SEED_SLEEPS)
    await client.start_workflow(QueryPatchWorkflow.run, id="rh-query", task_queue=QUEUE)
    await client.start_workflow(
        ChainWorkflow.run, args=[0, 200], id="rh-chain", task_queue=QUEUE
    )
    await start_retries(client, "seed")
    await client.get_workflow_handle("rh-query").signal(QueryPatchWorkflow.bump, "seed")
    print(f"{now().isoformat()} seeded", flush=True)


async def cmd_step(label: str) -> None:
    client = await connect()
    await start_sleepers(client, label, STEP_SLEEPS)
    await start_retries(client, label)
    await client.get_workflow_handle("rh-query").signal(QueryPatchWorkflow.bump, label)
    print(f"{now().isoformat()} step {label} started", flush=True)


async def cmd_sleepers(label: str, *seconds: str) -> None:
    client = await connect()
    await start_sleepers(client, label, [int(s) for s in seconds])
    print(f"{now().isoformat()} sleepers {label} {seconds} started", flush=True)


async def cmd_load(n: str, seconds: str, first: str = "0") -> None:
    client = await connect()
    for i in range(int(first), int(first) + int(n)):
        await client.start_workflow(
            SleeperWorkflow.run, int(seconds), id=f"rh-load-{i}", task_queue=QUEUE
        )
    print(f"{now().isoformat()} started {n} load sleepers of {seconds}s", flush=True)


async def listing(client: Client, query: str) -> list:
    return [w async for w in client.list_workflows(query)]


async def cmd_check(label: str) -> dict:
    client = await connect()
    t = now()
    report: dict = {"label": label, "at": t.isoformat()}

    info = await client.workflow_service.get_cluster_info(GetClusterInfoRequest())
    report["server_version"] = info.server_version

    # Schedule: every minute since the first run, no gaps, starts on time.
    desc = await client.get_schedule_handle(SCHEDULE_ID).describe()
    ticks = await listing(client, "WorkflowType='TickWorkflow'")
    scheduled = sorted(
        datetime.fromisoformat(w.id.removeprefix("rh-tick-").replace("Z", "+00:00"))
        for w in ticks
    )
    late = [
        (w.start_time - datetime.fromisoformat(w.id.removeprefix("rh-tick-").replace("Z", "+00:00"))).total_seconds()
        for w in ticks
    ]
    missing = []
    if scheduled:
        m = scheduled[0]
        have = set(scheduled)
        while m <= scheduled[-1]:
            if m not in have:
                missing.append(m.isoformat())
            m += timedelta(minutes=1)
    report["schedule"] = {
        "num_actions": desc.info.num_actions,
        "missed_catchup_window": desc.info.num_actions_missed_catchup_window,
        "skipped_overlap": desc.info.num_actions_skipped_overlap,
        "runs_in_visibility": len(ticks),
        "first": scheduled[0].isoformat() if scheduled else None,
        "last": scheduled[-1].isoformat() if scheduled else None,
        "missing_minutes": missing,
        "max_start_delay_s": round(max(late), 1) if late else None,
        "starts_delayed_over_10s": sum(1 for x in late if x > 10),
        "next": [x.isoformat() for x in desc.info.next_action_times[:1]],
    }

    # Sleepers: fired on time; none overdue.
    sleepers = [
        w
        for w in await listing(client, "WorkflowType='SleeperWorkflow'")
        if w.id.startswith("rh-sleep-")
    ]
    fired, overdue, running = [], [], 0
    for w in sleepers:
        if w.status == WorkflowExecutionStatus.COMPLETED:
            r = await client.get_workflow_handle(w.id, run_id=w.run_id).result()
            fired.append({"id": w.id, "late_s": round(r["late_s"], 1), "fired": r["fired"]})
        elif w.status == WorkflowExecutionStatus.RUNNING:
            running += 1
            planned = int(w.id.rsplit("-", 1)[1])
            if t > w.start_time + timedelta(seconds=planned + 60):
                overdue.append(w.id)
    fired.sort(key=lambda x: x["fired"])
    report["sleepers"] = {
        "completed": len(fired),
        "running": running,
        "overdue": overdue,
        "max_late_s": max((x["late_s"] for x in fired), default=None),
        "fired": fired,
    }

    # Query + patched workflow.
    q = client.get_workflow_handle("rh-query")
    report["query"] = await q.query(QueryPatchWorkflow.state)

    # continue_as_new chain.
    chain = client.get_workflow_handle("rh-chain")
    cdesc = await chain.describe()
    report["chain"] = {
        "current_n": await chain.query(ChainWorkflow.current),
        "current_run_started": cdesc.start_time.isoformat(),
        "stalled": t > cdesc.start_time + timedelta(seconds=CHAIN_SLEEP + 90),
        "continued_as_new_runs": len(
            await listing(client, "WorkflowId='rh-chain' AND ExecutionStatus='ContinuedAsNew'")
        ),
    }

    # Activity retries.
    retries = await listing(client, "WorkflowType='RetryWorkflow'")
    report["retries"] = {
        "completed": sum(1 for w in retries if w.status == WorkflowExecutionStatus.COMPLETED),
        "running": sum(1 for w in retries if w.status == WorkflowExecutionStatus.RUNNING),
        "total": len(retries),
    }

    # Visibility queries.
    since = (t - timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
    vis = {}
    for name, query in [
        ("by_type", "WorkflowType='SleeperWorkflow'"),
        ("running", "ExecutionStatus='Running'"),
        ("completed", "ExecutionStatus='Completed'"),
        ("started_last_30m", f"StartTime > '{since}'"),
        ("type_and_status", "WorkflowType='TickWorkflow' AND ExecutionStatus='Completed'"),
        ("starts_with", "WorkflowId STARTS_WITH 'rh-sleep-'"),
    ]:
        try:
            vis[name] = len(await listing(client, query))
        except Exception as e:  # noqa: BLE001 - record unsupported queries
            vis[name] = f"error: {getattr(e, 'message', e)}"
    report["visibility"] = vis

    # Nothing failed, timed out, terminated or canceled.
    bad = await listing(
        client,
        "ExecutionStatus='Failed' OR ExecutionStatus='TimedOut' "
        "OR ExecutionStatus='Terminated' OR ExecutionStatus='Canceled'",
    )
    report["bad"] = [f"{w.id} {w.status.name if w.status else None}" for w in bad]

    # Histories written by this server still replay on SDK 1.5.0.
    replayer = Replayer(workflows=WORKFLOWS)
    replayed = []
    for wid in ["rh-query", "rh-chain"]:
        h = await client.get_workflow_handle(wid).fetch_history()
        await replayer.replay_workflow(h)
        replayed.append(f"{wid} ({len(h.events)} events)")
    last_sleeper = next((x["id"] for x in reversed(fired)), None)
    if last_sleeper:
        h = await client.get_workflow_handle(last_sleeper).fetch_history()
        await replayer.replay_workflow(h)
        replayed.append(f"{last_sleeper} ({len(h.events)} events)")
    report["replayed_ok"] = replayed

    os.makedirs(OUT, exist_ok=True)
    with open(f"{OUT}/check-{label}.json", "w") as f:
        json.dump(report, f, indent=2, default=str)
    summary = {
        "version": report["server_version"],
        "sched": {k: v for k, v in report["schedule"].items() if k != "next"},
        "sleepers": {k: v for k, v in report["sleepers"].items() if k != "fired"},
        "query_bumps": len(report["query"]["bumps"]),
        "query_pending": report["query"]["pending"],
        "chain": report["chain"],
        "retries": report["retries"],
        "visibility": vis,
        "bad": report["bad"],
        "replayed_ok": replayed,
    }
    print(json.dumps(summary, indent=1, default=str))
    return report


async def cmd_probe() -> None:
    client = await connect()
    down_since = None
    while True:
        try:
            await asyncio.wait_for(
                client.workflow_service.describe_namespace(
                    DescribeNamespaceRequest(namespace="default")
                ),
                timeout=5,
            )
            if down_since:
                print(f"{now().isoformat()} UP after {(now() - down_since).total_seconds():.1f}s", flush=True)
                down_since = None
        except Exception as e:  # noqa: BLE001 - any failure counts as down
            if not down_since:
                down_since = now()
                print(f"{down_since.isoformat()} DOWN {type(e).__name__}: {e}", flush=True)
        await asyncio.sleep(1)


async def cmd_finish() -> None:
    client = await connect()
    await client.get_workflow_handle("rh-query").signal(QueryPatchWorkflow.finish)
    print(await client.get_workflow_handle("rh-query").result())


if __name__ == "__main__":
    cmd, *rest = sys.argv[1:]
    asyncio.run(
        {
            "worker": cmd_worker,
            "seed": cmd_seed,
            "step": cmd_step,
            "check": cmd_check,
            "sleepers": cmd_sleepers,
            "load": cmd_load,
            "probe": cmd_probe,
            "finish": cmd_finish,
        }[cmd](*rest)
    )

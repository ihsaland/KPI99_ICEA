"""
Spark event log ingestion: parse JSON/JSON.gz/.zip event logs and aggregate per-job metrics.
Supports SparkListenerJobStart, SparkListenerJobEnd, SparkListenerTaskEnd.
"""
import gzip
import json
import zipfile
from collections import defaultdict
from io import BytesIO
from typing import BinaryIO


def normalize_eventlog_content(content: bytes, filename: str = "") -> list[tuple[bytes, str]]:
    """
    If content is a .zip, extract all event log files (.json / .json.gz / .gz) and return
    a list of (content, inner_name) so the caller can parse and merge. Spark may write
    one or many JSON files per log. Otherwise return [(content, filename)].
    """
    if not content or len(content) < 4:
        return [(content, filename)] if content else []
    # ZIP magic: PK\x03\x04
    if content[:2] != b"PK":
        return [(content, filename)]
    try:
        z = zipfile.ZipFile(BytesIO(content), "r")
    except zipfile.BadZipFile:
        return [(content, filename)]
    candidates = [n for n in z.namelist() if not n.endswith("/")]
    # Event log members: explicit .json/.gz extensions OR Spark-style events_* (exclude appstatus)
    json_like = [
        n for n in candidates
        if n.lower().endswith(".json") or n.lower().endswith(".json.gz") or n.lower().endswith(".gz")
    ]
    # Spark zips often use names like events_0_appid, events_1_appid with no extension
    events_like = [
        n for n in candidates
        if "events" in n.lower() and "appstatus" not in n.lower()
    ]
    combined = list(dict.fromkeys(json_like + events_like))  # prefer json_like first, then events_*
    names = combined if combined else candidates
    out = [(z.read(n), n) for n in names]
    z.close()
    return out if out else [(content, filename)]


def _get(d: dict, *keys: str):
    """Get first existing key from dict (handles 'Stage ID' vs 'StageId' etc.)."""
    for k in keys:
        if k in d:
            return d[k]
    return None


def _get_nested(d: dict, *paths):
    """Follow path like ('Task Info', 'Finish Time') or ('Task Metrics', 'Executor Run Time')."""
    for path in paths:
        v = d
        for key in path:
            v = _get(v, key, key.replace(" ", "")) if isinstance(v, dict) else None
            if v is None:
                break
        if v is not None:
            return v
    return None


def _read_event_lines(content: bytes, filename: str = "") -> list[dict]:
    """Yield parsed JSON events (one per line). Handles .gz if content looks gzipped or filename ends with .gz."""
    is_gz = filename.lower().endswith(".gz") or content[:2] == b"\x1f\x8b"
    stream: BinaryIO = gzip.GzipFile(fileobj=BytesIO(content)) if is_gz else BytesIO(content)
    events = []
    for line in stream:
        if not isinstance(line, bytes):
            line = line.encode("utf-8") if isinstance(line, str) else line
        line = line.strip()
        if not line:
            continue
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return events


def parse_event_log(
    content: bytes | None = None,
    filename: str = "",
    events: list[dict] | None = None,
) -> tuple[dict[int, set[int]], dict[int, dict], dict[int, dict]]:
    """
    Parse event log and return:
    - job_stages: job_id -> set of stage_ids
    - job_times: job_id -> {start_time_ms?, end_time_ms?, result?}
    - stage_metrics: stage_id -> {executor_run_time_ms, bytes_read, bytes_written, task_count}

    Call with either (content, filename) or (events=...) for a single pass over pre-read events (e.g. from many zip members).
    """
    if events is None:
        events = _read_event_lines(content or b"", filename)
    job_stages: dict[int, set[int]] = defaultdict(set)
    job_times: dict[int, dict] = defaultdict(dict)
    stage_metrics: dict[int, dict] = defaultdict(lambda: {
        "executor_run_time_ms": 0, "bytes_read": 0, "bytes_written": 0, "task_count": 0,
        "peak_execution_memory_bytes": 0, "executor_cpu_time_ns": 0,
        "memory_spilled_bytes": 0, "disk_spilled_bytes": 0,
        "shuffle_read_bytes": 0, "shuffle_write_bytes": 0,
    })

    for ev in events:
        event_type = _get(ev, "Event")
        if not event_type:
            continue

        if event_type == "SparkListenerJobStart":
            job_id = _get(ev, "Job ID", "JobId")
            if job_id is None:
                continue
            stage_infos = _get(ev, "Stage Infos", "StageInfos") or []
            for si in stage_infos:
                stage_id = _get(si, "Stage ID", "StageId")
                if stage_id is not None:
                    job_stages[job_id].add(stage_id)
            submission = _get(ev, "Submission Time", "SubmissionTime")
            if submission is not None:
                job_times[job_id]["start_time_ms"] = submission

        elif event_type == "SparkListenerJobEnd":
            job_id = _get(ev, "Job ID", "JobId")
            if job_id is None:
                continue
            completion = _get(ev, "Completion Time", "CompletionTime")
            if completion is not None:
                job_times[job_id]["end_time_ms"] = completion
            job_result = _get(ev, "Job Result", "JobResult")
            if job_result:
                job_times[job_id]["result"] = _get(job_result, "Result", "ClassName", str(job_result))

        elif event_type == "SparkListenerTaskEnd":
            stage_id = _get(ev, "Stage ID", "StageId")
            if stage_id is None:
                continue
            task_info = _get(ev, "Task Info", "TaskInfo") or {}
            task_metrics = _get(ev, "Task Metrics", "TaskMetrics") or {}
            run_time = _get(task_metrics, "Executor Run Time", "Executor RunTime", "ExecutorRunTime")
            if run_time is not None:
                stage_metrics[stage_id]["executor_run_time_ms"] += int(run_time)
            stage_metrics[stage_id]["task_count"] += 1
            # Executor CPU Time (nanoseconds in Spark)
            cpu_ns = _get(task_metrics, "Executor CPU Time", "Executor CPUTime", "ExecutorCpuTime")
            if cpu_ns is not None:
                stage_metrics[stage_id]["executor_cpu_time_ns"] += int(cpu_ns)
            # Peak Execution Memory (bytes) — max across tasks
            peak_mem = _get(task_metrics, "Peak Execution Memory", "PeakExecutionMemory")
            if peak_mem is not None:
                stage_metrics[stage_id]["peak_execution_memory_bytes"] = max(
                    stage_metrics[stage_id]["peak_execution_memory_bytes"], int(peak_mem)
                )
            # Spill
            mem_spill = _get(task_metrics, "Memory Bytes Spilled", "MemoryBytesSpilled")
            if mem_spill is not None:
                stage_metrics[stage_id]["memory_spilled_bytes"] += int(mem_spill)
            disk_spill = _get(task_metrics, "Disk Bytes Spilled", "DiskBytesSpilled")
            if disk_spill is not None:
                stage_metrics[stage_id]["disk_spilled_bytes"] += int(disk_spill)
            # Input/Output bytes
            input_metrics = _get(task_metrics, "Input Metrics", "InputMetrics") or {}
            output_metrics = _get(task_metrics, "Output Metrics", "OutputMetrics") or {}
            br = _get(input_metrics, "Bytes Read", "BytesRead")
            if br is not None:
                stage_metrics[stage_id]["bytes_read"] += int(br)
            bw = _get(output_metrics, "Bytes Written", "BytesWritten")
            if bw is not None:
                stage_metrics[stage_id]["bytes_written"] += int(bw)
            # Shuffle read/write (also add to bytes_read/bytes_written for backward compat)
            shuffle_read = _get(task_metrics, "Shuffle Read Metrics", "ShuffleReadMetrics") or {}
            shuffle_write = _get(task_metrics, "Shuffle Write Metrics", "ShuffleWriteMetrics") or {}
            srb = _get(shuffle_read, "Remote Bytes Read", "Total Bytes Read")
            if srb is not None:
                stage_metrics[stage_id]["bytes_read"] += int(srb)
                stage_metrics[stage_id]["shuffle_read_bytes"] += int(srb)
            swb = _get(shuffle_write, "Shuffle Bytes Written", "Bytes Written")
            if swb is not None:
                stage_metrics[stage_id]["bytes_written"] += int(swb)
                stage_metrics[stage_id]["shuffle_write_bytes"] += int(swb)

    return dict(job_stages), dict(job_times), dict(stage_metrics)


def _parse_spark_memory_to_gb(s: str | None) -> float | None:
    """Parse Spark memory string (e.g. '11g', '1024m', '1g') to GB. Returns None if invalid."""
    if not s or not isinstance(s, str):
        return None
    s = s.strip().lower()
    if not s:
        return None
    mult = 1.0
    if s.endswith("g"):
        mult = 1.0
        s = s[:-1]
    elif s.endswith("m") or s.endswith("mb"):
        mult = 1.0 / 1024.0
        s = s.rstrip("b").rstrip("m")
    elif s.endswith("k"):
        mult = 1.0 / (1024 * 1024)
        s = s[:-1]
    try:
        return round(float(s) * mult, 2)
    except ValueError:
        return None


def extract_cluster_info(events: list[dict]) -> dict:
    """
    Extract executor count and executor config from SparkListenerExecutorAdded and
    SparkListenerEnvironmentUpdate so defaults can be overridden when building the analysis request.
    Returns dict with optional: executor_count, executor_cores, executor_memory_gb.
    """
    executor_ids: set[str] = set()
    executor_cores: int | None = None
    executor_memory_gb: float | None = None
    spark_props: dict = {}

    for ev in events:
        event_type = _get(ev, "Event")
        if not event_type:
            continue
        if event_type == "SparkListenerExecutorAdded":
            eid = ev.get("Executor ID") or ev.get("ExecutorId")
            if eid is not None and str(eid).lower() != "driver":
                executor_ids.add(str(eid))
            info = ev.get("Executor Info") or ev.get("ExecutorInfo") or {}
            if info and executor_cores is None:
                executor_cores = info.get("Total Cores") or info.get("TotalCores")
                if executor_cores is not None:
                    executor_cores = int(executor_cores)
        elif event_type == "SparkListenerEnvironmentUpdate":
            sp = ev.get("Spark Properties") or ev.get("SparkProperties")
            if isinstance(sp, dict):
                spark_props.update(sp)

    # Prefer Spark properties for executor config (authoritative)
    if spark_props:
        cores_str = spark_props.get("spark.executor.cores")
        if cores_str is not None:
            try:
                executor_cores = int(cores_str)
            except (ValueError, TypeError):
                pass
        mem_str = spark_props.get("spark.executor.memory")
        if mem_str is not None:
            executor_memory_gb = _parse_spark_memory_to_gb(mem_str)

    out: dict = {}
    if executor_ids:
        out["executor_count"] = len(executor_ids)
    if executor_cores is not None and executor_cores > 0:
        out["executor_cores"] = executor_cores
    if executor_memory_gb is not None and executor_memory_gb > 0:
        out["executor_memory_gb"] = executor_memory_gb
    return out


def read_all_events(parts: list[tuple[bytes, str]]) -> list[dict]:
    """Read event lines from each (content, filename) and return one combined list. One pass over parts, single event list for parse_event_log(events=)."""
    out: list[dict] = []
    for content, name in parts:
        out.extend(_read_event_lines(content, name))
    return out


def aggregate_job_level(
    job_stages: dict[int, set[int]],
    job_times: dict[int, dict],
    stage_metrics: dict[int, dict],
    executor_hourly_cost_usd: float | None = None,
) -> list[dict]:
    """
    Aggregate per-job: duration (ms), executor_run_time_ms, bytes_read, bytes_written, estimated_cost_usd.
    """
    jobs = []
    for job_id, stage_ids in job_stages.items():
        duration_ms = None
        times = job_times.get(job_id, {})
        start = times.get("start_time_ms")
        end = times.get("end_time_ms")
        if start is not None and end is not None:
            duration_ms = end - start

        executor_run_time_ms = 0
        bytes_read = 0
        bytes_written = 0
        peak_execution_memory_bytes = 0
        executor_cpu_time_ns = 0
        memory_spilled_bytes = 0
        disk_spilled_bytes = 0
        shuffle_read_bytes = 0
        shuffle_write_bytes = 0
        for sid in stage_ids:
            sm = stage_metrics.get(sid, {})
            executor_run_time_ms += sm.get("executor_run_time_ms", 0)
            bytes_read += sm.get("bytes_read", 0)
            bytes_written += sm.get("bytes_written", 0)
            peak_execution_memory_bytes = max(
                peak_execution_memory_bytes, sm.get("peak_execution_memory_bytes", 0)
            )
            executor_cpu_time_ns += sm.get("executor_cpu_time_ns", 0)
            memory_spilled_bytes += sm.get("memory_spilled_bytes", 0)
            disk_spilled_bytes += sm.get("disk_spilled_bytes", 0)
            shuffle_read_bytes += sm.get("shuffle_read_bytes", 0)
            shuffle_write_bytes += sm.get("shuffle_write_bytes", 0)

        executor_hours = executor_run_time_ms / (1000.0 * 3600.0)
        estimated_cost_usd = None
        if executor_hourly_cost_usd is not None and executor_hourly_cost_usd >= 0:
            estimated_cost_usd = round(executor_hours * executor_hourly_cost_usd, 4)

        duration_sec = (round((end - start) / 1000.0, 2) if start is not None and end is not None else
                       round(executor_run_time_ms / 1000.0, 2))

        job_out = {
            "job_id": job_id,
            "duration_ms": duration_ms,
            "duration_sec": duration_sec,
            "executor_run_time_ms": executor_run_time_ms,
            "executor_hours": round(executor_hours, 6),
            "bytes_read": bytes_read,
            "bytes_written": bytes_written,
            "estimated_cost_usd": estimated_cost_usd,
            "result": times.get("result", "Unknown"),
        }
        if peak_execution_memory_bytes > 0:
            job_out["peak_execution_memory_bytes"] = peak_execution_memory_bytes
        if executor_cpu_time_ns > 0:
            job_out["executor_cpu_time_ms"] = round(executor_cpu_time_ns / 1e6, 2)
        if memory_spilled_bytes > 0:
            job_out["memory_spilled_bytes"] = memory_spilled_bytes
        if disk_spilled_bytes > 0:
            job_out["disk_spilled_bytes"] = disk_spilled_bytes
        if shuffle_read_bytes > 0:
            job_out["shuffle_read_bytes"] = shuffle_read_bytes
        if shuffle_write_bytes > 0:
            job_out["shuffle_write_bytes"] = shuffle_write_bytes
        jobs.append(job_out)
    jobs.sort(key=lambda x: x["job_id"])
    return jobs

"""
Spark event log ingestion: parse JSON/JSON.gz event logs and aggregate per-job metrics.
Supports SparkListenerJobStart, SparkListenerJobEnd, SparkListenerTaskEnd.
"""
import gzip
import json
from collections import defaultdict
from io import BytesIO
from typing import BinaryIO


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


def parse_event_log(content: bytes, filename: str = "") -> tuple[dict[int, set[int]], dict[int, dict], list[dict]]:
    """
    Parse event log and return:
    - job_stages: job_id -> set of stage_ids
    - job_times: job_id -> {start_time_ms?, end_time_ms?, result?}
    - stage_metrics: stage_id -> {executor_run_time_ms, bytes_read, bytes_written, task_count}
    """
    events = _read_event_lines(content, filename)
    job_stages: dict[int, set[int]] = defaultdict(set)
    job_times: dict[int, dict] = defaultdict(dict)
    stage_metrics: dict[int, dict] = defaultdict(lambda: {"executor_run_time_ms": 0, "bytes_read": 0, "bytes_written": 0, "task_count": 0})

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
            # Input/Output bytes
            input_metrics = _get(task_metrics, "Input Metrics", "InputMetrics") or {}
            output_metrics = _get(task_metrics, "Output Metrics", "OutputMetrics") or {}
            br = _get(input_metrics, "Bytes Read", "BytesRead")
            if br is not None:
                stage_metrics[stage_id]["bytes_read"] += int(br)
            bw = _get(output_metrics, "Bytes Written", "BytesWritten")
            if bw is not None:
                stage_metrics[stage_id]["bytes_written"] += int(bw)
            # Shuffle read/write
            shuffle_read = _get(task_metrics, "Shuffle Read Metrics", "ShuffleReadMetrics") or {}
            shuffle_write = _get(task_metrics, "Shuffle Write Metrics", "ShuffleWriteMetrics") or {}
            srb = _get(shuffle_read, "Remote Bytes Read", "Total Bytes Read")
            if srb is not None:
                stage_metrics[stage_id]["bytes_read"] += int(srb)
            swb = _get(shuffle_write, "Shuffle Bytes Written", "Bytes Written")
            if swb is not None:
                stage_metrics[stage_id]["bytes_written"] += int(swb)

    return dict(job_stages), dict(job_times), dict(stage_metrics)


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
        for sid in stage_ids:
            sm = stage_metrics.get(sid, {})
            executor_run_time_ms += sm.get("executor_run_time_ms", 0)
            bytes_read += sm.get("bytes_read", 0)
            bytes_written += sm.get("bytes_written", 0)

        executor_hours = executor_run_time_ms / (1000.0 * 3600.0)
        estimated_cost_usd = None
        if executor_hourly_cost_usd is not None and executor_hourly_cost_usd >= 0:
            estimated_cost_usd = round(executor_hours * executor_hourly_cost_usd, 4)

        duration_sec = (round((end - start) / 1000.0, 2) if start is not None and end is not None else
                       round(executor_run_time_ms / 1000.0, 2))

        jobs.append({
            "job_id": job_id,
            "duration_ms": duration_ms,
            "duration_sec": duration_sec,
            "executor_run_time_ms": executor_run_time_ms,
            "executor_hours": round(executor_hours, 6),
            "bytes_read": bytes_read,
            "bytes_written": bytes_written,
            "estimated_cost_usd": estimated_cost_usd,
            "result": times.get("result", "Unknown"),
        })
    jobs.sort(key=lambda x: x["job_id"])
    return jobs

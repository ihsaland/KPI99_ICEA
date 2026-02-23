"""Input/output types for ICEA MVP."""
from typing import Literal, Optional
from pydantic import BaseModel, Field


class NodeConfig(BaseModel):
    """Node (worker) configuration."""
    cores: int = Field(..., ge=1, le=256, description="vCPUs per node")
    memory_gb: float = Field(..., ge=1, le=1024, description="Memory GB per node")
    hourly_cost_usd: float = Field(..., ge=0, description="USD per hour per node")
    count: int = Field(..., ge=1, le=10000, description="Number of nodes in cluster")


class ExecutorConfig(BaseModel):
    """Executor configuration."""
    cores: int = Field(..., ge=1, le=32, description="Cores per executor")
    memory_gb: float = Field(..., ge=1, le=128, description="Memory GB per executor")


class WorkloadConfig(BaseModel):
    """Workload timing and frequency."""
    avg_runtime_minutes: float = Field(..., ge=0.1, le=1440, description="Average job runtime in minutes")
    jobs_per_day: float = Field(..., ge=0.1, le=100000, description="Job runs per day")
    min_runtime_minutes: Optional[float] = Field(None, ge=0, le=1440, description="Min job runtime (for range)")
    max_runtime_minutes: Optional[float] = Field(None, ge=0, le=1440, description="Max job runtime (for range)")
    partition_count: Optional[int] = Field(
        None, ge=1, le=1_000_000,
        description="Typical number of partitions (e.g. Spark RDD/DataFrame). Used for parallelism and risk notes."
    )
    input_data_gb: Optional[float] = Field(
        None, ge=0.01, le=1_000_000,
        description="Typical input data size per job (GB). Used for spill/shuffle risk notes."
    )
    concurrent_jobs: Optional[float] = Field(
        None, ge=0.1, le=10_000,
        description="Typical or max concurrent jobs sharing the cluster. Refines utilization and cost notes."
    )
    peak_executor_memory_gb: Optional[float] = Field(
        None, ge=0.5, le=128,
        description="Observed peak executor memory (GB) from Spark UI. Used to flag OOM/spill risk vs configured memory."
    )
    shuffle_read_mb: Optional[float] = Field(
        None, ge=0, le=10_000_000,
        description="Typical shuffle read per job (MB). Used for shuffle/memory risk notes."
    )
    shuffle_write_mb: Optional[float] = Field(
        None, ge=0, le=10_000_000,
        description="Typical shuffle write per job (MB). Used for shuffle/memory risk notes."
    )
    data_skew: Optional[Literal["low", "medium", "high"]] = Field(
        None,
        description="Data skew level (low/medium/high). High skew can increase tail latency and underutilization."
    )
    spot_pct: Optional[float] = Field(
        None, ge=0, le=100,
        description="Share of capacity on spot/preemptible (0–100%). Affects cost and reliability notes."
    )
    autoscale_min_nodes: Optional[int] = Field(None, ge=1, le=10000, description="Min nodes when autoscaling.")
    autoscale_max_nodes: Optional[int] = Field(None, ge=1, le=10000, description="Max nodes when autoscaling.")


class Assumptions(BaseModel):
    """Optional overhead reserves (daemon/system)."""
    reserve_cores: int = Field(0, ge=0, le=16)
    reserve_memory_gb: float = Field(0, ge=0, le=32)


class AnalyzeRequest(BaseModel):
    """Request body for /v1/analyze."""
    cloud: str = Field("aws", description="Cloud provider: aws, azure, gcp, oci, alibaba, ibm, digitalocean, linode, databricks, emr, synapse, dataproc, on-prem")
    node: NodeConfig
    executor: ExecutorConfig
    workload: WorkloadConfig
    assumptions: Optional[Assumptions] = Field(default_factory=Assumptions)
    # Optional granularity and forecasting
    region: Optional[str] = Field(None, max_length=64, description="e.g. us-east-1, westeurope")
    instance_type: Optional[str] = Field(None, max_length=64, description="e.g. r6g.4xlarge")
    utilization_factor: Optional[float] = Field(None, ge=0.01, le=1.0, description="Shared cluster: effective usage 0–1")
    forecast_months: Optional[int] = Field(None, ge=1, le=36, description="Project cost/savings over N months")
    growth_rate_pct: Optional[float] = Field(None, ge=-50, le=500, description="Annual growth % for jobs or runtime")


class PackingResult(BaseModel):
    """Result of executor packing calculation."""
    executors_per_node: int
    cpu_utilization: float
    mem_utilization: float
    cpu_waste: float
    mem_waste: float
    waste: float  # max(cpu_waste, mem_waste)
    efficiency_score: int  # 0-100


class CostResult(BaseModel):
    """Cost model outputs."""
    hourly_cluster_cost_usd: float
    daily_cost_usd: float
    waste_cost_daily_usd: float
    waste_cost_monthly_usd: float


class RecommendedConfig(BaseModel):
    """Recommended executor configuration."""
    executor_cores: int
    executor_memory_gb: float
    executors_per_node: int
    efficiency_score: int
    waste: float
    waste_cost_monthly_usd: float
    savings_vs_current_monthly_usd: float


class AnalyzeResponse(BaseModel):
    """Response from /v1/analyze."""
    packing: PackingResult
    cost: CostResult
    recommendation: Optional[RecommendedConfig] = None
    risk_notes: list[str] = Field(default_factory=list)


class CheckoutTier1Request(BaseModel):
    """Request for Tier 1 checkout: config + optional URLs (base for success, no token)."""
    request: AnalyzeRequest
    success_url_base: Optional[str] = None  # e.g. https://icea.example.com/report-success.html
    cancel_url: Optional[str] = None
    amount_cents: Optional[int] = Field(29900, ge=29900, le=29900)  # $299 fixed


class ExpertRequest(BaseModel):
    """Contact/request for Tier 2 or Tier 3."""
    name: str = Field(..., min_length=1, max_length=200)
    email: str = Field(..., min_length=1, max_length=200)
    company: Optional[str] = Field(None, max_length=200)
    message: Optional[str] = Field(None, max_length=2000)
    config: Optional[AnalyzeRequest] = None
    tier: str = Field(..., pattern="^(2|3)$")  # "2" or "3"


class JobLevelSummary(BaseModel):
    """Per-job metrics from event log ingestion."""
    job_id: int
    duration_sec: float
    executor_run_time_ms: float
    executor_hours: float
    bytes_read: int = 0
    bytes_written: int = 0
    estimated_cost_usd: Optional[float] = None
    result: str = "Unknown"


class JobReportRequest(BaseModel):
    """Request body for POST /v1/report/jobs."""
    jobs: list[JobLevelSummary]
    executor_hourly_cost_usd: Optional[float] = None
    source_filename: Optional[str] = None

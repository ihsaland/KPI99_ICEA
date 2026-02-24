(function () {
  const form = document.getElementById("icea-form");
  const previewSection = document.getElementById("preview");
  const previewScore = document.getElementById("preview-score");
  const previewWaste = document.getElementById("preview-waste");
  const previewExecutors = document.getElementById("preview-executors");
  const previewSavings = document.getElementById("preview-savings");
  const previewSavingsWrap = document.getElementById("preview-savings-wrap");
  const previewRisks = document.getElementById("preview-risks");

  var catalogInstances = [];

  function getPayload() {
    var v = function (id) { return document.getElementById(id); };
    var num = function (id, def) { var n = parseFloat(v(id) && v(id).value); return (n !== undefined && !isNaN(n)) ? n : def; };
    var str = function (id) { var s = v(id) && v(id).value && v(id).value.trim(); return s || null; };
    var payload = {
      cloud: (v("cloud") && v("cloud").value) || "aws",
      node: {
        cores: parseInt(v("node_cores") && v("node_cores").value, 10) || 0,
        memory_gb: parseFloat(v("node_memory_gb") && v("node_memory_gb").value) || 0,
        hourly_cost_usd: parseFloat(v("node_hourly_cost_usd") && v("node_hourly_cost_usd").value) || 0,
        count: parseInt(v("node_count") && v("node_count").value, 10) || 0,
      },
      executor: {
        cores: parseInt(v("executor_cores") && v("executor_cores").value, 10) || 0,
        memory_gb: parseFloat(v("executor_memory_gb") && v("executor_memory_gb").value) || 0,
      },
      workload: {
        avg_runtime_minutes: parseFloat(v("avg_runtime_minutes") && v("avg_runtime_minutes").value) || 0,
        jobs_per_day: parseFloat(v("jobs_per_day") && v("jobs_per_day").value) || 0,
        min_runtime_minutes: num("min_runtime_minutes", null),
        max_runtime_minutes: num("max_runtime_minutes", null),
        partition_count: (function () {
          var n = parseInt(v("partition_count") && v("partition_count").value, 10);
          return (n > 0 && !isNaN(n)) ? n : null;
        })(),
        input_data_gb: (function () {
          var n = parseFloat(v("input_data_gb") && v("input_data_gb").value);
          return (n > 0 && !isNaN(n)) ? n : null;
        })(),
        concurrent_jobs: (function () {
          var n = parseFloat(v("concurrent_jobs") && v("concurrent_jobs").value);
          return (n > 0 && !isNaN(n)) ? n : null;
        })(),
        peak_executor_memory_gb: (function () {
          var n = parseFloat(v("peak_executor_memory_gb") && v("peak_executor_memory_gb").value);
          return (n > 0 && !isNaN(n)) ? n : null;
        })(),
        shuffle_read_mb: (function () {
          var n = parseFloat(v("shuffle_read_mb") && v("shuffle_read_mb").value);
          return (n >= 0 && !isNaN(n)) ? n : null;
        })(),
        shuffle_write_mb: (function () {
          var n = parseFloat(v("shuffle_write_mb") && v("shuffle_write_mb").value);
          return (n >= 0 && !isNaN(n)) ? n : null;
        })(),
        data_skew: (function () {
          var s = str("data_skew");
          return (s === "low" || s === "medium" || s === "high") ? s : null;
        })(),
        spot_pct: (function () {
          var n = parseFloat(v("spot_pct") && v("spot_pct").value);
          return (n >= 0 && n <= 100 && !isNaN(n)) ? n : null;
        })(),
        autoscale_min_nodes: (function () {
          var n = parseInt(v("autoscale_min_nodes") && v("autoscale_min_nodes").value, 10);
          return (n > 0 && !isNaN(n)) ? n : null;
        })(),
        autoscale_max_nodes: (function () {
          var n = parseInt(v("autoscale_max_nodes") && v("autoscale_max_nodes").value, 10);
          return (n > 0 && !isNaN(n)) ? n : null;
        })(),
      },
      assumptions: {
        reserve_cores: parseInt(v("reserve_cores") && v("reserve_cores").value, 10) || 0,
        reserve_memory_gb: parseFloat(v("reserve_memory_gb") && v("reserve_memory_gb").value) || 0,
      },
    };
    if (str("region")) payload.region = str("region");
    if (str("instance_type")) payload.instance_type = str("instance_type");
    var uf = num("utilization_factor", null);
    if (uf !== null && uf > 0 && uf <= 1) payload.utilization_factor = uf;
    var fm = parseInt(v("forecast_months") && v("forecast_months").value, 10);
    if (fm > 0) payload.forecast_months = fm;
    var gr = num("growth_rate_pct", null);
    if (gr !== null && !isNaN(gr)) payload.growth_rate_pct = gr;
    if (payload.workload.min_runtime_minutes == null) delete payload.workload.min_runtime_minutes;
    if (payload.workload.max_runtime_minutes == null) delete payload.workload.max_runtime_minutes;
    if (payload.workload.partition_count == null) delete payload.workload.partition_count;
    if (payload.workload.input_data_gb == null) delete payload.workload.input_data_gb;
    if (payload.workload.concurrent_jobs == null) delete payload.workload.concurrent_jobs;
    if (payload.workload.peak_executor_memory_gb == null) delete payload.workload.peak_executor_memory_gb;
    if (payload.workload.shuffle_read_mb == null) delete payload.workload.shuffle_read_mb;
    if (payload.workload.shuffle_write_mb == null) delete payload.workload.shuffle_write_mb;
    if (payload.workload.data_skew == null) delete payload.workload.data_skew;
    if (payload.workload.spot_pct == null) delete payload.workload.spot_pct;
    if (payload.workload.autoscale_min_nodes == null) delete payload.workload.autoscale_min_nodes;
    if (payload.workload.autoscale_max_nodes == null) delete payload.workload.autoscale_max_nodes;
    return payload;
  }

  function clearRowErrors() {
    setFormError("");
    if (form) form.querySelectorAll(".row").forEach(function (row) { row.classList.remove("invalid"); });
  }

  function setFormError(message) {
    var el = document.getElementById("form-error");
    if (!el) return;
    el.textContent = message || "";
    el.classList.toggle("hidden", !message);
  }

  function showPreview(data) {
    if (!previewSection) return;
    previewSection.classList.remove("hidden");
    if (previewScore) previewScore.textContent = data.packing.efficiency_score + "/100";
    if (previewWaste) previewWaste.textContent = "$" + Number(data.cost.waste_cost_monthly_usd).toLocaleString("en-US", { minimumFractionDigits: 2 });
    if (previewExecutors) previewExecutors.textContent = data.packing.executors_per_node;
    if (previewSavingsWrap && previewSavings) {
      if (data.recommendation && data.recommendation.savings_vs_current_monthly_usd > 0) {
        previewSavingsWrap.style.display = "";
        previewSavings.textContent = "$" + Number(data.recommendation.savings_vs_current_monthly_usd).toLocaleString("en-US", { minimumFractionDigits: 2 }) + "/mo";
      } else {
        previewSavingsWrap.style.display = "none";
      }
    }
    if (previewRisks) {
      if (data.risk_notes && data.risk_notes.length > 0) {
        previewRisks.classList.remove("hidden");
        previewRisks.innerHTML = "<ul><li>" + data.risk_notes.join("</li><li>") + "</li></ul>";
      } else {
        previewRisks.classList.add("hidden");
        previewRisks.innerHTML = "";
      }
    }
  }

  function analyze() {
    clearRowErrors();
    var payload = getPayload();
    fetch("/v1/analyze", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    })
      .then(function (res) {
        if (!res.ok) {
          return res.json().then(function (j) {
            var msg = res.statusText;
            if (j && j.detail) msg = typeof j.detail === "string" ? j.detail : (j.detail[0] && j.detail[0].msg) || msg;
            var e = new Error(msg);
            e.detail = j;
            throw e;
          });
        }
        return res.json();
      })
      .then(showPreview)
      .catch(function (err) {
        var msg = err.message || "Request failed";
        if (err.detail && typeof err.detail === "string") msg = err.detail;
        setFormError(msg);
      })
      .finally(function () {});
  }

  // Tier 1: checkout then redirect to Stripe
  function startTier1Checkout() {
    clearRowErrors();
    var payload = getPayload();
    var successBase = window.location.origin + "/report-success.html";
    var body = {
      request: payload,
      success_url_base: successBase,
      cancel_url: window.location.origin + "/",
      amount_cents: 29900,
    };
    var btn = document.querySelector('.btn-tier[data-tier="1"]');
    if (btn) btn.disabled = true;
    fetch("/v1/checkout/tier1", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    })
      .then(function (res) {
        return res.json().then(function (j) {
          if (!res.ok) throw new Error(j.detail || res.statusText);
          return j;
        });
      })
      .then(function (data) {
        if (data.checkout_url) window.location.href = data.checkout_url;
        else setFormError("No checkout URL returned.");
      })
      .catch(function (err) {
        setFormError(err.message || "Checkout failed.");
      })
      .finally(function () {
        if (btn) btn.disabled = false;
      });
  }

  // Tier 2/3: open modal, submit to request-expert
  var expertModal = document.getElementById("expert-modal");
  var expertForm = document.getElementById("expert-form");
  var expertTier = document.getElementById("expert-tier");
  var expertModalTitle = document.getElementById("expert-modal-title");

  function openExpertModal(tier) {
    expertTier.value = tier;
    expertModalTitle.textContent = tier === "3" ? "Request Enterprise Analysis" : "Request Expert Analysis";
    if (expertModal) expertModal.classList.remove("hidden");
  }

  function closeExpertModal() {
    if (expertModal) expertModal.classList.add("hidden");
  }

  if (document.getElementById("expert-modal-cancel")) {
    document.getElementById("expert-modal-cancel").addEventListener("click", closeExpertModal);
  }
  if (expertModal && expertModal.querySelector(".modal-backdrop")) {
    expertModal.querySelector(".modal-backdrop").addEventListener("click", closeExpertModal);
  }

  expertForm.addEventListener("submit", function (e) {
    e.preventDefault();
    var tier = expertTier.value;
    var payload = {
      name: document.getElementById("expert-name").value.trim(),
      email: document.getElementById("expert-email").value.trim(),
      company: document.getElementById("expert-company").value.trim() || null,
      message: document.getElementById("expert-message").value.trim() || null,
      config: getPayload(),
      tier: tier,
    };
    var submitBtn = expertForm.querySelector('button[type="submit"]');
    if (submitBtn) submitBtn.disabled = true;
    fetch("/v1/request-expert", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    })
      .then(function (res) {
        return res.json().then(function (j) {
          if (!res.ok) throw new Error(j.detail || res.statusText);
          return j;
        });
      })
      .then(function (data) {
        closeExpertModal();
        alert(data.message || "Request received. We will contact you shortly.");
        expertForm.reset();
      })
      .catch(function (err) {
        alert(err.message || "Request failed.");
      })
      .finally(function () {
        if (submitBtn) submitBtn.disabled = false;
      });
  });

  // Tier buttons
  document.querySelectorAll(".btn-tier[data-tier='1']").forEach(function (btn) {
    btn.addEventListener("click", startTier1Checkout);
  });
  document.querySelectorAll(".btn-tier[data-tier='2']").forEach(function (btn) {
    btn.addEventListener("click", function () { openExpertModal("2"); });
  });
  document.querySelectorAll(".btn-tier[data-tier='3']").forEach(function (btn) {
    btn.addEventListener("click", function () { openExpertModal("3"); });
  });

  // View report (HTML): open KPI99 report in new tab with optional PDF download
  var btnViewReport = document.getElementById("btn-view-report");
  if (btnViewReport) {
    btnViewReport.addEventListener("click", function () {
      setFormError("");
      var payload = getPayload();
      btnViewReport.disabled = true;
      fetch("/v1/report/html", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      })
        .then(function (res) {
          if (!res.ok) return res.text().then(function (t) { throw new Error(t || res.statusText); });
          return res.text();
        })
        .then(function (html) {
          var w = window.open("", "_blank");
          if (w) {
            w.document.write(html);
            w.document.close();
          } else {
            setFormError("Allow pop-ups to view the report, or use Download PDF.");
          }
        })
        .catch(function (err) {
          setFormError(err.message || "View report failed");
        })
        .finally(function () {
          btnViewReport.disabled = false;
        });
    });
  }

  // Demo report (PDF download)
  var btnDemoReport = document.getElementById("btn-demo-report");
  if (btnDemoReport) {
    btnDemoReport.addEventListener("click", function () {
      setFormError("");
      var payload = getPayload();
      btnDemoReport.disabled = true;
      fetch("/v1/report", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      })
        .then(function (res) {
          if (!res.ok) return res.text().then(function (t) { throw new Error(t || res.statusText); });
          return res.blob();
        })
        .then(function (blob) {
          var a = document.createElement("a");
          a.href = URL.createObjectURL(blob);
          a.download = "icea-report.pdf";
          a.click();
          URL.revokeObjectURL(a.href);
        })
        .catch(function (err) {
          setFormError(err.message || "Download failed");
        })
        .finally(function () {
          btnDemoReport.disabled = false;
        });
    });
  }

  // Demo option: visible only when backend allows demo (not in production)
  var demoCard = document.getElementById("demo-tier-card");
  fetch("/v1/health")
    .then(function (r) { return r.json(); })
    .then(function (data) {
      if (demoCard) {
        if (data.demo_available) {
          demoCard.classList.remove("hidden");
        } else {
          demoCard.classList.add("hidden");
        }
      }
    })
    .catch(function () {
      if (demoCard) demoCard.classList.add("hidden");
    });

  // Event log ingest and job-level PDF
  var lastEventlogResult = null;
  var eventlogForm = document.getElementById("eventlog-form");
  var eventlogResult = document.getElementById("eventlog-result");
  var eventlogError = document.getElementById("eventlog-error");
  if (eventlogForm) {
    eventlogForm.addEventListener("submit", function (e) {
      e.preventDefault();
      var fileInput = document.getElementById("eventlog-file");
      if (!fileInput || !fileInput.files || !fileInput.files[0]) {
        if (eventlogError) { eventlogError.textContent = "Select a file."; eventlogError.classList.remove("hidden"); }
        return;
      }
      var fd = new FormData();
      fd.append("file", fileInput.files[0]);
      var hourly = document.getElementById("eventlog-executor-hourly");
      if (hourly && hourly.value) fd.append("executor_hourly_cost_usd", hourly.value);
      var btn = document.getElementById("eventlog-ingest-btn");
      if (btn) btn.disabled = true;
      if (eventlogError) { eventlogError.textContent = ""; eventlogError.classList.add("hidden"); }
      fetch("/v1/ingest/eventlog", { method: "POST", body: fd })
        .then(function (res) {
          if (!res.ok) return res.json().then(function (j) { throw new Error(j.detail || res.statusText); });
          return res.json();
        })
        .then(function (data) {
          lastEventlogResult = data;
          if (eventlogResult) {
            document.getElementById("eventlog-total-jobs").textContent = data.total_jobs || 0;
            document.getElementById("eventlog-total-hrs").textContent = (data.total_executor_hours != null) ? data.total_executor_hours : "0";
            document.getElementById("eventlog-total-cost").textContent = (data.total_estimated_cost_usd != null) ? "$" + Number(data.total_estimated_cost_usd).toFixed(2) : "—";
            eventlogResult.classList.remove("hidden");
          }
        })
        .catch(function (err) {
          if (eventlogError) { eventlogError.textContent = err.message || "Ingest failed"; eventlogError.classList.remove("hidden"); }
        })
        .finally(function () { if (btn) btn.disabled = false; });
    });
  }
  var eventlogUseSample = document.getElementById("eventlog-use-sample");
  if (eventlogUseSample) {
    eventlogUseSample.addEventListener("click", function () {
      var btn = eventlogUseSample;
      var hourlyInput = document.getElementById("eventlog-executor-hourly");
      if (btn) btn.disabled = true;
      if (eventlogError) { eventlogError.textContent = ""; eventlogError.classList.add("hidden"); }
      fetch("/v1/sample-eventlog")
        .then(function (res) { if (!res.ok) throw new Error("Sample not found"); return res.blob(); })
        .then(function (blob) {
          var file = new File([blob], "sample-eventlog.json", { type: "application/json" });
          var fd = new FormData();
          fd.append("file", file);
          if (hourlyInput && hourlyInput.value) fd.append("executor_hourly_cost_usd", hourlyInput.value);
          return fetch("/v1/ingest/eventlog", { method: "POST", body: fd });
        })
        .then(function (res) {
          if (!res.ok) return res.json().then(function (j) { throw new Error(j.detail || res.statusText); });
          return res.json();
        })
        .then(function (data) {
          lastEventlogResult = data;
          if (eventlogResult) {
            document.getElementById("eventlog-total-jobs").textContent = data.total_jobs || 0;
            document.getElementById("eventlog-total-hrs").textContent = (data.total_executor_hours != null) ? data.total_executor_hours : "0";
            document.getElementById("eventlog-total-cost").textContent = (data.total_estimated_cost_usd != null) ? "$" + Number(data.total_estimated_cost_usd).toFixed(2) : "—";
            eventlogResult.classList.remove("hidden");
          }
        })
        .catch(function (err) {
          if (eventlogError) { eventlogError.textContent = err.message || "Sample ingest failed"; eventlogError.classList.remove("hidden"); }
        })
        .finally(function () { if (btn) btn.disabled = false; });
    });
  }
  var eventlogPdfBtn = document.getElementById("eventlog-download-pdf");
  if (eventlogPdfBtn) {
    eventlogPdfBtn.addEventListener("click", function () {
      if (!lastEventlogResult || !lastEventlogResult.jobs || !lastEventlogResult.jobs.length) return;
      var hourlyInput = document.getElementById("eventlog-executor-hourly");
      var executorHourly = (hourlyInput && hourlyInput.value) ? parseFloat(hourlyInput.value) : null;
      fetch("/v1/report/jobs", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          jobs: lastEventlogResult.jobs,
          executor_hourly_cost_usd: executorHourly,
          source_filename: lastEventlogResult.source_filename || "",
        }),
      })
        .then(function (res) {
          if (!res.ok) return res.text().then(function (t) { throw new Error(t || res.statusText); });
          return res.blob();
        })
        .then(function (blob) {
          var a = document.createElement("a");
          a.href = URL.createObjectURL(blob);
          a.download = "icea-job-report.pdf";
          a.click();
          URL.revokeObjectURL(a.href);
        })
        .catch(function (err) { if (eventlogError) { eventlogError.textContent = err.message || "PDF failed"; eventlogError.classList.remove("hidden"); } });
    });
  }

  var API_BASE = "";

  var CLOUD_FALLBACK_HTML = "<option value=\"aws\">AWS</option><option value=\"azure\">Azure</option><option value=\"gcp\">GCP</option><option value=\"oci\">Oracle Cloud</option><option value=\"alibaba\">Alibaba</option><option value=\"ibm\">IBM</option><option value=\"digitalocean\">DigitalOcean</option><option value=\"linode\">Linode</option><option value=\"databricks\">Databricks</option><option value=\"emr\">AWS EMR</option><option value=\"synapse\">Azure Synapse</option><option value=\"dataproc\">Google Dataproc</option><option value=\"on-prem\">On-prem</option>";

  var FALLBACK_REGIONS = { aws: [{ id: "us-east-1", name: "US East (N. Virginia)" }, { id: "us-west-2", name: "US West (Oregon)" }, { id: "eu-west-1", name: "EU (Ireland)" }], azure: [{ id: "eastus", name: "East US" }, { id: "westeurope", name: "West Europe" }], gcp: [{ id: "us-central1", name: "us-central1 (Iowa)" }, { id: "europe-west1", name: "europe-west1 (Belgium)" }], emr: [{ id: "us-east-1", name: "US East" }, { id: "us-west-2", name: "US West" }], synapse: [{ id: "eastus", name: "East US" }, { id: "westeurope", name: "West Europe" }], dataproc: [{ id: "us-central1", name: "us-central1" }, { id: "europe-west1", name: "europe-west1" }] };
  var FALLBACK_INSTANCES = { aws: [{ id: "r6g.4xlarge", name: "r6g.4xlarge", cores: 16, memory_gb: 128, hourly_usd: 0.8064 }, { id: "m6i.4xlarge", name: "m6i.4xlarge", cores: 16, memory_gb: 64, hourly_usd: 0.768 }], azure: [{ id: "Standard_D16s_v3", name: "Standard D16s v3", cores: 16, memory_gb: 64, hourly_usd: 0.768 }, { id: "Standard_E16s_v3", name: "Standard E16s v3", cores: 16, memory_gb: 128, hourly_usd: 1.008 }], gcp: [{ id: "n2-standard-16", name: "n2-standard-16", cores: 16, memory_gb: 64, hourly_usd: 0.7769 }, { id: "n2-highmem-16", name: "n2-highmem-16", cores: 16, memory_gb: 128, hourly_usd: 1.0481 }], emr: [{ id: "m6i.4xlarge", name: "m6i.4xlarge", cores: 16, memory_gb: 64, hourly_usd: 1.008 }, { id: "r6g.4xlarge", name: "r6g.4xlarge", cores: 16, memory_gb: 128, hourly_usd: 1.128 }], synapse: [{ id: "Large", name: "Large (16 vCores)", cores: 16, memory_gb: 128, hourly_usd: 1.52 }], dataproc: [{ id: "n2-standard-16", name: "n2-standard-16", cores: 16, memory_gb: 64, hourly_usd: 0.777 }], "on-prem": [{ id: "custom", name: "Custom (enter specs below)", cores: 16, memory_gb: 64, hourly_usd: 0 }] };

  function setCloudFallbackAndLoadFirst() {
    var cloud = document.getElementById("cloud");
    if (cloud) {
      cloud.innerHTML = CLOUD_FALLBACK_HTML;
      cloud.value = "aws";
      loadRegions("aws");
      loadInstances("aws", null);
    }
  }

  function loadProviders() {
    var cloud = document.getElementById("cloud");
    if (!cloud) return;
    fetch(API_BASE + "/v1/catalog/providers")
      .then(function (r) {
        if (!r.ok) return null;
        return r.json();
      })
      .then(function (list) {
        if (!Array.isArray(list) || list.length === 0) {
          setCloudFallbackAndLoadFirst();
          return;
        }
        cloud.innerHTML = "";
        list.forEach(function (p) {
          var opt = document.createElement("option");
          opt.value = p.id;
          opt.textContent = p.name;
          cloud.appendChild(opt);
        });
        cloud.value = list[0].id;
        loadRegions(list[0].id);
        loadInstances(list[0].id, null);
      })
      .catch(function () {
        setCloudFallbackAndLoadFirst();
      });
  }

  function applyRegionsToList(regionEl, list) {
    if (!Array.isArray(list) || list.length === 0) return;
    list.forEach(function (r) {
      var opt = document.createElement("option");
      opt.value = r.id;
      opt.textContent = r.name;
      regionEl.appendChild(opt);
    });
  }

  function applyInstancesToList(instSelect, list) {
    if (!Array.isArray(list) || list.length === 0) return;
    catalogInstances = list;
    list.forEach(function (i) {
      var opt = document.createElement("option");
      opt.value = i.id;
      opt.textContent = i.name + " — " + i.cores + " vCPU, " + i.memory_gb + " GB — $" + i.hourly_usd + "/hr";
      instSelect.appendChild(opt);
    });
  }

  function loadRegions(cloud) {
    var regionEl = document.getElementById("region");
    if (!regionEl) return;
    regionEl.innerHTML = "<option value=\"\">— Select region —</option>";
    if (!cloud) return;
    fetch(API_BASE + "/v1/catalog/regions?cloud=" + encodeURIComponent(cloud))
      .then(function (r) {
        if (!r.ok) return null;
        return r.json();
      })
      .then(function (list) {
        if (Array.isArray(list) && list.length > 0) {
          applyRegionsToList(regionEl, list);
          return;
        }
        var fallback = FALLBACK_REGIONS[cloud];
        if (fallback) applyRegionsToList(regionEl, fallback);
      })
      .catch(function () {
        var fallback = FALLBACK_REGIONS[cloud];
        if (fallback) applyRegionsToList(regionEl, fallback);
      });
  }

  function loadInstances(cloud, region) {
    var instSelect = document.getElementById("instance_type");
    if (!instSelect) return;
    instSelect.innerHTML = "<option value=\"\">— Select instance type —</option>";
    catalogInstances = [];
    if (!cloud) return;
    var url = API_BASE + "/v1/catalog/instances?cloud=" + encodeURIComponent(cloud);
    if (region) url += "&region=" + encodeURIComponent(region);
    fetch(url)
      .then(function (r) {
        if (!r.ok) return null;
        return r.json();
      })
      .then(function (list) {
        if (Array.isArray(list) && list.length > 0) {
          applyInstancesToList(instSelect, list);
          return;
        }
        var fallback = FALLBACK_INSTANCES[cloud];
        if (fallback) applyInstancesToList(instSelect, fallback);
      })
      .catch(function () {
        var fallback = FALLBACK_INSTANCES[cloud];
        if (fallback) applyInstancesToList(instSelect, fallback);
      });
  }

  function onInstanceTypeChange() {
    var instSelect = document.getElementById("instance_type");
    var id = instSelect && instSelect.value;
    if (!id) return;
    var inst = catalogInstances.find(function (i) { return i.id === id; });
    if (!inst) return;
    var cores = document.getElementById("node_cores");
    var mem = document.getElementById("node_memory_gb");
    var cost = document.getElementById("node_hourly_cost_usd");
    if (cores) cores.value = inst.cores;
    if (mem) mem.value = inst.memory_gb;
    if (cost) cost.value = inst.hourly_usd;
    analyze();
  }

  if (document.getElementById("cloud")) {
    document.getElementById("cloud").addEventListener("change", function () {
      var cloud = this.value;
      loadRegions(cloud);
      loadInstances(cloud, null);
    });
  }
  if (document.getElementById("region")) {
    document.getElementById("region").addEventListener("change", function () {
      var cloud = document.getElementById("cloud") && document.getElementById("cloud").value;
      var region = this.value || null;
      loadInstances(cloud, region);
    });
  }
  if (document.getElementById("instance_type")) {
    document.getElementById("instance_type").addEventListener("change", onInstanceTypeChange);
  }

  if (form) {
    form.addEventListener("input", function () {
      clearTimeout(window._previewTimeout);
      window._previewTimeout = setTimeout(analyze, 400);
    });
    form.addEventListener("change", function () {
      clearTimeout(window._previewTimeout);
      window._previewTimeout = setTimeout(analyze, 400);
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", function () {
      loadProviders();
      setTimeout(analyze, 500);
    });
  } else {
    loadProviders();
    setTimeout(analyze, 500);
  }
})();

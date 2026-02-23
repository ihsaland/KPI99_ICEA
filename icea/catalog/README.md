# Provider catalogs

Each JSON file defines one provider with:

- **id**: provider key (e.g. `aws`, `emr`)
- **name**: display name
- **regions**: `[{ "id": "us-east-1", "name": "US East (N. Virginia)" }, ...]`
- **instance_types**: list of instances. Each has:
  - **id**, **name**, **cores**, **memory_gb**
  - **hourly_usd** (single default) and/or **prices**: `{ "region_id": 0.80, ... }` for regional pricing

Regional pricing: if **prices** is present, `GET /v1/catalog/instances?cloud=X&region=Y` returns `hourly_usd` for that region. Otherwise **hourly_usd** is used.

Files: `aws`, `azure`, `gcp`, `oci`, `alibaba`, `ibm`, `digitalocean`, `linode`, `databricks`, `on-prem`, `emr`, `synapse`, `dataproc`.

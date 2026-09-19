import json
import os
from pathlib import Path

summary = json.loads(Path("bundle-summary.json").read_text())
job = summary["resources"]["jobs"]["electricity_etl"]
job_id = str(job.get("id") or "").strip()
job_name = str(job.get("name") or "electricity-etl").strip()
if not job_id or job_id in {"None", "null"}:
    raise SystemExit("bundle summary did not include resources.jobs.electricity_etl.id")
Path("job.env").write_text(
    f"ELECTRICITY_ETL_JOB_ID={job_id}\nELECTRICITY_ETL_JOB_NAME={job_name}\n"
)
print(f"Deployed job id={job_id} name={job_name}")
with Path(os.environ["GITHUB_OUTPUT"]).open("a") as fh:
    fh.write(f"job_id={job_id}\njob_name={job_name}\n")

"""Fabric capacity-unit (CU) consumption rates used by the compute estimate.

Single-sourced from Microsoft Learn, recorded with the page and the date it was read, like the compatibility catalog.
Microsoft can change these at any time (each page says so): re-verify before relying on an estimate, and update
``VERIFIED_ON`` and the values together.
"""

from __future__ import annotations

VERIFIED_ON = "2026-10-15"

PIPELINES_PRICING = "https://learn.microsoft.com/en-us/fabric/data-factory/pricing-pipelines"  # ms.date 2026-06-19
SPARK_COMPUTE = "https://learn.microsoft.com/en-us/fabric/data-engineering/spark-compute"  # ms.date 2026-03-05
DATAFLOW_GEN2_PRICING = "https://learn.microsoft.com/en-us/fabric/data-factory/pricing-dataflows-gen2"  # ms.date 2026-09-10
THROTTLING = "https://learn.microsoft.com/en-us/fabric/enterprise/throttling"  # ms.date 2026-08-14

# Pipelines: "1.5 CU hours" per intelligent optimization throughput (ITO) resource-hour of Copy run time.
DATA_MOVEMENT_CU_PER_ITO = 1.5
# Pipelines: "0.0056 CU hours for each non-copy activity run" (orchestration).
ORCHESTRATION_CU_HOURS_PER_RUN = 0.0056
# Pipelines: SSIS "1.35 CU hours per VCore".
SSIS_CU_PER_VCORE = 1.35
# Spark: "One capacity unit = two Spark vCores."
SPARK_VCORES_PER_CU = 2
# Spark: starter pools use Medium nodes ("Medium | 8 | 64 GB").
SPARK_MEDIUM_NODE_VCORES = 8
# Throttling: background operations (pipelines) are smoothed over 24 hours; an F<n> provides n CUs.
SMOOTHING_HOURS_BACKGROUND = 24
F_SKUS = (2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048)

# Copy is billed per started minute: a 10-second Fabric copy at 4 throughput units reported billableDuration
# 0.0667 DIU-hours (4 x 1 minute) in its run output, the same as ADF. Observed in both products' activity-run
# output on 2026-10-15 (PL_Compute_Test); not stated on the pricing page.
COPY_BILLING_GRANULARITY_SECONDS = 60

# Fabric documents custom Copy throughput values of 4-256 (the converter warns on lower DIUs).
ITO_MINIMUM = 4

SOURCES = [
    {"what": "Pipeline data movement and orchestration rates", "url": PIPELINES_PRICING, "verified_on": VERIFIED_ON},
    {"what": "Spark vCores per CU and node sizes", "url": SPARK_COMPUTE, "verified_on": VERIFIED_ON},
    {"what": "Dataflow Gen2 rates (mapping data flow transforms not billed yet)", "url": DATAFLOW_GEN2_PRICING,
     "verified_on": VERIFIED_ON},
    {"what": "Copy billed per started minute (observed in ADF and Fabric activity-run billingReference)",
     "url": "measured: PL_Compute_Test run output in adf-data-migration-7593 and adf-migration-test", "verified_on": VERIFIED_ON},
    {"what": "24-hour smoothing of background operations; F SKU CU", "url": THROTTLING, "verified_on": VERIFIED_ON},
]

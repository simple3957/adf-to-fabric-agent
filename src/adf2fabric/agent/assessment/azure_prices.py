"""Azure Databricks prices for the compute estimate: live from the Azure Retail Prices API, or the recorded table.

Live prices are read per region, tier and VM size, so the estimate works for any Databricks workspace. The region and
tier come from the workspace itself (an ARM GET of the linked service's ``workspaceResourceId``). The recorded table in
``compute_rates`` is the offline fallback and covers only what was read on ``DATABRICKS_VERIFIED_ON``.

All calls are read-only GETs; the Retail Prices API needs no authentication. Transports are injectable for tests.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Optional, Protocol

from adf2fabric.agent.assessment import compute_rates as R

RETAIL_PRICES = R.AZURE_RETAIL_PRICES
CURRENCIES = ("USD", "INR")
WORKSPACE_API_VERSION = "2024-05-01"
_MAX_PAGES = 20


class PriceLookup(Protocol):
    source: str

    def dbu(self, region: str, tier: str, workload: str = "jobs") -> Optional[dict[str, float]]: ...

    def vm(self, region: str, node_type: str) -> Optional[dict[str, float]]: ...


class RecordedPrices:
    """The table in compute_rates (one region); None for anything it does not hold."""

    source = f"recorded table ({R.DATABRICKS_REGION}, read {R.DATABRICKS_VERIFIED_ON})"

    def dbu(self, region: str, tier: str, workload: str = "jobs") -> Optional[dict[str, float]]:
        return R.DATABRICKS_DBU_PRICE.get((tier, workload)) if region == R.DATABRICKS_REGION else None

    def vm(self, region: str, node_type: str) -> Optional[dict[str, float]]:
        return R.DATABRICKS_VM_PRICE.get(node_type) if region == R.DATABRICKS_REGION else None


class LivePrices:
    """Azure Retail Prices API (pay-as-you-go, Linux). Results are cached per query."""

    def __init__(self, transport: Any, currencies: tuple[str, ...] = CURRENCIES):
        self._t, self._cur, self._cache = transport, currencies, {}
        self.source = f"Azure Retail Prices API ({RETAIL_PRICES}), read {date.today().isoformat()}"

    def _items(self, flt: str, currency: str) -> list[dict]:
        out, url, params = [], RETAIL_PRICES, {"currencyCode": currency, "$filter": flt}
        for _ in range(_MAX_PAGES):
            resp = self._t.get(url, params=params, timeout=60)
            if getattr(resp, "status_code", None) != 200:
                raise RuntimeError(f"Retail Prices API returned HTTP {getattr(resp, 'status_code', '?')}")
            body = resp.json() or {}
            out += body.get("Items", [])
            url, params = body.get("NextPageLink"), None
            if not url:
                break
        return out

    def _price(self, key: tuple, flt: str, keep) -> Optional[dict[str, float]]:
        if key not in self._cache:
            prices: dict[str, float] = {}
            for cur in self._cur:
                items = [i for i in self._items(flt, cur) if keep(i)]
                if items:
                    prices[cur] = min(float(i["retailPrice"]) for i in items)
            self._cache[key] = prices or None
        return self._cache[key]

    def dbu(self, region: str, tier: str, workload: str = "jobs") -> Optional[dict[str, float]]:
        sku = f"{tier.capitalize()} {'Jobs Compute' if workload == 'jobs' else 'All-purpose Compute'}"
        flt = (f"serviceName eq 'Azure Databricks' and armRegionName eq '{region}' and priceType eq 'Consumption' "
               f"and skuName eq '{sku}'")
        return self._price(("dbu", region, tier, workload), flt,
                           lambda i: i.get("meterName") == f"{sku} DBU" and "Photon" not in i.get("meterName", ""))

    def vm(self, region: str, node_type: str) -> Optional[dict[str, float]]:
        flt = (f"serviceName eq 'Virtual Machines' and armRegionName eq '{region}' and armSkuName eq '{node_type}' "
               "and priceType eq 'Consumption'")
        return self._price(("vm", region, node_type), flt,
                           lambda i: not any(x in i.get("skuName", "") for x in ("Spot", "Low Priority"))
                           and "Windows" not in i.get("productName", ""))


def workspace_info(transport: Any, token: str, resource_id: str) -> Optional[tuple[str, str]]:
    """(region, tier) of a Databricks workspace from ARM, e.g. ("centralindia", "premium"); None if not readable."""
    if not resource_id or not resource_id.lower().startswith("/subscriptions/"):
        return None
    resp = transport.get(f"https://management.azure.com{resource_id}?api-version={WORKSPACE_API_VERSION}",
                         headers={"Authorization": f"Bearer {token}"}, timeout=60)
    if getattr(resp, "status_code", None) != 200:
        return None
    body = resp.json() or {}
    region = (body.get("location") or "").replace(" ", "").lower()
    tier = ((body.get("sku") or {}).get("name") or "").lower()
    return (region, tier) if region and tier else None

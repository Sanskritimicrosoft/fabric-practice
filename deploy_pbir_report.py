#!/usr/bin/env python3
"""Deploy a Power BI report to Microsoft Fabric using the PBIR folder format."""

import sys
import json
import base64
import time
import subprocess
import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
WORKSPACE_ID = "33ae4c2c-e78c-44be-81f9-911aec35cb56"
TABLE = "randstad_demo_120_rows"
API = "https://api.fabric.microsoft.com/v1"
REPORT_NAME = "Randstad Executive Dashboard"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def b64(obj: dict | list | str) -> str:
    """Return base64-encoded JSON string."""
    raw = json.dumps(obj) if not isinstance(obj, str) else obj
    return base64.b64encode(raw.encode()).decode()


def part(path: str, payload_obj) -> dict:
    return {"path": path, "payload": b64(payload_obj), "payloadType": "InlineBase64"}


def get_token() -> str:
    result = subprocess.run(
        'az account get-access-token --resource "https://api.fabric.microsoft.com" --query accessToken -o tsv',
        capture_output=True, text=True, check=True, shell=True,
    )
    return result.stdout.strip()


def headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


# ---------------------------------------------------------------------------
# Field helpers
# ---------------------------------------------------------------------------

def col(prop: str) -> dict:
    return {
        "field": {
            "Column": {
                "Expression": {"SourceRef": {"Entity": TABLE}},
                "Property": prop,
            }
        },
        "queryRef": f"{TABLE}.{prop}",
        "nativeQueryRef": prop,
    }


def msr(prop: str) -> dict:
    return {
        "field": {
            "Measure": {
                "Expression": {"SourceRef": {"Entity": TABLE}},
                "Property": prop,
            }
        },
        "queryRef": f"{TABLE}.{prop}",
        "nativeQueryRef": prop,
    }


# ---------------------------------------------------------------------------
# Visual builder
# ---------------------------------------------------------------------------
_vid = 0


def _next_id() -> str:
    global _vid
    _vid += 1
    return f"v{_vid:04d}"


def _visual(vtype: str, x: int, y: int, w: int, h: int, query_state: dict | None = None) -> dict:
    name = _next_id()
    v: dict = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/1.0.0/schema.json",
        "name": name,
        "position": {"x": x, "y": y, "width": w, "height": h},
        "visual": {"visualType": vtype},
    }
    if query_state:
        v["visual"]["query"] = {"queryState": query_state}
    return v


def card_visual(x, y, field_fn, field_name):
    return _visual("card", x, y, 290, 120, {"Values": {"projections": [field_fn(field_name)]}})


def bar_visual(x, y, w, h, cat_col, val_field):
    return _visual("clusteredBarChart", x, y, w, h, {
        "Category": {"projections": [col(cat_col)]},
        "Y": {"projections": [val_field]},
    })


def col_chart_visual(x, y, w, h, cat_col, val_field):
    return _visual("clusteredColumnChart", x, y, w, h, {
        "Category": {"projections": [col(cat_col)]},
        "Y": {"projections": [val_field]},
    })


def donut_visual(x, y, w, h, cat_col, val_field):
    return _visual("donutChart", x, y, w, h, {
        "Category": {"projections": [col(cat_col)]},
        "Y": {"projections": [val_field]},
    })


def table_visual(x, y, w, h, columns: list[str]):
    return _visual("tableEx", x, y, w, h, {
        "Values": {"projections": [col(c) for c in columns]},
    })


def slicer_visual(x, y, w, h, column: str):
    return _visual("slicer", x, y, w, h, {
        "Values": {"projections": [col(column)]},
    })


# ---------------------------------------------------------------------------
# Page definitions
# ---------------------------------------------------------------------------

def page1_visuals():
    visuals = []
    # 4 KPI cards across top
    visuals.append(card_visual(20, 20, msr, "Total Margin at Risk"))
    visuals.append(card_visual(330, 20, msr, "Average Risk Score"))
    visuals.append(card_visual(640, 20, msr, "High Risk Clients"))
    visuals.append(card_visual(950, 20, msr, "Open Job Orders"))
    # Bar: Margin at Risk by client_name
    visuals.append(bar_visual(20, 160, 600, 250, "client_name", col("margin_at_risk")))
    # Bar: Risk Score by client_name
    visuals.append(bar_visual(640, 160, 600, 250, "client_name", col("risk_score")))
    # Donut: Risk Level distribution
    visuals.append(donut_visual(20, 430, 350, 270, "risk_level", col("client_name")))
    # Table
    visuals.append(table_visual(390, 430, 500, 270,
                                ["client_name", "region", "industry", "risk_score", "margin_at_risk", "main_issue"]))
    # 3 Slicers
    visuals.append(slicer_visual(910, 430, 170, 90, "region"))
    visuals.append(slicer_visual(910, 530, 170, 90, "industry"))
    visuals.append(slicer_visual(910, 630, 170, 90, "risk_level"))
    return visuals


def page2_visuals():
    visuals = []
    visuals.append(card_visual(20, 20, msr, "Negative Interactions"))
    visuals.append(card_visual(330, 20, msr, "Average Days Open"))
    visuals.append(bar_visual(20, 160, 600, 250, "main_issue", col("client_name")))
    visuals.append(bar_visual(640, 160, 600, 250, "client_name", col("interaction_issue")))
    visuals.append(table_visual(20, 430, 750, 270,
                                ["client_name", "interaction_date", "interaction_type", "interaction_issue", "sentiment"]))
    visuals.append(slicer_visual(790, 430, 200, 90, "sentiment"))
    visuals.append(slicer_visual(790, 540, 200, 90, "client_name"))
    return visuals


def page3_visuals():
    visuals = []
    visuals.append(card_visual(20, 20, msr, "Open Job Orders"))
    visuals.append(card_visual(330, 20, msr, "Average Match Score"))
    visuals.append(card_visual(640, 20, msr, "Available Candidates"))
    visuals.append(bar_visual(20, 160, 400, 250, "client_name", col("days_open")))
    visuals.append(col_chart_visual(440, 160, 400, 250, "job_status", col("job_id")))
    visuals.append(bar_visual(860, 160, 400, 250, "required_skill", col("job_id")))
    visuals.append(table_visual(20, 430, 1230, 270,
                                ["client_name", "role", "required_skill", "job_status", "days_open", "candidate_name", "match_score"]))
    return visuals


def page4_visuals():
    visuals = []
    visuals.append(card_visual(20, 20, msr, "High Impact Alerts"))
    visuals.append(bar_visual(20, 160, 600, 280, "client_name", col("event_type")))
    visuals.append(bar_visual(640, 160, 600, 280, "recommended_action", col("client_name")))
    visuals.append(table_visual(20, 460, 1230, 240,
                                ["client_name", "event_time", "event_description", "impact_level", "recommended_action"]))
    return visuals


PAGES = [
    ("ReportSection1", "Executive Risk Cockpit", page1_visuals),
    ("ReportSection2", "Why At Risk?", page2_visuals),
    ("ReportSection3", "Staffing & Fulfilment", page3_visuals),
    ("ReportSection4", "AI Actions & Alerts", page4_visuals),
]


# ---------------------------------------------------------------------------
# Build the full definition parts list
# ---------------------------------------------------------------------------

def build_parts(sm_id: str) -> list[dict]:
    parts: list[dict] = []

    # 1. definition.pbir
    parts.append(part("definition.pbir", {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definitionProperties/2.0.0/schema.json",
        "version": "4.0",
        "datasetReference": {
            "byConnection": {
                "connectionString": f"semanticmodelid={sm_id}",
            }
        },
    }))

    # 2. definition/version.json
    parts.append(part("definition/version.json", {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/versionMetadata/1.0.0/schema.json",
        "version": "1.0.0"
    }))

    # 3. definition/report.json
    parts.append(part("definition/report.json", {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/report/1.0.0/schema.json",
        "themeCollection": {
            "baseTheme": {
                "name": "CY24SU06",
                "reportVersionAtImport": "5.53",
                "type": "SharedResources",
            }
        },
        "layoutOptimization": "None",
    }))

    # 4. definition/pages/pages.json
    parts.append(part("definition/pages/pages.json", {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/pagesMetadata/1.0.0/schema.json",
        "pageOrder": [p[0] for p in PAGES],
    }))

    # 5 & 6. Per-page parts
    for page_name, display_name, visuals_fn in PAGES:
        # page.json
        parts.append(part(f"definition/pages/{page_name}/page.json", {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/1.0.0/schema.json",
            "name": page_name,
            "displayName": display_name,
            "displayOption": "FitToPage",
            "height": 720,
            "width": 1280,
        }))

        # visual.json for each visual
        for vis in visuals_fn():
            vis_name = vis["name"]
            parts.append(part(
                f"definition/pages/{page_name}/visuals/{vis_name}/visual.json",
                vis,
            ))

    return parts


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if len(sys.argv) < 2:
        print("Usage: python deploy_pbir_report.py <SEMANTIC_MODEL_ID>")
        sys.exit(1)

    sm_id = sys.argv[1]
    print(f"Semantic Model ID: {sm_id}")

    # Auth
    print("Getting auth token...")
    token = get_token()
    hdrs = headers(token)

    # Delete existing report with same name
    print(f"Checking for existing report '{REPORT_NAME}'...")
    resp = requests.get(f"{API}/workspaces/{WORKSPACE_ID}/items?type=Report", headers=hdrs)
    resp.raise_for_status()
    for item in resp.json().get("value", []):
        if item["displayName"] == REPORT_NAME:
            print(f"  Deleting existing report {item['id']}...")
            requests.delete(f"{API}/workspaces/{WORKSPACE_ID}/items/{item['id']}", headers=hdrs).raise_for_status()
            print("  Deleted.")
            time.sleep(2)

    # Build definition
    print("Building PBIR definition...")
    parts_list = build_parts(sm_id)
    print(f"  {len(parts_list)} parts built.")

    body = {
        "displayName": REPORT_NAME,
        "type": "Report",
        "definition": {"parts": parts_list},
    }

    # Create report
    print("Creating report via Fabric API...")
    resp = requests.post(
        f"{API}/workspaces/{WORKSPACE_ID}/items",
        headers=hdrs,
        json=body,
    )

    if resp.status_code == 201:
        report_id = resp.json()["id"]
        print(f"Report created immediately: {report_id}")
    elif resp.status_code == 202:
        # Async – poll operation
        operation_url = resp.headers.get("Location") or resp.headers.get("x-ms-operation-url")
        retry_after = int(resp.headers.get("Retry-After", "5"))
        print(f"Async operation started. Polling (retry-after={retry_after}s)...")

        if operation_url and not operation_url.startswith("http"):
            operation_url = f"{API}/{operation_url.lstrip('/')}"

        for attempt in range(60):
            time.sleep(retry_after)
            poll = requests.get(operation_url, headers=hdrs)
            poll.raise_for_status()
            data = poll.json()
            status = data.get("status", "Unknown")
            print(f"  Poll {attempt+1}: {status}")
            if status in ("Succeeded", "succeeded"):
                report_id = data.get("resourceId") or data.get("id")
                break
            elif status in ("Failed", "failed"):
                print(f"Operation failed: {json.dumps(data, indent=2)}")
                sys.exit(1)
        else:
            print("Timed out waiting for operation to complete.")
            sys.exit(1)

        # If resourceId wasn't in the poll response, list reports to find it
        if not report_id:
            resp2 = requests.get(f"{API}/workspaces/{WORKSPACE_ID}/items?type=Report", headers=hdrs)
            resp2.raise_for_status()
            for item in resp2.json().get("value", []):
                if item["displayName"] == REPORT_NAME:
                    report_id = item["id"]
                    break
    else:
        print(f"Unexpected status {resp.status_code}: {resp.text}")
        sys.exit(1)

    url = f"https://app.fabric.microsoft.com/groups/{WORKSPACE_ID}/reports/{report_id}"
    print(f"\nReport deployed successfully!")
    print(f"Report ID: {report_id}")
    print(f"URL: {url}")


if __name__ == "__main__":
    main()

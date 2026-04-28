#!/usr/bin/env python3
"""Deploy a simplified single-page Randstad Client Risk Cockpit report (PBIR format)."""

import sys, json, base64, time, subprocess, requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
WORKSPACE_ID = "33ae4c2c-e78c-44be-81f9-911aec35cb56"
SM_ID = "905f10c5-2189-4ee1-807c-1caf56e249f1"
TABLE = "randstad_demo_120_rows"
API = "https://api.fabric.microsoft.com/v1"
REPORT_NAME = "Randstad Executive Dashboard"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def b64(obj):
    raw = json.dumps(obj) if not isinstance(obj, str) else obj
    return base64.b64encode(raw.encode()).decode()

def part(path, payload_obj):
    return {"path": path, "payload": b64(payload_obj), "payloadType": "InlineBase64"}

def get_token():
    r = subprocess.run(
        'az account get-access-token --resource "https://api.fabric.microsoft.com" --query accessToken -o tsv',
        capture_output=True, text=True, check=True, shell=True,
    )
    return r.stdout.strip()

def headers(token):
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# ---------------------------------------------------------------------------
# Field references
# ---------------------------------------------------------------------------

def col(prop):
    return {
        "field": {"Column": {"Expression": {"SourceRef": {"Entity": TABLE}}, "Property": prop}},
        "queryRef": f"{TABLE}.{prop}",
        "nativeQueryRef": prop,
    }

def msr(prop):
    return {
        "field": {"Measure": {"Expression": {"SourceRef": {"Entity": TABLE}}, "Property": prop}},
        "queryRef": f"{TABLE}.{prop}",
        "nativeQueryRef": prop,
    }

# ---------------------------------------------------------------------------
# Visual builders
# ---------------------------------------------------------------------------
_vid = 0

def _next_id():
    global _vid
    _vid += 1
    return f"v{_vid:04d}"

def _visual(vtype, x, y, w, h, query_state=None):
    name = _next_id()
    v = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/1.0.0/schema.json",
        "name": name,
        "position": {"x": x, "y": y, "width": w, "height": h},
        "visual": {"visualType": vtype},
    }
    if query_state:
        v["visual"]["query"] = {"queryState": query_state}
    return v

def card_visual(x, y, measure_name):
    return _visual("card", x, y, 290, 120, {"Values": {"projections": [msr(measure_name)]}})

def bar_visual(x, y, w, h, cat_col, val_field):
    return _visual("clusteredBarChart", x, y, w, h, {
        "Category": {"projections": [col(cat_col)]},
        "Y": {"projections": [val_field]},
    })

def table_visual(x, y, w, h, columns):
    return _visual("tableEx", x, y, w, h, {
        "Values": {"projections": [col(c) for c in columns]},
    })

def slicer_visual(x, y, w, h, column):
    return _visual("slicer", x, y, w, h, {
        "Values": {"projections": [col(column)]},
    })

# ---------------------------------------------------------------------------
# Page: Client Risk Cockpit
# ---------------------------------------------------------------------------

def build_visuals():
    visuals = []

    # Row 1: 3 KPI cards
    visuals.append(card_visual(20, 20, "Total Margin at Risk"))
    visuals.append(card_visual(330, 20, "Average Risk Score"))
    visuals.append(card_visual(640, 20, "High Risk Clients"))

    # Row 2: 2 bar charts
    visuals.append(bar_visual(20, 160, 600, 250, "client_name", msr("Total Margin at Risk")))
    visuals.append(bar_visual(640, 160, 600, 250, "client_name", msr("Average Risk Score")))

    # Row 3: Table + 3 slicers
    visuals.append(table_visual(20, 430, 940, 270,
        ["client_name", "region", "industry", "account_owner", "risk_score", "margin_at_risk", "main_issue", "recommended_action"]))
    visuals.append(slicer_visual(980, 430, 270, 80, "region"))
    visuals.append(slicer_visual(980, 520, 270, 80, "industry"))
    visuals.append(slicer_visual(980, 610, 270, 80, "risk_level"))

    return visuals

# ---------------------------------------------------------------------------
# Build PBIR parts
# ---------------------------------------------------------------------------

def build_parts():
    parts = []

    # definition.pbir
    parts.append(part("definition.pbir", {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definitionProperties/2.0.0/schema.json",
        "version": "4.0",
        "datasetReference": {
            "byConnection": {
                "connectionString": f"semanticmodelid={SM_ID}",
            }
        },
    }))

    # version.json
    parts.append(part("definition/version.json", {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/versionMetadata/1.0.0/schema.json",
        "version": "1.0.0"
    }))

    # report.json
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

    # pages.json - single page
    page_name = "ClientRiskCockpit"
    parts.append(part("definition/pages/pages.json", {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/pagesMetadata/1.0.0/schema.json",
        "pageOrder": [page_name],
    }))

    # page.json
    parts.append(part(f"definition/pages/{page_name}/page.json", {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/1.0.0/schema.json",
        "name": page_name,
        "displayName": "Client Risk Cockpit",
        "displayOption": "FitToPage",
        "height": 720,
        "width": 1280,
    }))

    # visuals
    for vis in build_visuals():
        vis_name = vis["name"]
        parts.append(part(f"definition/pages/{page_name}/visuals/{vis_name}/visual.json", vis))

    return parts

# ---------------------------------------------------------------------------
# Deploy
# ---------------------------------------------------------------------------

def main():
    token = get_token()
    h = headers(token)
    print(f"Semantic Model: {SM_ID}")

    # Delete existing report
    print("Checking for existing report...")
    r = requests.get(f"{API}/workspaces/{WORKSPACE_ID}/items?type=Report", headers=h)
    for it in r.json().get("value", []):
        if it["displayName"] == REPORT_NAME:
            print(f"  Deleting {it['id']}...")
            requests.delete(f"{API}/workspaces/{WORKSPACE_ID}/items/{it['id']}", headers=h)
            time.sleep(3)

    # Build and deploy
    parts = build_parts()
    print(f"Built {len(parts)} parts (1 page, {len(parts) - 5} visuals)")

    payload = {
        "displayName": REPORT_NAME,
        "type": "Report",
        "definition": {"parts": parts},
    }

    r = requests.post(f"{API}/workspaces/{WORKSPACE_ID}/items", headers=h, json=payload)
    print(f"Create: {r.status_code}")

    if r.status_code == 202:
        op_id = r.headers.get("x-ms-operation-id", "")
        for i in range(20):
            time.sleep(3)
            poll = requests.get(f"{API}/operations/{op_id}", headers=h)
            st = poll.json().get("status", "")
            print(f"  Poll {i}: {st}")
            if st == "Succeeded":
                break
            if st == "Failed":
                print(f"  Error: {poll.json().get('error', {}).get('message', '')[:500]}")
                sys.exit(1)
    elif r.status_code == 201:
        pass
    else:
        print(r.text[:500])
        sys.exit(1)

    # Get report ID
    r2 = requests.get(f"{API}/workspaces/{WORKSPACE_ID}/items?type=Report", headers=h)
    for it in r2.json().get("value", []):
        if it["displayName"] == REPORT_NAME:
            rid = it["id"]
            print(f"\nReport ID: {rid}")
            print(f"URL: https://app.fabric.microsoft.com/groups/{WORKSPACE_ID}/reports/{rid}")
            break

if __name__ == "__main__":
    main()

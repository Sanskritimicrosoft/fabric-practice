"""Final clean deployment: semantic model + PBIR report with correct column names."""
import subprocess, requests, json, base64, time, sys, os, uuid

WORKSPACE_ID = "33ae4c2c-e78c-44be-81f9-911aec35cb56"
LAKEHOUSE_ID = "3a63410e-9dba-4a4f-8fa1-c0b419a3342d"
SQL_ENDPOINT_CONN = "7zy6lned6lkulpbvjqg6klyvwu-frgk4m4m467ejapzsenoynolky.datawarehouse.fabric.microsoft.com"
SQL_ENDPOINT_ID = "d8c4d0e2-3e83-4396-9079-20947dad59c9"
LAKEHOUSE_CONN_ID = "5499266c-d55e-4982-a276-6395677a7db0"
TABLE_NAME = "randstad_demo_120_rows"
API_BASE = "https://api.fabric.microsoft.com/v1"

def get_token(resource="https://api.fabric.microsoft.com"):
    r = subprocess.run(
        f'az account get-access-token --resource "{resource}" --query accessToken -o tsv',
        capture_output=True, text=True, shell=True
    )
    return r.stdout.strip()

def b64enc(obj):
    return base64.b64encode(json.dumps(obj).encode()).decode()

def uid():
    return str(uuid.uuid4())

token = get_token()
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# Step 1: Clean up
print("=== Step 1: Cleaning up ===")
r = requests.get(f"{API_BASE}/workspaces/{WORKSPACE_ID}/items", headers=headers)
for it in r.json().get("value", []):
    if it["type"] in ("SemanticModel", "Report") and "Randstad" in it.get("displayName", ""):
        print(f"  Deleting {it['type']}: {it['displayName']}")
        requests.delete(f"{API_BASE}/workspaces/{WORKSPACE_ID}/items/{it['id']}", headers=headers)
        time.sleep(2)
time.sleep(3)

# Step 2: Build model with CORRECT column names matching the Delta table
print("\n=== Step 2: Creating Semantic Model ===")
columns = [
    ("client_name", "string"),
    ("region", "string"),
    ("industry", "string"),
    ("account_owner", "string"),
    ("revenue", "double"),
    ("risk_score", "double"),
    ("margin_at_risk", "double"),
    ("risk_level", "string"),
    ("main_issue", "string"),
    ("sentiment", "string"),
    ("interaction_date", "dateTime"),
    ("interaction_type", "string"),
    ("interaction_issue", "string"),
    ("job_id", "string"),
    ("role", "string"),
    ("required_skill", "string"),
    ("job_status", "string"),
    ("days_open", "int64"),
    ("candidate_id", "string"),
    ("candidate_name", "string"),
    ("candidate_skill", "string"),
    ("match_score", "double"),
    ("availability", "string"),
    ("event_time", "dateTime"),
    ("event_type", "string"),
    ("event_description", "string"),
    ("impact_level", "string"),
    ("recommended_action", "string"),
]

col_defs = []
for name, dtype in columns:
    col_defs.append({
        "name": name,
        "dataType": dtype,
        "sourceColumn": name,
        "type": "data",
        "lineageTag": uid(),
        "summarizeBy": "none" if dtype in ("string", "dateTime") else "sum",
    })

T = TABLE_NAME
measures = [
    ("Total Margin at Risk", f"SUM('{T}'[margin_at_risk])", "#,##0"),
    ("Average Risk Score", f"AVERAGE('{T}'[risk_score])", "0.0"),
    ("High Risk Clients", f'CALCULATE(DISTINCTCOUNT(\'{T}\'[client_name]), \'{T}\'[risk_level] = "High")', "0"),
    ("Open Job Orders", f'CALCULATE(DISTINCTCOUNT(\'{T}\'[job_id]), \'{T}\'[job_status] = "Open")', "0"),
    ("Negative Interactions", f'CALCULATE(COUNTROWS(\'{T}\'), \'{T}\'[sentiment] = "Negative")', "0"),
    ("Available Candidates", f'CALCULATE(DISTINCTCOUNT(\'{T}\'[candidate_id]), \'{T}\'[availability] = "Available")', "0"),
    ("High Impact Alerts", f'CALCULATE(COUNTROWS(\'{T}\'), \'{T}\'[impact_level] = "High")', "0"),
    ("Average Days Open", f"AVERAGE('{T}'[days_open])", "0.0"),
    ("Average Match Score", f"AVERAGE('{T}'[match_score])", "0.0"),
    ("Total Revenue", f"SUM('{T}'[revenue])", "#,##0"),
    ("Client Count", f"DISTINCTCOUNT('{T}'[client_name])", "0"),
]

measure_defs = [{"name": n, "expression": e, "formatString": f, "lineageTag": uid()} for n, e, f in measures]

model_bim = {
    "compatibilityLevel": 1604,
    "model": {
        "culture": "en-US",
        "defaultPowerBIDataSourceVersion": "powerBI_V3",
        "defaultMode": "directLake",
        "tables": [{
            "name": TABLE_NAME,
            "lineageTag": uid(),
            "columns": col_defs,
            "measures": measure_defs,
            "partitions": [{
                "name": "partition",
                "mode": "directLake",
                "source": {
                    "type": "entity",
                    "entityName": TABLE_NAME,
                    "schemaName": "dbo",
                    "expressionSource": "DatabaseQuery"
                }
            }]
        }],
        "expressions": [{
            "name": "DatabaseQuery",
            "kind": "m",
            "expression": f'let\n    database = Sql.Database("{SQL_ENDPOINT_CONN}", "{SQL_ENDPOINT_ID}")\nin\n    database'
        }]
    }
}

pbism = {"version": "1.0", "settings": {}}
payload = {
    "displayName": "Randstad Executive Intelligence",
    "type": "SemanticModel",
    "definition": {
        "parts": [
            {"path": "model.bim", "payload": b64enc(model_bim), "payloadType": "InlineBase64"},
            {"path": "definition.pbism", "payload": b64enc(pbism), "payloadType": "InlineBase64"},
        ]
    }
}

r = requests.post(f"{API_BASE}/workspaces/{WORKSPACE_ID}/items", headers=headers, json=payload)
print(f"Create status: {r.status_code}")

sm_id = None
if r.status_code == 201:
    sm_id = r.json()["id"]
elif r.status_code == 202:
    op_id = r.headers.get("x-ms-operation-id", "")
    for i in range(30):
        time.sleep(3)
        poll = requests.get(f"{API_BASE}/operations/{op_id}", headers=headers)
        st = poll.json().get("status", "")
        print(f"  Poll {i}: {st}")
        if st == "Succeeded":
            break
        if st == "Failed":
            print(f"  Error: {poll.json().get('error', {}).get('message', '')}")
            sys.exit(1)
    r2 = requests.get(f"{API_BASE}/workspaces/{WORKSPACE_ID}/items?type=SemanticModel", headers=headers)
    for it in r2.json().get("value", []):
        if it["displayName"] == "Randstad Executive Intelligence":
            sm_id = it["id"]
else:
    print(r.text[:500])
    sys.exit(1)

print(f"Semantic Model ID: {sm_id}")

# Step 3: Fix data source binding
print("\n=== Step 3: Fixing data source binding ===")
pbi_token = get_token("https://analysis.windows.net/powerbi/api")
pbi_headers = {"Authorization": f"Bearer {pbi_token}", "Content-Type": "application/json"}

# Bind to gateway (Lakehouse connection)
time.sleep(5)
r3 = requests.post(
    f"https://api.powerbi.com/v1.0/myorg/groups/{WORKSPACE_ID}/datasets/{sm_id}/Default.BindToGateway",
    headers=pbi_headers,
    json={"gatewayObjectId": LAKEHOUSE_CONN_ID, "datasourceObjectIds": []}
)
print(f"BindToGateway: {r3.status_code}")

# Update datasources to use lakehouse name
r4 = requests.post(
    f"https://api.powerbi.com/v1.0/myorg/groups/{WORKSPACE_ID}/datasets/{sm_id}/Default.UpdateDatasources",
    headers=pbi_headers,
    json={"updateDetails": [{
        "datasourceSelector": {
            "datasourceType": "Sql",
            "connectionDetails": {
                "server": SQL_ENDPOINT_CONN,
                "database": SQL_ENDPOINT_ID
            }
        },
        "connectionDetails": {
            "server": SQL_ENDPOINT_CONN,
            "database": "randstad_demo_lakehouse"
        }
    }]}
)
print(f"UpdateDatasources: {r4.status_code}")

# Refresh
time.sleep(3)
rr = requests.post(
    f"https://api.powerbi.com/v1.0/myorg/groups/{WORKSPACE_ID}/datasets/{sm_id}/refreshes",
    headers=pbi_headers
)
print(f"Refresh: {rr.status_code}")
time.sleep(15)

# Step 4: Test DAX
print("\n=== Step 4: Testing DAX ===")
test_queries = [
    ("Row count", "EVALUATE ROW(\"cnt\", COUNTROWS(randstad_demo_120_rows))"),
    ("Total Revenue", "EVALUATE ROW(\"rev\", [Total Revenue])"),
    ("Avg Risk Score", "EVALUATE ROW(\"risk\", [Average Risk Score])"),
    ("High Risk Clients", "EVALUATE ROW(\"hrc\", [High Risk Clients])"),
    ("Top 3 clients", f"EVALUATE TOPN(3, SUMMARIZE('{T}', '{T}'[client_name], '{T}'[revenue]))"),
]

all_pass = True
for name, q in test_queries:
    dax = {"queries": [{"query": q}], "serializerSettings": {"includeNulls": True}}
    dr = requests.post(
        f"https://api.powerbi.com/v1.0/myorg/groups/{WORKSPACE_ID}/datasets/{sm_id}/executeQueries",
        headers=pbi_headers, json=dax
    )
    if dr.status_code == 200:
        rows = dr.json().get("results", [{}])[0].get("tables", [{}])[0].get("rows", [])
        print(f"  ✓ {name}: {json.dumps(rows[0]) if rows else 'no rows'}")
    else:
        all_pass = False
        err = dr.json().get("error", {}).get("pbi.error", {}).get("details", [])
        msg = next((d["detail"]["value"][:100] for d in err if d.get("code") == "DetailsMessage"), dr.text[:100])
        print(f"  ✗ {name}: {msg}")

if not all_pass:
    print("\nSome queries failed! Aborting report deployment.")
    sys.exit(1)

# Step 5: Deploy PBIR report
print(f"\n=== Step 5: Deploying PBIR report ===")
os.system(f"python deploy_pbir_report.py {sm_id}")

print(f"\n{'='*60}")
print(f"DEPLOYMENT COMPLETE!")
print(f"Semantic Model ID: {sm_id}")
print(f"Report URL will be printed above by deploy_pbir_report.py")

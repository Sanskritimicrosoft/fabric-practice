#!/usr/bin/env python3
"""Deploy a Fabric Data Agent for the Randstad Operating Intelligence Ontology.

Creates a conversational AI agent that can answer C-level questions like:
  • Which clients have the highest margin at risk and why?
  • Which high-risk clients have open jobs and negative sentiment?
  • Which skills are causing staffing bottlenecks?
  • Which account owners should act first this week?
  • What action is recommended for each high-risk client?

Data sources:
  1. Ontology: Randstad_Risk_Ontology (entity graph — relationships, traversals)
  2. Lakehouse: randstad_demo_lakehouse table randstad_demo_120_rows (aggregations, filters)
"""

import subprocess, requests, json, base64, time, uuid

WORKSPACE_ID = "33ae4c2c-e78c-44be-81f9-911aec35cb56"
ONTOLOGY_ID = "2f5d56c3-16b3-470a-a008-0fa06ed19a4b"
LAKEHOUSE_ID = "3a63410e-9dba-4a4f-8fa1-c0b419a3342d"
TABLE = "randstad_demo_120_rows"
API = "https://api.fabric.microsoft.com/v1"
AGENT_NAME = "Randstad_Risk_Agent"


def get_token():
    return subprocess.run(
        'az account get-access-token --resource "https://api.fabric.microsoft.com" --query accessToken -o tsv',
        capture_output=True, text=True, shell=True
    ).stdout.strip()


def b64(obj):
    return base64.b64encode(json.dumps(obj).encode()).decode()


def part(path, obj):
    return {"path": path, "payload": b64(obj), "payloadType": "InlineBase64"}


def uid():
    return str(uuid.uuid4())


# ─── AI Instructions ───────────────────────────────────────────────────
AI_INSTRUCTIONS = """You are the Randstad Risk Intelligence Agent — a C-level decision support assistant.

You have TWO data sources:
(1) Ontology: Randstad_Risk_Ontology — a knowledge graph with 9 entity types
    (Client, RiskProfile, Job, Candidate, Alert, Interaction, Skill, Action, BranchMarket)
    and 12 relationships connecting them.
(2) Lakehouse table: randstad_demo_120_rows — the underlying data with 28 columns
    and 120 rows of Randstad client risk, workforce, and operations data.

ROUTING RULES (very important):
- Entity traversals and relationship questions → use ONTOLOGY (GQL).
  Examples: "Which clients have alerts?", "What skills does candidate X have?",
  "Show me the relationship between client and risk profile."
- Aggregations, totals, rankings, filtering → use LAKEHOUSE TABLE (SQL).
  Examples: "Total margin at risk", "Top 5 clients by risk score",
  "Average days open for jobs", "Count of high-risk clients."
- Mixed questions (relationships + numbers) → use ONTOLOGY for structure,
  then LAKEHOUSE for computation.

KEY BUSINESS DEFINITIONS:
- "Margin at Risk" = SUM(margin_at_risk) — total revenue exposure from at-risk clients
- "High Risk" = rows where risk_level = 'High'
- "Risk Score" = 0-100 scale; higher = worse
- "Staffing Bottleneck" = skills with many open jobs and few available candidates
- "Negative Sentiment" = interactions where sentiment = 'Negative'
- "Open Jobs" = jobs where job_status = 'Open'
- "Available Candidates" = candidates where availability = 'Available'

ENTITY DESCRIPTIONS:
- Client: A Randstad client account (client_name, region, industry, account_owner, revenue)
- RiskProfile: Quantified risk per client (risk_score, risk_level, margin_at_risk, main_issue)
- Job: Open/filled job order (job_id, role, required_skill, job_status, days_open)
- Candidate: Talent candidate (candidate_id, candidate_name, candidate_skill, match_score)
- Alert: Business event requiring attention (event_type, event_description, impact_level)
- Interaction: Client touchpoint (interaction_type, interaction_issue, sentiment)
- Skill: Workforce skill tracked across jobs and candidates
- Action: Recommended mitigation action from risk profile
- BranchMarket: Regional market/branch serving clients

BEHAVIOR:
- Never ask which data source to use — route automatically.
- Keep answers concise: 1-2 sentences + the key number or table.
- If time period is not specified, state: "Based on all available data."
- For rankings, default to top 5 unless the user specifies otherwise.
- When showing client risk data, always include: client_name, risk_score, margin_at_risk, risk_level.
- When showing workforce data, include: role, required_skill, days_open, job_status.
- Proactively suggest follow-up questions when relevant.

Support group by in GQL"""

# ─── Ontology Data Source ──────────────────────────────────────────────
ONTOLOGY_ENTITIES = [
    ("Client", "client_name, region, industry, account_owner, revenue"),
    ("RiskProfile", "client_name, risk_score, risk_level, margin_at_risk, main_issue, recommended_action"),
    ("Job", "job_id, client_name, role, required_skill, job_status, days_open"),
    ("Candidate", "candidate_id, candidate_name, candidate_skill, match_score, availability"),
    ("Alert", "client_name, event_time, event_type, event_description, impact_level, recommended_action"),
    ("Interaction", "client_name, interaction_date, interaction_type, interaction_issue, sentiment"),
    ("Skill", "skill_name"),
    ("Action", "action_name"),
    ("BranchMarket", "market_region"),
]

ontology_source = {
    "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/dataAgent/definition/dataSource/1.0.0/schema.json",
    "artifactId": ONTOLOGY_ID,
    "workspaceId": WORKSPACE_ID,
    "dataSourceInstructions": None,
    "displayName": "Randstad_Risk_Ontology",
    "type": "ontology",
    "userDescription": None,
    "metadata": {},
    "elements": [
        {
            "id": name,
            "is_selected": True,
            "display_name": name,
            "type": "ontology.entity",
            "description": props,
            "children": [],
        }
        for name, props in ONTOLOGY_ENTITIES
    ],
}

# ─── Lakehouse Data Source ─────────────────────────────────────────────
COLUMNS = [
    ("client_name", "varchar"),
    ("region", "varchar"),
    ("industry", "varchar"),
    ("account_owner", "varchar"),
    ("revenue", "float"),
    ("risk_score", "float"),
    ("margin_at_risk", "float"),
    ("risk_level", "varchar"),
    ("main_issue", "varchar"),
    ("sentiment", "varchar"),
    ("interaction_date", "datetime2"),
    ("interaction_type", "varchar"),
    ("interaction_issue", "varchar"),
    ("job_id", "varchar"),
    ("role", "varchar"),
    ("required_skill", "varchar"),
    ("job_status", "varchar"),
    ("days_open", "int"),
    ("candidate_id", "varchar"),
    ("candidate_name", "varchar"),
    ("candidate_skill", "varchar"),
    ("match_score", "float"),
    ("availability", "varchar"),
    ("event_time", "datetime2"),
    ("event_type", "varchar"),
    ("event_description", "varchar"),
    ("impact_level", "varchar"),
    ("recommended_action", "varchar"),
]

lakehouse_source = {
    "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/dataAgent/definition/dataSource/1.0.0/schema.json",
    "artifactId": LAKEHOUSE_ID,
    "workspaceId": WORKSPACE_ID,
    "dataSourceInstructions": None,
    "displayName": "randstad_demo_lakehouse",
    "type": "lakehouse_tables",
    "userDescription": None,
    "metadata": {},
    "elements": [
        {
            "id": uid(),
            "is_selected": True,
            "display_name": "dbo",
            "type": "lakehouse_tables.schema",
            "description": None,
            "children": [
                {
                    "id": uid(),
                    "is_selected": True,
                    "display_name": TABLE,
                    "type": "lakehouse_tables.table",
                    "description": None,
                    "children": [
                        {
                            "id": uid(),
                            "is_selected": True,
                            "display_name": col_name,
                            "type": "lakehouse_tables.column",
                            "data_type": col_type,
                            "description": None,
                            "children": [],
                        }
                        for col_name, col_type in COLUMNS
                    ],
                }
            ],
        }
    ],
}

stage_config = {
    "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/dataAgent/definition/stageConfiguration/1.0.0/schema.json",
    "aiInstructions": AI_INSTRUCTIONS,
}

# ─── Build Definition Parts ────────────────────────────────────────────
parts = [
    part("Files/Config/data_agent.json", {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/dataAgent/definition/dataAgent/2.1.0/schema.json"
    }),
    # Draft config
    part("Files/Config/draft/stage_config.json", stage_config),
    part("Files/Config/draft/ontology-Randstad_Risk_Ontology/datasource.json", ontology_source),
    part("Files/Config/draft/lakehouse-tables-randstad_demo_lakehouse/datasource.json", lakehouse_source),
    # Publish info
    part("Files/Config/publish_info.json", {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/dataAgent/definition/publishInfo/1.0.0/schema.json",
        "description": "Randstad Risk Intelligence Agent — conversational AI for C-level decision support",
    }),
    # Published config (same as draft)
    part("Files/Config/published/stage_config.json", stage_config),
    part("Files/Config/published/ontology-Randstad_Risk_Ontology/datasource.json", ontology_source),
    part("Files/Config/published/lakehouse-tables-randstad_demo_lakehouse/datasource.json", lakehouse_source),
]

print(f"Built Data Agent definition: {len(parts)} parts")
print(f"  Ontology entities: {len(ONTOLOGY_ENTITIES)}")
print(f"  Lakehouse columns: {len(COLUMNS)}")

# ─── Deploy ────────────────────────────────────────────────────────────
token = get_token()
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# Check if agent already exists
print(f"\nChecking for existing agent '{AGENT_NAME}'...")
items = requests.get(f"{API}/workspaces/{WORKSPACE_ID}/items", headers=headers).json().get("value", [])
existing = [i for i in items if i["type"] == "DataAgent" and i["displayName"] == AGENT_NAME]

if existing:
    agent_id = existing[0]["id"]
    print(f"  Found existing agent: {agent_id} — updating definition...")
    r = requests.post(
        f"{API}/workspaces/{WORKSPACE_ID}/items/{agent_id}/updateDefinition",
        headers=headers,
        json={"definition": {"parts": parts}},
    )
else:
    print("  Creating new DataAgent item...")
    r = requests.post(
        f"{API}/workspaces/{WORKSPACE_ID}/items",
        headers=headers,
        json={
            "displayName": AGENT_NAME,
            "type": "DataAgent",
            "description": "Randstad Risk Intelligence Agent — conversational AI for C-level decision support",
            "definition": {"parts": parts},
        },
    )

print(f"Status: {r.status_code}")

if r.status_code == 201:
    agent_id = r.json()["id"]
    print(f"✓ Created agent: {agent_id}")
elif r.status_code == 200:
    print("✓ Updated successfully!")
elif r.status_code == 202:
    op_id = r.headers.get("x-ms-operation-id", "")
    for i in range(30):
        time.sleep(3)
        poll = requests.get(f"{API}/operations/{op_id}", headers=headers)
        st = poll.json().get("status", "")
        print(f"  Poll {i}: {st}")
        if st == "Succeeded":
            # Try to get the result for creation
            try:
                result = requests.get(f"{API}/operations/{op_id}/result", headers=headers)
                if result.status_code == 200:
                    agent_id = result.json().get("id", agent_id if existing else "unknown")
            except Exception:
                pass
            print("✓ Agent deployed successfully!")
            break
        if st == "Failed":
            err = poll.json().get("error", {})
            print(f"✗ Error: {err.get('message', '')[:800]}")
            break
else:
    print(f"✗ Response: {r.text[:800]}")

# ─── Verify ────────────────────────────────────────────────────────────
print("\nVerifying...")
items2 = requests.get(f"{API}/workspaces/{WORKSPACE_ID}/items", headers=headers).json().get("value", [])
agent_items = [i for i in items2 if i["type"] == "DataAgent" and i["displayName"] == AGENT_NAME]
if agent_items:
    aid = agent_items[0]["id"]
    print(f"  ✓ Agent exists: {aid}")
    print(f"\n{'='*60}")
    print(f"  Randstad Risk Intelligence Agent — DEPLOYED")
    print(f"{'='*60}")
    print(f"  Open: https://app.fabric.microsoft.com/groups/{WORKSPACE_ID}/dataagents/{aid}")
else:
    print("  ✗ Agent not found in workspace items")

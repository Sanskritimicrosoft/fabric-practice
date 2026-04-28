#!/usr/bin/env python3
"""Deploy the enhanced Randstad Operating Intelligence Ontology.

9 entity types, 12 relationships — a business-ready knowledge graph
bound to the randstad_demo_120_rows lakehouse table.

Entity model:
  Client ──hasRiskProfile──► RiskProfile ──recommendsAction──► Action
    │  └──servedByBranchMarket──► BranchMarket
    ├──hasAlerts──► Alert ──triggeredByRiskProfile──► RiskProfile
    ├──hasInteractions──► Interaction ──influencesRiskProfile──► RiskProfile
    └──hasJobs──► Job ──requiresSkill──► Skill
                    ▲                       ▲
          Candidate ┘ matchesJob    hasSkill ┘

Suggested C-level questions this ontology supports:
  • Which clients have the highest margin at risk and why?
  • Which high-risk clients have open jobs and negative sentiment?
  • Which skills are causing staffing bottlenecks?
  • Which account owners should act first this week?
  • What action is recommended for each high-risk client?
"""

import subprocess, requests, json, base64, time, uuid, random

WORKSPACE_ID = "33ae4c2c-e78c-44be-81f9-911aec35cb56"
ONTOLOGY_ID = "2f5d56c3-16b3-470a-a008-0fa06ed19a4b"
LAKEHOUSE_ID = "3a63410e-9dba-4a4f-8fa1-c0b419a3342d"
TABLE = "randstad_demo_120_rows"
API = "https://api.fabric.microsoft.com/v1"


def get_token():
    return subprocess.run(
        'az account get-access-token --resource "https://api.fabric.microsoft.com" --query accessToken -o tsv',
        capture_output=True, text=True, shell=True
    ).stdout.strip()


def b64(obj):
    return base64.b64encode(json.dumps(obj).encode()).decode()


def part(path, obj):
    return {"path": path, "payload": b64(obj), "payloadType": "InlineBase64"}


def gen_id():
    return str(random.randint(100000000000000, 999999999999999))


def gen_prop_id():
    return str(random.randint(5600000000000000000, 5699999999999999999))


# ─── Entity Definitions ────────────────────────────────────────────────
# pk_cols: columns that form the entity's identity (composite supported)
# col_map: optional override for data-binding source column name
#          (when ontology property name differs from lakehouse column)
entities = {
    "Client": {
        "desc": "A Randstad client account — the central business entity",
        "pk_cols": ["client_name"],
        "properties": [
            ("client_name", "String"),
            ("region", "String"),
            ("industry", "String"),
            ("account_owner", "String"),
            ("revenue", "Double"),
        ],
    },
    "RiskProfile": {
        "desc": "Quantified risk posture per client — score, level, margin exposure, and recommended action",
        "pk_cols": ["client_name", "risk_level"],  # composite: unique per client+level
        "properties": [
            ("client_name", "String"),
            ("risk_score", "Double"),
            ("risk_level", "String"),
            ("margin_at_risk", "Double"),
            ("main_issue", "String"),
            ("recommended_action", "String"),
        ],
    },
    "Job": {
        "desc": "An open or filled job order placed by a client",
        "pk_cols": ["job_id"],
        "properties": [
            ("job_id", "String"),
            ("client_name", "String"),
            ("role", "String"),
            ("required_skill", "String"),
            ("job_status", "String"),
            ("days_open", "BigInt"),
        ],
    },
    "Candidate": {
        "desc": "A talent candidate being matched to open roles",
        "pk_cols": ["candidate_id"],
        "properties": [
            ("candidate_id", "String"),
            ("candidate_name", "String"),
            ("candidate_skill", "String"),
            ("match_score", "Double"),
            ("availability", "String"),
        ],
    },
    "Alert": {
        "desc": "A business event or risk trigger requiring attention",
        "pk_cols": ["client_name", "event_type"],  # composite derived ID
        "properties": [
            ("client_name", "String"),
            ("event_time", "DateTime"),
            ("event_type", "String"),
            ("event_description", "String"),
            ("impact_level", "String"),
            ("recommended_action", "String"),
        ],
    },
    "Interaction": {
        "desc": "A client touchpoint — call, email, or meeting with sentiment tracking",
        "pk_cols": ["client_name", "interaction_type"],  # composite derived ID
        "properties": [
            ("client_name", "String"),
            ("interaction_date", "DateTime"),
            ("interaction_type", "String"),
            ("interaction_issue", "String"),
            ("sentiment", "String"),
        ],
    },
    "Skill": {
        "desc": "A workforce skill tracked across jobs and candidates",
        "pk_cols": ["skill_name"],
        "properties": [
            ("skill_name", "String"),
        ],
        "col_map": {"skill_name": "required_skill"},
        # Multiple table columns can identify this entity
        "alt_cols": {"candidate_skill": "skill_name"},
    },
    "Action": {
        "desc": "A recommended business action to mitigate client risk",
        "pk_cols": ["action_name"],
        "properties": [
            ("action_name", "String"),
        ],
        "col_map": {"action_name": "recommended_action"},
    },
    "BranchMarket": {
        "desc": "A regional market or branch office serving clients",
        "pk_cols": ["market_region"],
        "properties": [
            ("market_region", "String"),
        ],
        "col_map": {"market_region": "region"},
    },
}

# ─── Generate IDs ──────────────────────────────────────────────────────
entity_ids = {}
prop_ids = {}

for ename, edef in entities.items():
    entity_ids[ename] = gen_id()
    for pname, ptype in edef["properties"]:
        prop_ids[(ename, pname)] = gen_prop_id()

# ─── Relationships ─────────────────────────────────────────────────────
# (name, description, source, target, src_join_col, tgt_join_col)
# src_join_col / tgt_join_col = lakehouse column used in the contextualization
# They must map to properties that are in the respective entity's entityIdParts.
relationships = [
    # Original client-centric relationships
    ("hasRiskProfile",
     "Client carries a quantified risk profile",
     "Client", "RiskProfile", "client_name", "client_name"),
    ("hasJobs",
     "Client has placed job orders",
     "Client", "Job", "client_name", "job_id"),
    ("hasAlerts",
     "Client has triggered business alerts",
     "Client", "Alert", "client_name", "client_name"),
    ("hasInteractions",
     "Client has recorded interactions",
     "Client", "Interaction", "client_name", "client_name"),

    # New business relationships
    ("belongsToClient",
     "Job order belongs to a specific client account",
     "Job", "Client", "job_id", "client_name"),
    ("requiresSkill",
     "Job order requires a specific workforce skill",
     "Job", "Skill", "job_id", "required_skill"),
    ("hasSkill",
     "Candidate possesses a workforce skill",
     "Candidate", "Skill", "candidate_id", "candidate_skill"),
    ("matchesJob",
     "Candidate is matched to a job order",
     "Candidate", "Job", "candidate_id", "job_id"),
    ("influencesRiskProfile",
     "Client interaction influences the risk assessment",
     "Interaction", "RiskProfile", "client_name", "client_name"),
    ("triggeredByRiskProfile",
     "Alert was triggered by a risk condition",
     "Alert", "RiskProfile", "client_name", "client_name"),
    ("recommendsAction",
     "Risk profile recommends a specific mitigation action",
     "RiskProfile", "Action", "client_name", "recommended_action"),
    ("servedByBranchMarket",
     "Client is served by a regional branch market",
     "Client", "BranchMarket", "client_name", "region"),
]

# ─── Build Definition Parts ────────────────────────────────────────────
parts = [part("definition.json", {})]


def get_source_col(entity_name, prop_name):
    """Get the actual lakehouse column name (handles col_map overrides)."""
    col_map = entities[entity_name].get("col_map", {})
    return col_map.get(prop_name, prop_name)


# Entity Types + Data Bindings
for ename, edef in entities.items():
    eid = entity_ids[ename]
    pk_prop_ids = [prop_ids[(ename, pn)] for pn in edef["pk_cols"]]

    entity_def = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/ontology/entityType/1.0.0/schema.json",
        "id": eid,
        "namespace": "usertypes",
        "baseEntityTypeId": None,
        "name": ename,
        "entityIdParts": pk_prop_ids,
        "displayNamePropertyId": None,
        "namespaceType": "Custom",
        "visibility": "Visible",
        "properties": [
            {
                "id": prop_ids[(ename, pname)],
                "name": pname,
                "redefines": None,
                "baseTypeNamespaceType": None,
                "valueType": ptype,
            }
            for pname, ptype in edef["properties"]
        ],
        "timeseriesProperties": [],
    }
    parts.append(part(f"EntityTypes/{eid}/definition.json", entity_def))

    binding_id = str(uuid.uuid4())
    binding = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/ontology/dataBinding/1.0.0/schema.json",
        "id": binding_id,
        "dataBindingConfiguration": {
            "dataBindingType": "NonTimeSeries",
            "propertyBindings": [
                {
                    "sourceColumnName": get_source_col(ename, pname),
                    "targetPropertyId": prop_ids[(ename, pname)],
                }
                for pname, _ in edef["properties"]
            ],
            "sourceTableProperties": {
                "sourceType": "LakehouseTable",
                "workspaceId": WORKSPACE_ID,
                "itemId": LAKEHOUSE_ID,
                "sourceTableName": TABLE,
                "sourceSchema": "dbo",
            },
        },
    }
    parts.append(part(f"EntityTypes/{eid}/DataBindings/{binding_id}.json", binding))

def build_key_bindings(entity_name, join_cols_override=None):
    """Build key ref bindings for ALL entityIdParts of an entity.
    
    join_cols_override: dict mapping property_name -> table_column_name
    for cases where the join column differs from the data binding column.
    """
    edef = entities[entity_name]
    bindings = []
    for pk_prop in edef["pk_cols"]:
        if join_cols_override and pk_prop in join_cols_override:
            col_name = join_cols_override[pk_prop]
        else:
            col_name = get_source_col(entity_name, pk_prop)
        bindings.append({
            "sourceColumnName": col_name,
            "targetPropertyId": prop_ids[(entity_name, pk_prop)],
        })
    return bindings


# Relationship Types + Contextualizations
# Format: (name, desc, src_entity, tgt_entity, src_col_overrides, tgt_col_overrides)
# Overrides: dict of {property_name: table_column_name} for non-default join columns
for rname, rdesc, src_ent, tgt_ent, src_join, tgt_join in relationships:
    rid = gen_id()

    # Find the property ID for the join column (must be in entityIdParts)
    # For source: the src_join column identifies the source entity
    # For target: the tgt_join column identifies the target entity
    # Must cover ALL PK parts for composite keys
    src_col_overrides = {}
    tgt_col_overrides = {}

    # If src_join is a specific column, map the first PK prop to it
    src_pk0 = entities[src_ent]["pk_cols"][0]
    src_col_overrides[src_pk0] = src_join

    # For target, map first PK prop to the join column
    tgt_pk0 = entities[tgt_ent]["pk_cols"][0]
    # Handle alt_cols: if tgt_join is an alt column, map it to the PK prop it represents
    alt_map = entities[tgt_ent].get("alt_cols", {})
    col_map = entities[tgt_ent].get("col_map", {})
    col_map_inv = {v: k for k, v in col_map.items()}
    if tgt_join in alt_map:
        tgt_col_overrides[alt_map[tgt_join]] = tgt_join
    elif tgt_join in col_map_inv:
        tgt_col_overrides[col_map_inv[tgt_join]] = tgt_join
    else:
        tgt_col_overrides[tgt_pk0] = tgt_join

    rel_def = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/ontology/relationshipType/1.0.0/schema.json",
        "namespace": "usertypes",
        "id": rid,
        "name": rname,
        "namespaceType": "Custom",
        "source": {"entityTypeId": entity_ids[src_ent]},
        "target": {"entityTypeId": entity_ids[tgt_ent]},
    }
    parts.append(part(f"RelationshipTypes/{rid}/definition.json", rel_def))

    ctx_id = str(uuid.uuid4())
    ctx = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/ontology/contextualization/1.0.0/schema.json",
        "id": ctx_id,
        "dataBindingTable": {
            "workspaceId": WORKSPACE_ID,
            "itemId": LAKEHOUSE_ID,
            "sourceTableName": TABLE,
            "sourceSchema": "dbo",
            "sourceType": "LakehouseTable",
        },
        "sourceKeyRefBindings": build_key_bindings(src_ent, src_col_overrides),
        "targetKeyRefBindings": build_key_bindings(tgt_ent, tgt_col_overrides),
    }
    parts.append(part(f"RelationshipTypes/{rid}/Contextualizations/{ctx_id}.json", ctx))

# .platform metadata
ONTOLOGY_DESC = (
    "Randstad Operating Intelligence Ontology — a business knowledge graph "
    "modeling client risk, workforce supply, talent matching, and operational alerts. "
    "Designed for C-level decision support.\n\n"
    "Suggested questions:\n"
    "• Which clients have the highest margin at risk and why?\n"
    "• Which high-risk clients have open jobs and negative sentiment?\n"
    "• Which skills are causing staffing bottlenecks?\n"
    "• Which account owners should act first this week?\n"
    "• What action is recommended for each high-risk client?"
)
parts.append(part(".platform", {
    "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
    "metadata": {
        "type": "Ontology",
        "displayName": "Randstad_Risk_Ontology",
        "description": ONTOLOGY_DESC,
    },
    "config": {
        "version": "2.0",
        "logicalId": "00000000-0000-0000-0000-000000000000",
    },
}))

# ─── Deploy ────────────────────────────────────────────────────────────
print(f"Built {len(parts)} parts:")
print(f"  {len(entities)} entity types")
print(f"  {len(relationships)} relationship types")
for ename, edef in entities.items():
    pk_display = "+".join(edef["pk_cols"])
    print(f"    {ename} (PK: {pk_display}, {len(edef['properties'])} props) — {edef['desc']}")
print(f"  Relationships:")
for rname, rdesc, src, tgt, _, _ in relationships:
    print(f"    {src} ──{rname}──► {tgt}")

token = get_token()
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

print("\nUpdating ontology definition...")
r = requests.post(
    f"{API}/workspaces/{WORKSPACE_ID}/items/{ONTOLOGY_ID}/updateDefinition",
    headers=headers,
    json={"definition": {"parts": parts}},
)
print(f"Status: {r.status_code}")

if r.status_code == 200:
    print("✓ Updated successfully!")
elif r.status_code == 202:
    op_id = r.headers.get("x-ms-operation-id", "")
    for i in range(30):
        time.sleep(3)
        poll = requests.get(f"{API}/operations/{op_id}", headers=headers)
        st = poll.json().get("status", "")
        print(f"  Poll {i}: {st}")
        if st == "Succeeded":
            print("✓ Updated successfully!")
            break
        if st == "Failed":
            err = poll.json().get("error", {})
            print(f"  ✗ Error: {err.get('message', '')[:800]}")
            break
else:
    print(f"✗ Response: {r.text[:500]}")

# ─── Verify ────────────────────────────────────────────────────────────
print("\nVerifying stored definition...")
r2 = requests.post(
    f"{API}/workspaces/{WORKSPACE_ID}/items/{ONTOLOGY_ID}/getDefinition",
    headers=headers,
)
if r2.status_code == 202:
    op_id = r2.headers.get("x-ms-operation-id", "")
    for i in range(15):
        time.sleep(3)
        poll = requests.get(f"{API}/operations/{op_id}", headers=headers)
        if poll.json().get("status") == "Succeeded":
            result = requests.get(f"{API}/operations/{op_id}/result", headers=headers)
            stored = result.json().get("definition", {}).get("parts", [])
            ent_defs = [p for p in stored if "EntityTypes" in p["path"] and p["path"].endswith("definition.json")]
            rel_defs = [p for p in stored if "RelationshipTypes" in p["path"] and p["path"].endswith("definition.json")]
            bindings = [p for p in stored if "DataBindings" in p["path"]]
            ctxs = [p for p in stored if "Contextualizations" in p["path"]]
            print(f"  ✓ {len(ent_defs)} entities, {len(bindings)} bindings, {len(rel_defs)} relationships, {len(ctxs)} contextualizations")
            for ep in ent_defs:
                c = json.loads(base64.b64decode(ep["payload"]))
                nprops = len(c.get("properties", []))
                npk = len(c.get("entityIdParts", []))
                print(f"    {c['name']:20s}  {nprops} props, {npk} PK parts")
            for rp in rel_defs:
                c = json.loads(base64.b64decode(rp["payload"]))
                print(f"    {c['name']}")
            break

print(f"\n{'='*60}")
print(f"Randstad Operating Intelligence Ontology — DEPLOYED")
print(f"{'='*60}")
print(f"URL: https://app.fabric.microsoft.com/groups/{WORKSPACE_ID}/ontologies/{ONTOLOGY_ID}")

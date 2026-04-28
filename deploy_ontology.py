#!/usr/bin/env python3
"""Populate Randstad_Risk_Ontology with proper Fabric ontology format.

Based on the RetailSalesOntology format:
- EntityTypes/{id}/definition.json  (entity schema)
- EntityTypes/{id}/DataBindings/{uuid}.json  (lakehouse binding)
- RelationshipTypes/{id}/definition.json  (relationship)
- RelationshipTypes/{id}/Contextualizations/{uuid}.json  (join context)
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
    """Generate a numeric ID similar to the Fabric format."""
    return str(random.randint(100000000000000, 999999999999999))

def gen_prop_id():
    return str(random.randint(5600000000000000000, 5699999999999999999))

# Define entities with their properties
entities = {
    "Client": {
        "pk_cols": ["client_name"],
        "properties": [
            ("client_name", "String"),
            ("region", "String"),
            ("industry", "String"),
            ("account_owner", "String"),
            ("revenue", "Double"),
        ]
    },
    "RiskProfile": {
        "pk_cols": ["client_name"],
        "properties": [
            ("client_name", "String"),
            ("risk_score", "Double"),
            ("risk_level", "String"),
            ("margin_at_risk", "Double"),
            ("main_issue", "String"),
        ]
    },
    "Job": {
        "pk_cols": ["job_id"],
        "properties": [
            ("job_id", "String"),
            ("client_name", "String"),
            ("role", "String"),
            ("required_skill", "String"),
            ("job_status", "String"),
            ("days_open", "BigInt"),
        ]
    },
    "Candidate": {
        "pk_cols": ["candidate_id"],
        "properties": [
            ("candidate_id", "String"),
            ("candidate_name", "String"),
            ("candidate_skill", "String"),
            ("match_score", "Double"),
            ("availability", "String"),
        ]
    },
    "Alert": {
        "pk_cols": ["client_name"],
        "properties": [
            ("client_name", "String"),
            ("event_time", "DateTime"),
            ("event_type", "String"),
            ("event_description", "String"),
            ("impact_level", "String"),
            ("recommended_action", "String"),
        ]
    },
    "Interaction": {
        "pk_cols": ["client_name"],
        "properties": [
            ("client_name", "String"),
            ("interaction_date", "DateTime"),
            ("interaction_type", "String"),
            ("interaction_issue", "String"),
            ("sentiment", "String"),
        ]
    },
}

# Generate IDs
entity_ids = {}
prop_ids = {}  # (entity_name, prop_name) -> id

for ename, edef in entities.items():
    eid = gen_id()
    entity_ids[ename] = eid
    for pname, ptype in edef["properties"]:
        pid = gen_prop_id()
        prop_ids[(ename, pname)] = pid

# Relationships: (name, source_entity, target_entity)
# Join is always through the shared table. Source uses Client PK (client_name).
# Target uses the target entity's PK column.
relationships = [
    ("hasRiskProfile", "Client", "RiskProfile"),
    ("hasJobs", "Client", "Job"),
    ("hasAlerts", "Client", "Alert"),
    ("hasInteractions", "Client", "Interaction"),
]

# Build parts
parts = []

# 1. definition.json (empty, as in the reference)
parts.append(part("definition.json", {}))

# 2. EntityTypes
for ename, edef in entities.items():
    eid = entity_ids[ename]
    pk_prop_ids = [prop_ids[(ename, pn)] for pn in edef["pk_cols"]]

    # Entity definition
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

    # Data binding
    binding_id = str(uuid.uuid4())
    binding = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/ontology/dataBinding/1.0.0/schema.json",
        "id": binding_id,
        "dataBindingConfiguration": {
            "dataBindingType": "NonTimeSeries",
            "propertyBindings": [
                {
                    "sourceColumnName": pname,
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

# 3. RelationshipTypes
for rname, src_entity, tgt_entity in relationships:
    rid = gen_id()
    rel_def = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/ontology/relationshipType/1.0.0/schema.json",
        "namespace": "usertypes",
        "id": rid,
        "name": rname,
        "namespaceType": "Custom",
        "source": {"entityTypeId": entity_ids[src_entity]},
        "target": {"entityTypeId": entity_ids[tgt_entity]},
    }
    parts.append(part(f"RelationshipTypes/{rid}/definition.json", rel_def))

    # Contextualization — join through the shared lakehouse table
    # Source binding: client_name → Client's client_name prop (in Client's entityIdParts)
    # Target binding: target PK column → target's PK prop (in target's entityIdParts)
    src_pk_col = entities[src_entity]["pk_cols"][0]
    tgt_pk_col = entities[tgt_entity]["pk_cols"][0]
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
        "sourceKeyRefBindings": [
            {
                "sourceColumnName": src_pk_col,
                "targetPropertyId": prop_ids[(src_entity, src_pk_col)],
            }
        ],
        "targetKeyRefBindings": [
            {
                "sourceColumnName": tgt_pk_col,
                "targetPropertyId": prop_ids[(tgt_entity, tgt_pk_col)],
            }
        ],
    }
    parts.append(part(f"RelationshipTypes/{rid}/Contextualizations/{ctx_id}.json", ctx))

# 4. .platform
parts.append(part(".platform", {
    "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
    "metadata": {
        "type": "Ontology",
        "displayName": "Randstad_Risk_Ontology",
        "description": "Business ontology for Randstad client risk intelligence",
    },
    "config": {
        "version": "2.0",
        "logicalId": "00000000-0000-0000-0000-000000000000",
    },
}))

print(f"Built {len(parts)} parts:")
print(f"  {len(entities)} entity types")
print(f"  {len(relationships)} relationship types")
print(f"  Entity: {list(entities.keys())}")
print(f"  Relationships: {[r[0] for r in relationships]}")

# Update definition
token = get_token()
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

print("\nUpdating ontology definition...")
r = requests.post(
    f"{API}/workspaces/{WORKSPACE_ID}/items/{ONTOLOGY_ID}/updateDefinition",
    headers=headers,
    json={"definition": {"parts": parts}}
)
print(f"Status: {r.status_code}")

if r.status_code == 200:
    print("Updated successfully!")
elif r.status_code == 202:
    op_id = r.headers.get("x-ms-operation-id", "")
    for i in range(20):
        time.sleep(3)
        poll = requests.get(f"{API}/operations/{op_id}", headers=headers)
        st = poll.json().get("status", "")
        print(f"  Poll {i}: {st}")
        if st == "Succeeded":
            print("Updated successfully!")
            break
        if st == "Failed":
            print(f"  Error: {poll.json().get('error', {}).get('message', '')[:500]}")
            break
else:
    print(f"Response: {r.text[:500]}")

# Verify
print("\nVerifying...")
r2 = requests.post(f"{API}/workspaces/{WORKSPACE_ID}/items/{ONTOLOGY_ID}/getDefinition", headers=headers)
if r2.status_code == 202:
    op_id = r2.headers.get("x-ms-operation-id", "")
    for i in range(10):
        time.sleep(3)
        poll = requests.get(f"{API}/operations/{op_id}", headers=headers)
        if poll.json().get("status") == "Succeeded":
            result = requests.get(f"{API}/operations/{op_id}/result", headers=headers)
            stored_parts = result.json().get("definition", {}).get("parts", [])
            entity_parts = [p for p in stored_parts if "EntityTypes" in p["path"] and p["path"].endswith("definition.json")]
            rel_parts = [p for p in stored_parts if "RelationshipTypes" in p["path"] and p["path"].endswith("definition.json")]
            binding_parts = [p for p in stored_parts if "DataBindings" in p["path"]]
            ctx_parts = [p for p in stored_parts if "Contextualizations" in p["path"]]
            print(f"  Entity definitions: {len(entity_parts)}")
            print(f"  Data bindings: {len(binding_parts)}")
            print(f"  Relationship definitions: {len(rel_parts)}")
            print(f"  Contextualizations: {len(ctx_parts)}")
            for ep in entity_parts:
                content = json.loads(base64.b64decode(ep["payload"]))
                print(f"    Entity: {content.get('name')} ({len(content.get('properties', []))} props)")
            break

print(f"\n=== Ontology Complete ===")
print(f"URL: https://app.fabric.microsoft.com/groups/{WORKSPACE_ID}/ontologies/{ONTOLOGY_ID}")

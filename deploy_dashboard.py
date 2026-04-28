"""
Randstad Executive Dashboard - Fabric Deployment Script
Creates a Semantic Model + 4-page Power BI Report in Microsoft Fabric
"""

import json
import base64
import subprocess
import requests
import uuid
import time

# ── Configuration ──────────────────────────────────────────────────────────────
WORKSPACE_ID = "33ae4c2c-e78c-44be-81f9-911aec35cb56"
LAKEHOUSE_ID = "3a63410e-9dba-4a4f-8fa1-c0b419a3342d"
SQL_ENDPOINT_CONN = "7zy6lned6lkulpbvjqg6klyvwu-frgk4m4m467ejapzsenoynolky.datawarehouse.fabric.microsoft.com"
SQL_ENDPOINT_ID = "d8c4d0e2-3e83-4396-9079-20947dad59c9"
TABLE_NAME = "randstad_demo_120_rows"
SEMANTIC_MODEL_NAME = "Randstad Executive Intelligence"
REPORT_NAME = "Randstad Executive Dashboard"

API_BASE = "https://api.fabric.microsoft.com/v1"


def get_token():
    import os
    # Try environment variable first (set by wrapper script)
    token = os.environ.get("FABRIC_TOKEN", "")
    if token:
        return token
    # Try az CLI with shell=True for Windows PATH resolution
    result = subprocess.run(
        'az account get-access-token --resource "https://api.fabric.microsoft.com" --query accessToken -o tsv',
        capture_output=True, text=True, shell=True
    )
    return result.stdout.strip()


def api_headers(token):
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def b64(obj):
    """Base64 encode a dict or string."""
    s = json.dumps(obj) if isinstance(obj, dict) else obj
    return base64.b64encode(s.encode("utf-8")).decode("utf-8")


def uid():
    return str(uuid.uuid4())


# ── Semantic Model (TMSL) ─────────────────────────────────────────────────────

def build_model_bim():
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
            "lineageTag": uid(),
            "summarizeBy": "none" if dtype == "string" else ("none" if dtype == "dateTime" else "sum"),
        })

    measures = [
        ("Total Margin at Risk", f"SUM('{TABLE_NAME}'[margin_at_risk])", "#,##0"),
        ("Average Risk Score", f"AVERAGE('{TABLE_NAME}'[risk_score])", "0.0"),
        ("High Risk Clients", f"CALCULATE(DISTINCTCOUNT('{TABLE_NAME}'[client_name]), '{TABLE_NAME}'[risk_level] = \"High\")", "0"),
        ("Open Job Orders", f"CALCULATE(DISTINCTCOUNT('{TABLE_NAME}'[job_id]), '{TABLE_NAME}'[job_status] = \"Open\")", "0"),
        ("Negative Interactions", f"CALCULATE(COUNTROWS('{TABLE_NAME}'), '{TABLE_NAME}'[sentiment] = \"Negative\")", "0"),
        ("Available Candidates", f"CALCULATE(DISTINCTCOUNT('{TABLE_NAME}'[candidate_id]), '{TABLE_NAME}'[availability] = \"Available\")", "0"),
        ("High Impact Alerts", f"CALCULATE(COUNTROWS('{TABLE_NAME}'), '{TABLE_NAME}'[impact_level] = \"High\")", "0"),
        ("Average Days Open", f"AVERAGE('{TABLE_NAME}'[days_open])", "0.0"),
        ("Average Match Score", f"AVERAGE('{TABLE_NAME}'[match_score])", "0.0"),
    ]

    measure_defs = []
    for name, expr, fmt in measures:
        measure_defs.append({
            "name": name,
            "expression": expr,
            "formatString": fmt,
            "lineageTag": uid(),
        })

    model = {
        "compatibilityLevel": 1604,
        "model": {
            "culture": "en-US",
            "defaultPowerBIDataSourceVersion": "powerBI_V3",
            "defaultMode": "directLake",
            "sourceQueryCulture": "en-US",
            "annotations": [
                {
                    "name": "__PBI_TimeIntelligenceEnabled",
                    "value": "0"
                }
            ],
            "tables": [
                {
                    "name": TABLE_NAME,
                    "lineageTag": uid(),
                    "columns": col_defs,
                    "measures": measure_defs,
                    "partitions": [
                        {
                            "name": "partition",
                            "mode": "directLake",
                            "source": {
                                "type": "entity",
                                "entityName": TABLE_NAME,
                                "schemaName": "dbo",
                                "expressionSource": "DatabaseQuery"
                            }
                        }
                    ]
                }
            ],
            "expressions": [
                {
                    "name": "DatabaseQuery",
                    "kind": "m",
                    "expression": (
                        "let\n"
                        f'    database = Sql.Database("{SQL_ENDPOINT_CONN}", "{SQL_ENDPOINT_ID}")\n'
                        "in\n"
                        "    database"
                    )
                }
            ]
        }
    }
    return model


def build_pbism():
    return {
        "version": "1.0",
        "settings": {}
    }


# ── Report (PBIR) ─────────────────────────────────────────────────────────────

# Randstad brand colors
COLORS = {
    "primary": "#003A70",       # Randstad dark blue
    "secondary": "#0063B2",     # Lighter blue
    "accent": "#00A3E0",        # Bright blue
    "high_risk": "#D13438",     # Red
    "medium_risk": "#FF8C00",   # Orange/amber
    "low_risk": "#107C10",      # Green
    "bg": "#F5F5F5",
    "card_bg": "#FFFFFF",
    "text": "#333333",
    "light_text": "#666666",
}

# Visual type constants
CARD = "card"
BAR = "clusteredBarChart"
COLUMN = "clusteredColumnChart"
DONUT = "donutChart"
LINE = "lineChart"
TABLE = "tableEx"
SLICER = "slicer"
SCATTER = "scatterChart"
TEXT = "textbox"


def vc(name, x, y, w, h, config_obj, filters="[]"):
    """Create a visual container."""
    return {
        "x": x, "y": y, "z": 0, "width": w, "height": h,
        "config": json.dumps(config_obj),
        "filters": filters,
        "tabOrder": 0
    }


def card_config(name, measure_name, title_text, color=None):
    cfg = {
        "name": name,
        "layouts": [{"id": 0, "position": {"x": 0, "y": 0, "width": 100, "height": 100}}],
        "singleVisual": {
            "visualType": CARD,
            "projections": {
                "Values": [{"queryRef": f"{TABLE_NAME}.{measure_name}"}]
            },
            "objects": {
                "labels": [{"properties": {
                    "fontSize": {"expr": {"Literal": {"Value": "28D"}}},
                    "color": {"solid": {"color": {"expr": {"Literal": {"Value": f"'{color or COLORS['primary']}'"}}}}}
                }}],
                "categoryLabels": [{"properties": {
                    "show": {"expr": {"Literal": {"Value": "false"}}}
                }}]
            },
            "vcObjects": {
                "title": [{"properties": {
                    "text": {"expr": {"Literal": {"Value": f"'{title_text}'"}}},
                    "show": {"expr": {"Literal": {"Value": "true"}}},
                    "fontSize": {"expr": {"Literal": {"Value": "11D"}}},
                    "fontColor": {"solid": {"color": {"expr": {"Literal": {"Value": f"'{COLORS['light_text']}'"}}}}}
                }}],
                "background": [{"properties": {
                    "show": {"expr": {"Literal": {"Value": "true"}}},
                    "color": {"solid": {"color": {"expr": {"Literal": {"Value": f"'{COLORS['card_bg']}'"}}}}}
                }}],
                "border": [{"properties": {
                    "show": {"expr": {"Literal": {"Value": "true"}}},
                    "color": {"solid": {"color": {"expr": {"Literal": {"Value": "'#E0E0E0'"}}}}},
                    "radius": {"expr": {"Literal": {"Value": "8D"}}}
                }}]
            }
        }
    }
    return cfg


def bar_config(name, category_col, value_col_or_measure, title_text, is_measure=False):
    val_ref = f"{TABLE_NAME}.{value_col_or_measure}"
    cat_ref = f"{TABLE_NAME}.{category_col}"
    cfg = {
        "name": name,
        "layouts": [{"id": 0, "position": {"x": 0, "y": 0, "width": 100, "height": 100}}],
        "singleVisual": {
            "visualType": BAR,
            "projections": {
                "Category": [{"queryRef": cat_ref}],
                "Y": [{"queryRef": val_ref}]
            },
            "objects": {
                "dataPoint": [{"properties": {
                    "fill": {"solid": {"color": {"expr": {"Literal": {"Value": f"'{COLORS['primary']}'"}}}}}
                }}]
            },
            "vcObjects": {
                "title": [{"properties": {
                    "text": {"expr": {"Literal": {"Value": f"'{title_text}'"}}},
                    "show": {"expr": {"Literal": {"Value": "true"}}},
                    "fontSize": {"expr": {"Literal": {"Value": "12D"}}},
                    "fontColor": {"solid": {"color": {"expr": {"Literal": {"Value": f"'{COLORS['text']}'"}}}}}
                }}],
                "background": [{"properties": {
                    "show": {"expr": {"Literal": {"Value": "true"}}},
                    "color": {"solid": {"color": {"expr": {"Literal": {"Value": f"'{COLORS['card_bg']}'"}}}}}
                }}],
                "border": [{"properties": {
                    "show": {"expr": {"Literal": {"Value": "true"}}},
                    "color": {"solid": {"color": {"expr": {"Literal": {"Value": "'#E0E0E0'"}}}}},
                    "radius": {"expr": {"Literal": {"Value": "6D"}}}
                }}]
            }
        }
    }
    return cfg


def column_config(name, category_col, value_col, title_text):
    cfg = bar_config(name, category_col, value_col, title_text)
    cfg["singleVisual"]["visualType"] = COLUMN
    return cfg


def donut_config(name, category_col, value_col_or_measure, title_text):
    cat_ref = f"{TABLE_NAME}.{category_col}"
    val_ref = f"{TABLE_NAME}.{value_col_or_measure}"
    cfg = {
        "name": name,
        "layouts": [{"id": 0, "position": {"x": 0, "y": 0, "width": 100, "height": 100}}],
        "singleVisual": {
            "visualType": DONUT,
            "projections": {
                "Category": [{"queryRef": cat_ref}],
                "Y": [{"queryRef": val_ref}]
            },
            "objects": {},
            "vcObjects": {
                "title": [{"properties": {
                    "text": {"expr": {"Literal": {"Value": f"'{title_text}'"}}},
                    "show": {"expr": {"Literal": {"Value": "true"}}},
                    "fontSize": {"expr": {"Literal": {"Value": "12D"}}}
                }}],
                "background": [{"properties": {
                    "show": {"expr": {"Literal": {"Value": "true"}}},
                    "color": {"solid": {"color": {"expr": {"Literal": {"Value": f"'{COLORS['card_bg']}'"}}}}}
                }}],
                "border": [{"properties": {
                    "show": {"expr": {"Literal": {"Value": "true"}}},
                    "color": {"solid": {"color": {"expr": {"Literal": {"Value": "'#E0E0E0'"}}}}},
                    "radius": {"expr": {"Literal": {"Value": "6D"}}}
                }}]
            }
        }
    }
    return cfg


def table_config(name, columns, title_text):
    projections = {"Values": [{"queryRef": f"{TABLE_NAME}.{c}"} for c in columns]}
    cfg = {
        "name": name,
        "layouts": [{"id": 0, "position": {"x": 0, "y": 0, "width": 100, "height": 100}}],
        "singleVisual": {
            "visualType": TABLE,
            "projections": projections,
            "objects": {
                "grid": [{"properties": {
                    "gridVertical": {"expr": {"Literal": {"Value": "true"}}},
                    "gridVerticalColor": {"solid": {"color": {"expr": {"Literal": {"Value": "'#F0F0F0'"}}}}},
                    "rowPadding": {"expr": {"Literal": {"Value": "3D"}}}
                }}],
                "columnHeaders": [{"properties": {
                    "fontColor": {"solid": {"color": {"expr": {"Literal": {"Value": f"'{COLORS['card_bg']}'"}}}}}
                }},{"properties": {
                    "backColor": {"solid": {"color": {"expr": {"Literal": {"Value": f"'{COLORS['primary']}'"}}}}}
                }}]
            },
            "vcObjects": {
                "title": [{"properties": {
                    "text": {"expr": {"Literal": {"Value": f"'{title_text}'"}}},
                    "show": {"expr": {"Literal": {"Value": "true"}}},
                    "fontSize": {"expr": {"Literal": {"Value": "12D"}}}
                }}],
                "background": [{"properties": {
                    "show": {"expr": {"Literal": {"Value": "true"}}},
                    "color": {"solid": {"color": {"expr": {"Literal": {"Value": f"'{COLORS['card_bg']}'"}}}}}
                }}]
            }
        }
    }
    return cfg


def slicer_config(name, column, title_text):
    cfg = {
        "name": name,
        "layouts": [{"id": 0, "position": {"x": 0, "y": 0, "width": 100, "height": 100}}],
        "singleVisual": {
            "visualType": SLICER,
            "projections": {
                "Values": [{"queryRef": f"{TABLE_NAME}.{column}"}]
            },
            "objects": {
                "data": [{"properties": {
                    "mode": {"expr": {"Literal": {"Value": "'Dropdown'"}}}
                }}]
            },
            "vcObjects": {
                "title": [{"properties": {
                    "text": {"expr": {"Literal": {"Value": f"'{title_text}'"}}},
                    "show": {"expr": {"Literal": {"Value": "true"}}},
                    "fontSize": {"expr": {"Literal": {"Value": "10D"}}}
                }}]
            }
        }
    }
    return cfg


def line_config(name, date_col, value_col, legend_col, title_text):
    cfg = {
        "name": name,
        "layouts": [{"id": 0, "position": {"x": 0, "y": 0, "width": 100, "height": 100}}],
        "singleVisual": {
            "visualType": LINE,
            "projections": {
                "Category": [{"queryRef": f"{TABLE_NAME}.{date_col}"}],
                "Y": [{"queryRef": f"{TABLE_NAME}.{value_col}"}],
                "Series": [{"queryRef": f"{TABLE_NAME}.{legend_col}"}]
            },
            "objects": {},
            "vcObjects": {
                "title": [{"properties": {
                    "text": {"expr": {"Literal": {"Value": f"'{title_text}'"}}},
                    "show": {"expr": {"Literal": {"Value": "true"}}},
                    "fontSize": {"expr": {"Literal": {"Value": "12D"}}}
                }}],
                "background": [{"properties": {
                    "show": {"expr": {"Literal": {"Value": "true"}}},
                    "color": {"solid": {"color": {"expr": {"Literal": {"Value": f"'{COLORS['card_bg']}'"}}}}}
                }}],
                "border": [{"properties": {
                    "show": {"expr": {"Literal": {"Value": "true"}}},
                    "color": {"solid": {"color": {"expr": {"Literal": {"Value": "'#E0E0E0'"}}}}}
                }}]
            }
        }
    }
    return cfg


def scatter_config(name, x_col, y_col, size_col, cat_col, title_text):
    cfg = {
        "name": name,
        "layouts": [{"id": 0, "position": {"x": 0, "y": 0, "width": 100, "height": 100}}],
        "singleVisual": {
            "visualType": SCATTER,
            "projections": {
                "X": [{"queryRef": f"{TABLE_NAME}.{x_col}"}],
                "Y": [{"queryRef": f"{TABLE_NAME}.{y_col}"}],
                "Size": [{"queryRef": f"{TABLE_NAME}.{size_col}"}],
                "Category": [{"queryRef": f"{TABLE_NAME}.{cat_col}"}]
            },
            "objects": {},
            "vcObjects": {
                "title": [{"properties": {
                    "text": {"expr": {"Literal": {"Value": f"'{title_text}'"}}},
                    "show": {"expr": {"Literal": {"Value": "true"}}},
                    "fontSize": {"expr": {"Literal": {"Value": "12D"}}}
                }}],
                "background": [{"properties": {
                    "show": {"expr": {"Literal": {"Value": "true"}}},
                    "color": {"solid": {"color": {"expr": {"Literal": {"Value": f"'{COLORS['card_bg']}'"}}}}}
                }}]
            }
        }
    }
    return cfg


def textbox_config(name, text_content, font_size=14, bold=False):
    paragraphs = [{
        "textRuns": [{
            "value": text_content,
            "textStyle": {
                "fontSize": f"{font_size}px",
                "fontWeight": "bold" if bold else "normal",
                "color": COLORS["text"]
            }
        }]
    }]
    cfg = {
        "name": name,
        "layouts": [{"id": 0, "position": {"x": 0, "y": 0, "width": 100, "height": 100}}],
        "singleVisual": {
            "visualType": TEXT,
            "objects": {
                "general": [{"properties": {
                    "paragraphs": {"expr": {"Literal": {"Value": json.dumps(paragraphs)}}}
                }}]
            },
            "vcObjects": {
                "background": [{"properties": {
                    "show": {"expr": {"Literal": {"Value": "true"}}},
                    "color": {"solid": {"color": {"expr": {"Literal": {"Value": f"'{COLORS['card_bg']}'"}}}}}
                }}],
                "border": [{"properties": {
                    "show": {"expr": {"Literal": {"Value": "true"}}},
                    "color": {"solid": {"color": {"expr": {"Literal": {"Value": f"'{COLORS['primary']}'"}}}}}
                }}]
            }
        }
    }
    return cfg


def page_title_vc(name, title, subtitle, y=0):
    paragraphs = [
        {"textRuns": [{"value": title, "textStyle": {"fontSize": "22px", "fontWeight": "bold", "color": COLORS["primary"]}}]},
        {"textRuns": [{"value": subtitle, "textStyle": {"fontSize": "12px", "fontWeight": "normal", "color": COLORS["light_text"]}}]}
    ]
    cfg = {
        "name": name,
        "layouts": [{"id": 0, "position": {"x": 0, "y": 0, "width": 100, "height": 100}}],
        "singleVisual": {
            "visualType": TEXT,
            "objects": {
                "general": [{"properties": {
                    "paragraphs": {"expr": {"Literal": {"Value": json.dumps(paragraphs)}}}
                }}]
            },
            "vcObjects": {
                "background": [{"properties": {
                    "show": {"expr": {"Literal": {"Value": "true"}}},
                    "color": {"solid": {"color": {"expr": {"Literal": {"Value": f"'{COLORS['primary']}'"}}}}}
                }}]
            }
        }
    }
    return vc(name, 0, y, 1280, 60, cfg)


# ── Page builders ──────────────────────────────────────────────────────────────

def build_page1():
    """Executive Client Risk Cockpit"""
    visuals = []

    # Title bar
    visuals.append(page_title_vc("p1_title", "Executive Client Risk Cockpit",
                                  "Which clients are at risk and how much margin is exposed?"))

    # KPI row (4 cards)
    kw = 290
    kh = 120
    ky = 70
    gap = 20
    x_start = 20

    visuals.append(vc("p1_kpi1", x_start, ky, kw, kh,
                       card_config("p1_kpi1", "Total Margin at Risk", "Total Margin at Risk", COLORS["high_risk"])))
    visuals.append(vc("p1_kpi2", x_start + kw + gap, ky, kw, kh,
                       card_config("p1_kpi2", "Average Risk Score", "Avg Risk Score", COLORS["secondary"])))
    visuals.append(vc("p1_kpi3", x_start + 2 * (kw + gap), ky, kw, kh,
                       card_config("p1_kpi3", "High Risk Clients", "High Risk Clients", COLORS["high_risk"])))
    visuals.append(vc("p1_kpi4", x_start + 3 * (kw + gap), ky, kw, kh,
                       card_config("p1_kpi4", "Open Job Orders", "Open Job Orders", COLORS["accent"])))

    # Charts row
    cy = 200
    ch = 220
    cw = 420

    # Bar: Margin at Risk by Client
    visuals.append(vc("p1_bar1", 20, cy, cw, ch,
                       bar_config("p1_bar1", "client_name", "margin_at_risk", "Margin at Risk by Client")))

    # Bar: Avg Risk Score by Client
    visuals.append(vc("p1_bar2", 20 + cw + 15, cy, cw, ch,
                       bar_config("p1_bar2", "client_name", "risk_score", "Risk Score by Client")))

    # Donut: Risk Level distribution
    visuals.append(vc("p1_donut", 20 + 2 * (cw + 15), cy, 380, ch,
                       donut_config("p1_donut", "risk_level", "client_name", "Risk Level Distribution")))

    # Table
    visuals.append(vc("p1_table", 20, 430, 1060, 270,
                       table_config("p1_table",
                                    ["client_name", "region", "industry", "account_owner",
                                     "risk_score", "margin_at_risk", "main_issue", "recommended_action"],
                                    "Client Risk Detail")))

    # Slicers (right side)
    sx = 1100
    visuals.append(vc("p1_slicer1", sx, 430, 160, 80,
                       slicer_config("p1_slicer1", "region", "Region")))
    visuals.append(vc("p1_slicer2", sx, 520, 160, 80,
                       slicer_config("p1_slicer2", "industry", "Industry")))
    visuals.append(vc("p1_slicer3", sx, 610, 160, 80,
                       slicer_config("p1_slicer3", "risk_level", "Risk Level")))

    return {
        "name": "ReportSection1",
        "displayName": "Executive Risk Cockpit",
        "displayOption": 0,
        "width": 1280,
        "height": 720,
        "visualContainers": visuals,
        "config": json.dumps({
            "name": "ReportSection1",
            "displayName": "Executive Risk Cockpit",
            "ordinal": 0
        }),
        "filters": "[]"
    }


def build_page2():
    """Why Are Clients At Risk?"""
    visuals = []

    visuals.append(page_title_vc("p2_title", "Why Are Clients At Risk?",
                                  "Root cause analysis: issues, interactions, and sentiment driving risk"))

    # KPI row (2 cards)
    ky = 70
    visuals.append(vc("p2_kpi1", 20, ky, 290, 120,
                       card_config("p2_kpi1", "Negative Interactions", "Negative Interactions", COLORS["high_risk"])))
    visuals.append(vc("p2_kpi2", 330, ky, 290, 120,
                       card_config("p2_kpi2", "Average Days Open", "Avg Days Open", COLORS["secondary"])))

    # Slicers (top right)
    visuals.append(vc("p2_slicer1", 700, 70, 180, 55,
                       slicer_config("p2_slicer1", "client_name", "Client")))
    visuals.append(vc("p2_slicer2", 895, 70, 180, 55,
                       slicer_config("p2_slicer2", "sentiment", "Sentiment")))
    visuals.append(vc("p2_slicer3", 1090, 70, 170, 55,
                       slicer_config("p2_slicer3", "interaction_type", "Interaction Type")))

    # Slicers row 2
    visuals.append(vc("p2_slicer1b", 700, 130, 560, 55,
                       slicer_config("p2_slicer1b", "risk_level", "Risk Level")))

    # Charts
    cy = 200
    ch = 220

    # Bar: Count of issues by main_issue
    visuals.append(vc("p2_bar1", 20, cy, 400, ch,
                       bar_config("p2_bar1", "main_issue", "client_name", "Issues by Root Cause")))

    # Bar: Interaction issues by client_name
    visuals.append(vc("p2_bar2", 440, cy, 400, ch,
                       bar_config("p2_bar2", "client_name", "interaction_issue", "Interaction Issues by Client")))

    # Line: Interactions over time by sentiment
    visuals.append(vc("p2_line", 860, cy, 400, ch,
                       line_config("p2_line", "interaction_date", "client_name", "sentiment",
                                   "Interaction Trend by Sentiment")))

    # Table
    visuals.append(vc("p2_table", 20, 430, 1240, 280,
                       table_config("p2_table",
                                    ["client_name", "interaction_date", "interaction_type",
                                     "interaction_issue", "sentiment"],
                                    "Interaction Detail")))

    return {
        "name": "ReportSection2",
        "displayName": "Why At Risk?",
        "displayOption": 0,
        "width": 1280,
        "height": 720,
        "visualContainers": visuals,
        "config": json.dumps({
            "name": "ReportSection2",
            "displayName": "Why At Risk?",
            "ordinal": 1
        }),
        "filters": "[]"
    }


def build_page3():
    """Staffing & Talent Fulfilment View"""
    visuals = []

    visuals.append(page_title_vc("p3_title", "Staffing & Talent Fulfilment",
                                  "Operational staffing bottlenecks driving client risk"))

    # KPI row (3 cards)
    ky = 70
    kw = 290
    visuals.append(vc("p3_kpi1", 20, ky, kw, 120,
                       card_config("p3_kpi1", "Open Job Orders", "Open Roles", COLORS["high_risk"])))
    visuals.append(vc("p3_kpi2", 330, ky, kw, 120,
                       card_config("p3_kpi2", "Average Match Score", "Avg Candidate Match Score", COLORS["secondary"])))
    visuals.append(vc("p3_kpi3", 640, ky, kw, 120,
                       card_config("p3_kpi3", "Available Candidates", "Available Candidates", COLORS["low_risk"])))

    # Charts row 1
    cy = 200
    ch = 200
    cw = 400

    # Bar: Days Open by Client
    visuals.append(vc("p3_bar1", 20, cy, cw, ch,
                       bar_config("p3_bar1", "client_name", "days_open", "Days Open by Client")))

    # Column: Open vs Filled by job_status
    visuals.append(vc("p3_col1", 440, cy, 380, ch,
                       column_config("p3_col1", "job_status", "job_id", "Open vs Filled Job Orders")))

    # Bar: Required Skill demand
    visuals.append(vc("p3_bar2", 840, cy, 420, ch,
                       bar_config("p3_bar2", "required_skill", "job_id", "Skill Demand")))

    # Scatter: risk_score vs match_score
    visuals.append(vc("p3_scatter", 20, 410, 580, 300,
                       scatter_config("p3_scatter", "risk_score", "match_score", "margin_at_risk",
                                      "client_name", "Risk Score vs Match Score (size = Margin at Risk)")))

    # Table
    visuals.append(vc("p3_table", 620, 410, 640, 300,
                       table_config("p3_table",
                                    ["client_name", "job_id", "role", "required_skill",
                                     "job_status", "days_open", "candidate_name", "match_score", "availability"],
                                    "Staffing Detail")))

    return {
        "name": "ReportSection3",
        "displayName": "Staffing & Fulfilment",
        "displayOption": 0,
        "width": 1280,
        "height": 720,
        "visualContainers": visuals,
        "config": json.dumps({
            "name": "ReportSection3",
            "displayName": "Staffing & Fulfilment",
            "ordinal": 2
        }),
        "filters": "[]"
    }


def build_page4():
    """AI Action & Real-Time Alerts"""
    visuals = []

    visuals.append(page_title_vc("p4_title", "AI Action & Real-Time Alerts",
                                  "Fabric turns data into recommended action"))

    # Narrative text box
    visuals.append(vc("p4_narrative", 20, 70, 1240, 60,
                       textbox_config("p4_narrative",
                                      "Fabric helps Randstad move from retrospective reporting to proactive action: "
                                      "which clients are at risk, why, what value is exposed, and what action should happen next.",
                                      font_size=13, bold=False)))

    # KPI
    visuals.append(vc("p4_kpi1", 20, 140, 300, 110,
                       card_config("p4_kpi1", "High Impact Alerts", "High Impact Alerts", COLORS["high_risk"])))

    # Bar: Alerts by client
    visuals.append(vc("p4_bar1", 340, 140, 450, 200,
                       bar_config("p4_bar1", "client_name", "event_type", "Alerts by Client")))

    # Bar: Recommended actions by count
    visuals.append(vc("p4_bar2", 810, 140, 450, 200,
                       bar_config("p4_bar2", "recommended_action", "client_name", "Recommended Actions")))

    # Executive insight text box
    visuals.append(vc("p4_insight", 20, 260, 300, 80,
                       textbox_config("p4_insight",
                                      "Top priority: Clients with high risk scores, negative sentiment, "
                                      "open roles, and large margin exposure should receive immediate "
                                      "senior account intervention.",
                                      font_size=11, bold=True)))

    # Alert detail table
    visuals.append(vc("p4_table", 20, 350, 1240, 360,
                       table_config("p4_table",
                                    ["client_name", "event_time", "event_description",
                                     "impact_level", "recommended_action"],
                                    "Real-Time Alert Feed")))

    return {
        "name": "ReportSection4",
        "displayName": "AI Actions & Alerts",
        "displayOption": 0,
        "width": 1280,
        "height": 720,
        "visualContainers": visuals,
        "config": json.dumps({
            "name": "ReportSection4",
            "displayName": "AI Actions & Alerts",
            "ordinal": 3
        }),
        "filters": "[]"
    }


def build_report_json(semantic_model_id):
    """Build the full report.json."""
    report = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/report/1.0.0/schema.json",
        "themeCollection": {
            "baseTheme": {
                "name": "CY24SU06",
                "reportVersionAtImport": "5.53",
                "type": 2
            }
        },
        "sections": [
            build_page1(),
            build_page2(),
            build_page3(),
            build_page4()
        ],
        "config": json.dumps({
            "version": "5.53",
            "themeCollection": {
                "baseTheme": {
                    "name": "CY24SU06",
                    "reportVersionAtImport": "5.53",
                    "type": 2
                }
            },
            "activeSectionIndex": 0,
            "defaultDrillFilterOtherVisuals": True,
            "slowDataSourceSettings": {"isCrossHighlightingDisabled": False, "isSlicerSelectionsButtonEnabled": True}
        }),
        "layoutOptimization": 0
    }
    return report


def build_definition_pbir(semantic_model_id):
    return {
        "version": "4.0",
        "datasetReference": {
            "byPath": None,
            "byConnection": {
                "connectionString": None,
                "pbiServiceModelId": None,
                "pbiModelVirtualServerName": "sobe_wowvirtualserver",
                "pbiModelDatabaseName": semantic_model_id,
                "name": "EntityDataSource",
                "connectionType": "pbiServiceXmlaStyleLive"
            }
        }
    }


# ── Deployment ─────────────────────────────────────────────────────────────────

def create_semantic_model(token):
    print("📊 Creating Semantic Model...")
    headers = api_headers(token)

    model_bim = build_model_bim()
    pbism = build_pbism()

    payload = {
        "displayName": SEMANTIC_MODEL_NAME,
        "type": "SemanticModel",
        "definition": {
            "parts": [
                {"path": "model.bim", "payload": b64(model_bim), "payloadType": "InlineBase64"},
                {"path": "definition.pbism", "payload": b64(pbism), "payloadType": "InlineBase64"}
            ]
        }
    }

    url = f"{API_BASE}/workspaces/{WORKSPACE_ID}/items"
    resp = requests.post(url, headers=headers, json=payload)

    if resp.status_code in (200, 201, 202):
        # Handle long-running operation
        if resp.status_code == 202:
            print("   ⏳ Accepted (async). Polling for completion...")
            operation_url = resp.headers.get("Location", "")
            retry_after = int(resp.headers.get("Retry-After", "5"))
            for i in range(30):
                time.sleep(retry_after)
                poll = requests.get(operation_url, headers=headers)
                if poll.status_code == 200:
                    poll_data = poll.json()
                    status = poll_data.get("status", "")
                    print(f"   Status: {status}")
                    if status in ("Succeeded", "succeeded"):
                        result_url = poll_data.get("resourceLocation", "")
                        if result_url:
                            r2 = requests.get(result_url, headers=headers)
                            if r2.status_code == 200:
                                data = r2.json()
                                sm_id = data.get("id", "")
                                print(f"   ✅ Semantic Model created: {sm_id}")
                                return sm_id
                        # Try to find the item by listing
                        return find_item_by_name(token, SEMANTIC_MODEL_NAME, "SemanticModel")
                    elif status in ("Failed", "failed"):
                        print(f"   ❌ Failed: {poll_data}")
                        return None
            print("   ❌ Timed out waiting for semantic model creation")
            return None
        else:
            data = resp.json()
            sm_id = data.get("id", "")
            print(f"   ✅ Semantic Model created: {sm_id}")
            return sm_id
    else:
        print(f"   ❌ Error {resp.status_code}: {resp.text}")
        # If already exists, try to find it
        if "already in use" in resp.text.lower() or "already exists" in resp.text.lower():
            print("   🔍 Searching for existing model...")
            return find_item_by_name(token, SEMANTIC_MODEL_NAME, "SemanticModel")
        return None


def find_item_by_name(token, name, item_type):
    headers = api_headers(token)
    url = f"{API_BASE}/workspaces/{WORKSPACE_ID}/items"
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        for item in resp.json().get("value", []):
            if item.get("displayName") == name and item.get("type") == item_type:
                return item["id"]
    return None


def create_report(token, semantic_model_id):
    print("📈 Creating Power BI Report...")
    headers = api_headers(token)

    report_json = build_report_json(semantic_model_id)
    definition_pbir = build_definition_pbir(semantic_model_id)

    payload = {
        "displayName": REPORT_NAME,
        "type": "Report",
        "definition": {
            "parts": [
                {"path": "definition.pbir", "payload": b64(definition_pbir), "payloadType": "InlineBase64"},
                {"path": "report.json", "payload": b64(report_json), "payloadType": "InlineBase64"}
            ]
        }
    }

    url = f"{API_BASE}/workspaces/{WORKSPACE_ID}/items"
    resp = requests.post(url, headers=headers, json=payload)

    if resp.status_code in (200, 201, 202):
        if resp.status_code == 202:
            print("   ⏳ Accepted (async). Polling for completion...")
            operation_url = resp.headers.get("Location", "")
            retry_after = int(resp.headers.get("Retry-After", "5"))
            for i in range(30):
                time.sleep(retry_after)
                poll = requests.get(operation_url, headers=headers)
                if poll.status_code == 200:
                    poll_data = poll.json()
                    status = poll_data.get("status", "")
                    print(f"   Status: {status}")
                    if status in ("Succeeded", "succeeded"):
                        result_url = poll_data.get("resourceLocation", "")
                        if result_url:
                            r2 = requests.get(result_url, headers=headers)
                            if r2.status_code == 200:
                                data = r2.json()
                                rpt_id = data.get("id", "")
                                print(f"   ✅ Report created: {rpt_id}")
                                return rpt_id
                        return find_item_by_name(token, REPORT_NAME, "Report")
                    elif status in ("Failed", "failed"):
                        print(f"   ❌ Failed: {poll_data}")
                        return None
            print("   ❌ Timed out waiting for report creation")
            return None
        else:
            data = resp.json()
            rpt_id = data.get("id", "")
            print(f"   ✅ Report created: {rpt_id}")
            return rpt_id
    else:
        print(f"   ❌ Error {resp.status_code}: {resp.text}")
        if "already in use" in resp.text.lower() or "already exists" in resp.text.lower():
            print("   🔍 Searching for existing report...")
            return find_item_by_name(token, REPORT_NAME, "Report")
        return None


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  Randstad Executive Dashboard - Fabric Deployment")
    print("=" * 60)
    print()

    token = get_token()
    if not token:
        print("❌ Failed to get auth token. Run 'az login' first.")
        return

    print(f"✅ Auth token obtained")
    print(f"   Workspace: {WORKSPACE_ID}")
    print(f"   Lakehouse: {LAKEHOUSE_ID}")
    print()

    # Step 1: Create Semantic Model
    sm_id = create_semantic_model(token)
    if not sm_id:
        print("❌ Failed to create semantic model. Aborting.")
        return
    print()

    # Step 2: Create Report
    rpt_id = create_report(token, sm_id)
    if not rpt_id:
        print("❌ Failed to create report.")
        return

    print()
    print("=" * 60)
    print("  ✅ DEPLOYMENT COMPLETE!")
    print("=" * 60)
    print(f"  Semantic Model: {sm_id}")
    print(f"  Report:         {rpt_id}")
    print(f"  Workspace:      {WORKSPACE_ID}")
    print()
    print(f"  🔗 Open in Fabric:")
    print(f"     https://app.fabric.microsoft.com/groups/{WORKSPACE_ID}/reports/{rpt_id}")
    print()


if __name__ == "__main__":
    main()

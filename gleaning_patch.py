"""
gleaning_patch.py
=================
DROP-IN PATCH for process.py — adds LightRAG-style gleaning to your
existing extraction_prompt() function.

HOW TO INTEGRATE
----------------
1. Copy the GLEANING_PROMPT constant into process.py (near your PROMPT constant).
2. Copy the gleaning_round() function into process.py.
3. Replace your existing extraction_prompt() with the new version below.

Everything else (GlobalEntityRegistry, Neo4j writing, chunking) stays
100% unchanged.
"""

import json
import os
import re
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_community.chat_models import ChatOllama


# ══════════════════════════════════════════════════════════════════════════════
# GLEANING PROMPT  — paste this right after your existing PROMPT = """..."""
# ══════════════════════════════════════════════════════════════════════════════

GLEANING_PROMPT = """
You are an expert Knowledge Graph construction engine specialized in Neo4j.
You previously extracted a partial graph from a text chunk.
Your ONLY job now is to find what was MISSED — do NOT repeat anything already extracted.

════════════════════════════════════════════
ALREADY EXTRACTED (do NOT repeat these)
════════════════════════════════════════════

NODES ALREADY FOUND:
{already_nodes}

RELATIONSHIPS ALREADY FOUND:
{already_relationships}

════════════════════════════════════════════
ORIGINAL TEXT
════════════════════════════════════════════

{content}

════════════════════════════════════════════
YOUR TASK
════════════════════════════════════════════

Find ONLY the entities and relationships that were MISSED in the first pass.
Common things that get missed:
- People mentioned briefly or only by title/role (e.g. "the minister", "a spokesman")
- Organizations referenced by acronym only (e.g. "JAH", "WeI")
- Locations used as context (e.g. "in Lucknow", "near Karachi")
- Events or dates mentioned in passing
- Relationships implied by the text but not explicitly stated as actions
- Secondary connections between already-found entities

CRITICAL OUTPUT RULES (NON-NEGOTIABLE):
- Output MUST be valid JSON.
- Output MUST contain ONLY ONE JSON object.
- Output MUST contain EXACTLY two top-level keys: "nodes" and "relationships"
- Do NOT repeat nodes or relationships already in the ALREADY EXTRACTED section.
- Do NOT include markdown, comments, or extra text.
- Node IDs for NEW nodes MUST continue the sequence: if existing nodes go up
  to N{last_node_index}, start new nodes at N{next_node_index}.
- For NEW relationships between ALREADY EXISTING nodes, use their existing IDs.
- If truly nothing was missed, return: <JSON>{{"nodes":[],"relationships":[]}}</JSON>

GRAPH DATA MODEL (same as before — STRICT):

NODES — each new node MUST follow:
{{
  "id": "N{next_node_index}",
  "label": "Person | Organization | Location | Event | Product | Technology | Algorithm | Year | Book | Movie",
  "properties": {{
    "name": "string (REQUIRED)",
    "document_id": "{document_id}",
    "chunk_id": {chunk_id},
    "...": "any additional relevant properties"
  }}
}}

RELATIONSHIPS — each new relationship MUST follow:
{{
  "from": "<existing or new node ID>",
  "to":   "<existing or new node ID>",
  "type": "UPPER_SNAKE_CASE_VERB",
  "properties": {{
    "justification": "CLAIM: <from_name> <verb phrase> <to_name>.\\nEVIDENCE: <<=100 word snippet from text>>"
  }}
}}

JUSTIFICATION is REQUIRED for every relationship.
Do NOT invent facts. If unsure, omit the node/relationship.

FINAL VALIDATION before outputting:
- Are ALL new node IDs unique and not duplicating existing ones?
- Does every new relationship reference valid node IDs (existing or new)?
- Is every relationship semantically grounded in the text?

OUTPUT FORMAT (MUST FOLLOW EXACTLY):
Return ONLY the JSON wrapped like this, with nothing before or after:
<JSON>{{"nodes":[...],"relationships":[...]}}</JSON>
"""


# ══════════════════════════════════════════════════════════════════════════════
# GLEANING ROUND FUNCTION — paste this right before extraction_prompt()
# ══════════════════════════════════════════════════════════════════════════════

def _parse_json_from_response(raw: str) -> dict:
    """
    Robust parser that handles your <JSON>...</JSON> wrapper,
    markdown fences, and bare JSON objects.
    Same defensive logic used throughout your pipeline.
    """
    # Strip <think>...</think> from reasoning models (qwen3, deepseek, etc.)
    raw = re.sub(r"<think>.*?</think>", "", raw, flags=re.DOTALL).strip()

    # Try <JSON>...</JSON> wrapper first (your format)
    m = re.search(r"<JSON>(.*?)</JSON>", raw, re.DOTALL)
    if m:
        candidate = m.group(1).strip()
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            pass

    # Strip markdown fences
    raw = re.sub(r"```(?:json)?\s*", "", raw).replace("```", "").strip()

    # Find the outermost {...} object
    m = re.search(r"\{.*\}", raw, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(0))
        except json.JSONDecodeError:
            pass

    return {"nodes": [], "relationships": []}


def gleaning_round(
    chunk_id:       int,
    document_id:    str,
    content:        str,
    existing_nodes: list[dict],
    existing_rels:  list[dict],
    llm:            ChatOllama,
    round_num:      int = 1,
) -> dict:
    """
    Runs ONE gleaning pass and returns {"nodes": [...], "relationships": [...]}.
    Only genuinely NEW nodes/relationships are returned.

    Parameters match what extraction_prompt() already has available.
    """
    # Build compact summaries of what's already been found
    # (keep them short to save context — just id+name+label for nodes)
    node_summary = json.dumps(
        [
            {
                "id":    n.get("id", ""),
                "label": n.get("label", ""),
                "name":  (n.get("properties") or {}).get("name", n.get("id", "")),
            }
            for n in existing_nodes
        ],
        indent=2,
    )

    rel_summary = json.dumps(
        [
            {
                "from": r.get("from", ""),
                "to":   r.get("to",   ""),
                "type": r.get("type", ""),
                "justification": (r.get("properties") or {}).get("justification", "")[:120],
            }
            for r in existing_rels
        ],
        indent=2,
    )

    # Compute next node index so the prompt can tell the model where to start
    last_index   = 0
    for n in existing_nodes:
        nid = str(n.get("id", "N0"))
        digits = re.sub(r"\D", "", nid)
        if digits:
            last_index = max(last_index, int(digits))
    next_index = last_index + 1

    prompt_text = GLEANING_PROMPT.format(
        already_nodes         = node_summary,
        already_relationships = rel_summary,
        content               = content,
        document_id           = document_id,
        chunk_id              = chunk_id,
        last_node_index       = last_index,
        next_node_index       = next_index,
    )

    messages = [
        SystemMessage(content=(
            "You are a Neo4j knowledge-graph extraction engine. "
            "You return ONLY a single JSON object wrapped in <JSON>...</JSON> "
            "with keys: nodes, relationships. "
            "Never ask questions. Never output explanations. Never output markdown. "
            "If nothing was missed, return <JSON>{\"nodes\":[],\"relationships\":[]}</JSON>."
        )),
        HumanMessage(content=prompt_text),
    ]

    result      = llm.invoke(messages)
    raw         = result.content if hasattr(result, "content") else str(result)
    parsed      = _parse_json_from_response(raw)

    new_nodes   = parsed.get("nodes",         []) or []
    new_rels    = parsed.get("relationships",  []) or []

    # Deduplicate against existing by node ID (hard guarantee)
    existing_ids = {str(n.get("id", "")) for n in existing_nodes}
    new_nodes    = [n for n in new_nodes if str(n.get("id", "")) not in existing_ids]

    print(f"    [Gleaning round {round_num}] +{len(new_nodes)} nodes, +{len(new_rels)} relationships")
    return {"nodes": new_nodes, "relationships": new_rels}


# ══════════════════════════════════════════════════════════════════════════════
# UPDATED extraction_prompt()  — replace your existing one with this verbatim
# Only additions are:
#   • gleaning loop at the bottom (lines clearly marked)
#   • _parse_json_from_response() used for initial parse too (more robust)
# ══════════════════════════════════════════════════════════════════════════════

def extraction_prompt(chunk_id: int, document_id: str, content: str):
    """
    Extract entities and relationships from text using LLM.
    Returns (normalized_nodes, normalized_relationships).

    CHANGED: added gleaning loop — 2 extra passes to catch missed entities.
    Everything else is identical to the original.
    """
    # ── Original extraction pass (unchanged) ─────────────────────────────
    prompt      = PROMPT                                        # your existing PROMPT constant
    prompt_chunk = prompt.format(
        content=content, document_id=document_id, chunk_id=chunk_id
    )
    messages = [
        SystemMessage(content=(
            "You are a Neo4j knowledge-graph extraction engine. "
            "You must return ONLY a single JSON object wrapped in <JSON>...</JSON> "
            "with keys: nodes, relationships. "
            "Never ask questions. Never output explanations. Never output markdown. "
            "If extraction is not possible, return an empty graph: "
            "<JSON>{\"nodes\":[],\"relationships\":[]}</JSON>."
        )),
        HumanMessage(content=prompt_chunk),
    ]

    llm    = ChatOllama(model="qwen3:latest", base_url=os.getenv("OLLAMA_BASE_URL"))
    result = llm.invoke(messages)

    json_text = result.content if hasattr(result, "content") else str(result)

    # ── Parse initial result (more robust than before) ────────────────────
    parsed      = _parse_json_from_response(json_text)
    all_nodes   = list(parsed.get("nodes",         []) or [])
    all_rels    = list(parsed.get("relationships",  []) or [])

    print(f"  [Pass 0 — initial] {len(all_nodes)} nodes, {len(all_rels)} relationships")

    # ── GLEANING LOOP — 2 rounds (change MAX_GLEANING_ROUNDS to tune) ─────
    MAX_GLEANING_ROUNDS = 2
    STOP_IF_NEW_BELOW   = 1      # stop early if a round adds fewer than this

    for glean_round_num in range(1, MAX_GLEANING_ROUNDS + 1):
        glean_result = gleaning_round(
            chunk_id        = chunk_id,
            document_id     = document_id,
            content         = content,
            existing_nodes  = all_nodes,
            existing_rels   = all_rels,
            llm             = llm,
            round_num       = glean_round_num,
        )
        new_nodes = glean_result.get("nodes",        [])
        new_rels  = glean_result.get("relationships", [])

        all_nodes.extend(new_nodes)
        all_rels.extend(new_rels)

        # Early stopping: if this round found nothing meaningful, don't continue
        if len(new_nodes) < STOP_IF_NEW_BELOW and len(new_rels) < STOP_IF_NEW_BELOW:
            print(f"  [Gleaning] Stopped early after round {glean_round_num} — nothing new found")
            break
    # ── END GLEANING LOOP ─────────────────────────────────────────────────

    # Return exactly what your original code returned — caller is unchanged
    return json.dumps({"nodes": all_nodes, "relationships": all_rels})

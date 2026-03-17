"""
PageIndex-aligned RAG Pipeline (Final)
=======================================
Implements the exact PageIndex framework from:
  https://github.com/VectifyAI/PageIndex

Adapted to run locally with qwen3 via Ollama.

Key alignments with official PageIndex:
  1. Tree node schema:  title, node_id, start_index, end_index, summary, nodes
  2. Tree search prompt: exact format from docs.pageindex.ai/tutorials/tree-search/llm
                         LLM returns {"thinking": "...", "node_list": ["0001","0002"]}
  3. Context retrieval: by node_id -> start_index..end_index page range
  4. Tree saved as JSON file (like PageIndex SDK output)

Differences from stock PageIndex (intentional, local-only):
  - qwen3:latest via Ollama instead of gpt-4o
  - pdfplumber + pytesseract for PDF parsing
  - Source-aware cleaning for Wikipedia/academic/legal PDFs
  - Schema normaliser handles qwen3's tendency to invent JSON keys
"""

import re
import json
import pdfplumber
import pytesseract
from langchain_core.messages import HumanMessage
from langchain_community.chat_models import ChatOllama

# ── Model ──────────────────────────────────────────────────────────────────────
llm = ChatOllama(model="qwen3:latest", temperature=0)

# Update this path to your tesseract installation
pytesseract.pytesseract.tesseract_cmd = (
    r"C:\Users\user5\Downloads\tesseract-ocr-w64-setup.exe"
)


# ══════════════════════════════════════════════════════════════════════════════
# 1. PARSE PDF
# ══════════════════════════════════════════════════════════════════════════════
def parse_pdf(pdf_path):
    """
    Extract text per page. Falls back to OCR for image/scanned pages.
    Returns list of {"page": int, "text": str, "chars": list}
    """
    pages = []
    with pdfplumber.open(pdf_path) as pdf:
        for i, page in enumerate(pdf.pages):
            text = page.extract_text() or ""
            chars = page.chars
            if not text.strip():
                image = page.to_image(resolution=300).original
                text = pytesseract.image_to_string(image)
                chars = []
            pages.append({"page": i + 1, "text": text, "chars": chars})
    print(f"  [parse_pdf] Extracted {len(pages)} pages")
    return pages


# ══════════════════════════════════════════════════════════════════════════════
# 2. DETECT PDF SOURCE
# ══════════════════════════════════════════════════════════════════════════════
def detect_pdf_source(pages):
    """Auto-detect PDF type to apply appropriate cleaning."""
    sample = " ".join(p["text"][:600] for p in pages[:2]).lower()
    if "wikipedia" in sample or "wikimedia" in sample:
        return "wikipedia"
    if sum(1 for s in ["abstract", "doi:", "keywords:", "journal of",
                        "university", "et al.", "arxiv"] if s in sample) >= 2:
        return "academic"
    if sum(1 for s in ["whereas", "hereinafter", "pursuant",
                        "indemnify", "shall not"] if s in sample) >= 2:
        return "legal"
    if sum(1 for s in ["reuters", "associated press",
                        "published:", "updated:", "© "] if s in sample) >= 2:
        return "news"
    if sum(1 for s in ["executive summary", "table of contents",
                        "prepared by", "this report"] if s in sample) >= 2:
        return "report"
    if sum(1 for s in ["chapter ", "preface",
                        "bibliography", "foreword"] if s in sample) >= 2:
        return "book"
    return "generic"


# ══════════════════════════════════════════════════════════════════════════════
# 3. CLEAN TEXT
# ══════════════════════════════════════════════════════════════════════════════
def clean_text(text, source_type="generic"):
    """Remove source-specific noise before feeding to LLM."""
    # Universal
    text = re.sub(r'[^\x09\x0A\x0D\x20-\x7E\x80-\xFF]', ' ', text)
    text = re.sub(r'[ \t]{3,}', '  ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r'^\s*[-–]?\s*\d{1,4}\s*[-–]?\s*$', '', text, flags=re.MULTILINE)

    if source_type == "wikipedia":
        text = re.sub(
            r'\d{2}/\d{2}/\d{4},\s*\d+:\d+\s+\d{4}.*?Wikipedia\s*\n?',
            '', text, flags=re.IGNORECASE)
        text = re.sub(r'\[\d+\]', '', text)
        text = re.sub(r'\[nb\s*\d+\]', '', text, flags=re.IGNORECASE)
        text = re.sub(r'(?<!\w)edit(?!\w)', '', text, flags=re.IGNORECASE)
        text = re.sub(r'Retrieved from.*$', '', text, flags=re.MULTILINE)
        text = re.sub(r'https?://\S+', '', text)
    elif source_type == "academic":
        text = re.sub(r'^.{0,60}vol\.?\s*\d+.{0,60}$', '', text,
                      flags=re.MULTILINE | re.IGNORECASE)
        text = re.sub(r'doi:\s*\S+', '', text, flags=re.IGNORECASE)
    elif source_type == "legal":
        text = re.sub(r'CONFIDENTIAL.*?page\s*\d+\s*of\s*\d+', '',
                      text, flags=re.IGNORECASE)
        text = re.sub(r'_{3,}', '', text)
    elif source_type == "news":
        text = re.sub(r'\bADVERTISEMENT\b.*?\n', '', text, flags=re.IGNORECASE)
        text = re.sub(
            r'(Share this article|Subscribe now|Sign up for|Follow us).*?\n',
            '', text, flags=re.IGNORECASE)
    elif source_type == "report":
        text = re.sub(r'\b(CONFIDENTIAL|DRAFT|FOR INTERNAL USE ONLY)\b', '', text)
        text = re.sub(r'\.{4,}\s*\d+', '', text)
    elif source_type == "book":
        text = re.sub(r'^CHAPTER\s+\w+\s*$', '', text, flags=re.MULTILINE)

    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


# ══════════════════════════════════════════════════════════════════════════════
# 4. BUILD PAGEINDEX TREE  (LLM call)
# ══════════════════════════════════════════════════════════════════════════════
def build_pageindex_tree(pages, llm, chars_per_page=3000):
    """
    Core PageIndex step: LLM reads page text and returns a hierarchical
    tree using the official PageIndex node schema:
      title, node_id, start_index, end_index, summary, nodes
    """
    page_dump = ""
    for p in pages:
        page_dump += f"\n\n=== PAGE {p['page']} ===\n{p['text'][:chars_per_page]}"

    example = json.dumps([
        {
            "title": "Background",
            "node_id": "0001",
            "start_index": 1,
            "end_index": 2,
            "summary": "Historical context leading up to the conflict.",
            "nodes": [
                {
                    "title": "Early causes",
                    "node_id": "0002",
                    "start_index": 1,
                    "end_index": 1,
                    "summary": "Factors that initiated the tensions.",
                    "nodes": []
                }
            ]
        },
        {
            "title": "Military operations",
            "node_id": "0003",
            "start_index": 3,
            "end_index": 4,
            "summary": "Details of the airstrikes and counterattacks.",
            "nodes": []
        }
    ], indent=2)

    prompt = f"""You are a document analyst building a PageIndex tree structure.
Read the document pages and produce a hierarchical JSON index (like a Table of Contents).

Return a JSON ARRAY where every node has EXACTLY these six fields:
  "title"       - clean section name, no citation brackets like [1]
  "node_id"     - unique 4-digit string, e.g. "0001", "0002", "0003"
  "start_index" - first page number (integer) of this section
  "end_index"   - last page number (integer) of this section
  "summary"     - 1-2 sentences on what this section covers
  "nodes"       - list of child nodes (same structure) or empty list []

Example (follow this structure exactly):
{example}

STRICT RULES:
- Use ONLY the six fields: title, node_id, start_index, end_index, summary, nodes
- Do NOT add extra fields like "section", "subsections", "page", "content", "children"
- start_index and end_index must be integers matching actual page numbers
- node_id must be unique across the ENTIRE tree
- Do NOT write "Page 1" as a title — use the real section name from the document
- Return ONLY the raw JSON array. No markdown fences, no explanation

Document:
{page_dump}"""

    print("  [build_tree] Calling LLM (this may take a moment)...")
    res = llm.invoke([HumanMessage(content=prompt)])
    raw = res.content

    if isinstance(raw, list):
        raw = raw[0].get("text", "") if raw else ""
    raw = raw.strip()

    if raw.startswith("```"):
        parts = raw.split("```")
        raw = parts[1] if len(parts) > 1 else raw
        if raw.startswith("json"):
            raw = raw[4:]
    raw = raw.strip()

    # Attempt 1: direct parse
    try:
        tree = json.loads(raw)
        if isinstance(tree, dict):
            tree = tree.get("nodes", tree.get("children", [tree]))
        print(f"  [build_tree] Parsed: {len(tree)} top-level nodes")
        return tree
    except json.JSONDecodeError as e:
        print(f"  [build_tree] Parse failed: {e}")
        print(f"  [build_tree] First 400 chars:\n{raw[:400]}")

    # Attempt 2: extract array from anywhere
    match = re.search(r'\[.*\]', raw, re.DOTALL)
    if match:
        try:
            tree = json.loads(match.group())
            print(f"  [build_tree] Recovered: {len(tree)} nodes")
            return tree
        except json.JSONDecodeError:
            pass

    # Attempt 3: minimal retry
    print("  [build_tree] Retrying with minimal prompt...")
    simple = f"""List the main sections of this document as JSON.

{page_dump[:3000]}

Return ONLY (use integer page numbers not strings):
[
  {{"title": "Section name", "node_id": "0001", "start_index": 1, "end_index": 2,
    "summary": "What it covers", "nodes": []}},
  {{"title": "Next section", "node_id": "0002", "start_index": 3, "end_index": 3,
    "summary": "What it covers", "nodes": []}}
]"""

    res2 = llm.invoke([HumanMessage(content=simple)])
    raw2 = res2.content.strip()
    if raw2.startswith("```"):
        raw2 = raw2.split("```")[1].lstrip("json").strip()
    try:
        tree = json.loads(raw2)
        if isinstance(tree, dict):
            tree = [tree]
        print(f"  [build_tree] Retry succeeded: {len(tree)} nodes")
        return tree
    except json.JSONDecodeError:
        pass

    with open("raw_llm_output.txt", "w") as f:
        f.write(raw)
    print("  [build_tree] All attempts failed. Raw output saved to raw_llm_output.txt")
    return []


# ══════════════════════════════════════════════════════════════════════════════
# 5. NORMALISE SCHEMA
# ══════════════════════════════════════════════════════════════════════════════
def normalise_tree(raw_tree, pages):
    """
    Maps ANY LLM output schema to the PageIndex standard:
      title, node_id, start_index, end_index, summary, nodes

    Handles all observed qwen3 patterns:
      Pattern A  correct schema already
      Pattern B  "section"/"page"/"subsections" keys (seen in screenshots)
      Pattern C  "sub_section"/"content" in children
      Pattern D  wrapped in {"sections":[...]} object
      Pattern E  empty / failed
    """
    all_page_nums = [p["page"] for p in pages]
    node_counter = [0]

    def next_id():
        node_counter[0] += 1
        return f"{node_counter[0]:04d}"

    def to_int(val):
        try:
            return int(str(val))
        except (TypeError, ValueError):
            return None

    def best_title(node):
        for key in ("title", "section", "heading", "name", "topic", "chapter"):
            v = str(node.get(key, "")).strip()
            if v and not re.match(r'^Section \(depth \d+\)$', v) \
                  and not re.match(r'^Page \d+$', v) \
                  and v.lower() not in ("untitled", "none", "null", ""):
                return v
        return ""

    def best_summary(node):
        for key in ("summary", "content", "description", "overview", "text"):
            v = str(node.get(key, "")).strip()
            if v and v.lower() not in ("none", "null", ""):
                return v[:300]
        return ""

    def best_pages(node, inferred=None):
        si = to_int(node.get("start_index"))
        ei = to_int(node.get("end_index"))
        if si and ei:
            return si, ei
        if si:
            return si, si
        pl = node.get("pages", node.get("page_numbers"))
        if isinstance(pl, list) and pl:
            ints = [to_int(p) for p in pl if to_int(p)]
            if ints:
                return min(ints), max(ints)
        pg = to_int(node.get("page"))
        if pg:
            return pg, pg
        if inferred:
            return inferred, inferred
        return None, None

    def best_children(node):
        for key in ("nodes", "children", "subsections", "sub_sections",
                    "sections", "subtopics", "items"):
            v = node.get(key)
            if isinstance(v, list) and v:
                return v
        return []

    def norm(node, inferred=None):
        if not isinstance(node, dict):
            return None
        title   = best_title(node)
        summary = best_summary(node)
        si, ei  = best_pages(node, inferred)
        raw_kids = best_children(node)

        kids = []
        seen = set()
        for child in raw_kids:
            if not isinstance(child, dict):
                continue
            child = dict(child)
            for old, new in (("sub_section","title"), ("sub_sub_section","title"),
                             ("sub_subsections","nodes")):
                if old in child and new not in child:
                    child[new] = child.pop(old)
            r = norm(child, inferred=si if si else inferred)
            if r is None:
                continue
            if r.get("_promote"):
                for p in r["_promote"]:
                    if p["title"] not in seen:
                        seen.add(p["title"])
                        kids.append(p)
            elif r["title"] not in seen:
                seen.add(r["title"])
                kids.append(r)

        if not title:
            return {"_promote": kids}

        if si is None and kids:
            starts = [c["start_index"] for c in kids if c.get("start_index")]
            ends   = [c["end_index"]   for c in kids if c.get("end_index")]
            si = min(starts) if starts else 1
            ei = max(ends)   if ends   else si

        return {
            "title":       title,
            "node_id":     next_id(),
            "start_index": si or 1,
            "end_index":   ei or (si or 1),
            "summary":     summary,
            "nodes":       kids,
        }

    def flatten_promoted(nodes):
        result = []
        for n in nodes:
            if isinstance(n, dict) and "_promote" in n:
                result.extend(flatten_promoted(n["_promote"]))
            elif n:
                n["nodes"] = flatten_promoted(n.get("nodes", []))
                result.append(n)
        return result

    if isinstance(raw_tree, dict):
        for key in ("nodes", "sections", "children", "tree", "index"):
            if key in raw_tree and isinstance(raw_tree[key], list):
                raw_tree = raw_tree[key]
                break
        else:
            raw_tree = [raw_tree]

    if not isinstance(raw_tree, list) or not raw_tree:
        print("  [normalise] Empty — building page stubs")
        return [{"title": f"Page {p['page']}", "node_id": f"{p['page']:04d}",
                 "start_index": p["page"], "end_index": p["page"],
                 "summary": p["text"][:120].replace("\n"," "), "nodes": []}
                for p in pages]

    normalised = []
    seen_top = set()
    for i, node in enumerate(raw_tree):
        inferred = all_page_nums[i] if i < len(all_page_nums) else None
        r = norm(node, inferred=inferred)
        if r is None:
            continue
        if r.get("_promote"):
            for p in flatten_promoted(r["_promote"]):
                if p["title"] not in seen_top:
                    seen_top.add(p["title"])
                    normalised.append(p)
        elif r["title"] not in seen_top:
            seen_top.add(r["title"])
            normalised.append(r)

    normalised = flatten_promoted(normalised)
    if not normalised:
        print("  [normalise] Nothing produced — page stubs")
        return [{"title": f"Page {p['page']}", "node_id": f"{p['page']:04d}",
                 "start_index": p["page"], "end_index": p["page"],
                 "summary": p["text"][:120].replace("\n"," "), "nodes": []}
                for p in pages]

    print(f"  [normalise] {len(normalised)} top-level nodes")
    return normalised


# ══════════════════════════════════════════════════════════════════════════════
# 6. VALIDATE TREE
# ══════════════════════════════════════════════════════════════════════════════
def validate_tree(tree, all_page_numbers):
    """Repair page ranges and ensure structural correctness. Never drops nodes."""
    all_ints = set(int(p) for p in all_page_numbers)
    lo = min(all_ints) if all_ints else 1
    hi = max(all_ints) if all_ints else 1

    def clamp(v):
        try:
            return max(lo, min(hi, int(str(v))))
        except (TypeError, ValueError):
            return lo

    def repair(node):
        if not isinstance(node, dict):
            return None
        node.setdefault("title",       "Untitled")
        node.setdefault("node_id",     "0000")
        node.setdefault("start_index", lo)
        node.setdefault("end_index",   lo)
        node.setdefault("summary",     "")
        node.setdefault("nodes",       [])

        node["title"]       = str(node["title"]).strip() or "Untitled"
        node["summary"]     = str(node["summary"]).strip()
        node["start_index"] = clamp(node["start_index"])
        node["end_index"]   = clamp(node["end_index"])
        if node["end_index"] < node["start_index"]:
            node["end_index"] = node["start_index"]

        if not isinstance(node["nodes"], list):
            node["nodes"] = []

        kids = []
        seen = set()
        for child in node["nodes"]:
            r = repair(child)
            if r and r["title"] not in seen:
                seen.add(r["title"])
                kids.append(r)
        node["nodes"] = kids

        if kids:
            node["start_index"] = min(node["start_index"],
                                      min(c["start_index"] for c in kids))
            node["end_index"]   = max(node["end_index"],
                                      max(c["end_index"]   for c in kids))
        return node

    if not isinstance(tree, list):
        tree = [tree] if tree else []
    out = []
    seen = set()
    for n in tree:
        r = repair(n)
        if r and r["title"] not in seen:
            seen.add(r["title"])
            out.append(r)
    return out


# ══════════════════════════════════════════════════════════════════════════════
# 7. VERIFY TREE QUALITY
# ══════════════════════════════════════════════════════════════════════════════
def verify_tree_quality(tree):
    issues = []

    def print_node(node, indent=0):
        prefix = "  " * indent
        title   = node.get("title", "?")
        nid     = node.get("node_id", "?")
        start   = node.get("start_index", "?")
        end     = node.get("end_index", "?")
        summary = node.get("summary", "")
        print(f"{prefix}[{nid}] p{start}-p{end}  {title}")
        if summary:
            print(f"{prefix}    -> {summary[:80].replace(chr(10), ' ')}")
        if not summary:
            issues.append(f"NO SUMMARY: '{title}'")
        if re.search(r'\[\d+\]', title):
            issues.append(f"CITATION IN TITLE: '{title}'")
        if re.match(r'^Page \d+$', title):
            issues.append(f"PAGE-LEVEL FALLBACK: '{title}'")
        for child in node.get("nodes", []):
            print_node(child, indent + 1)

    print("\n" + "=" * 60)
    print("PAGEINDEX TREE STRUCTURE")
    print("=" * 60)
    total = 0
    for node in tree:
        print_node(node)
        total += 1 + sum(1 for _ in node.get("nodes", []))
    print(f"\nTotal nodes: {total}")
    print("=" * 60)
    if issues:
        print(f"QUALITY ISSUES ({len(issues)} found):")
        for issue in issues:
            print(f"  [!] {issue}")
    else:
        print("QUALITY CHECK: No issues found")
    print("=" * 60 + "\n")
    return len(issues) == 0


# ══════════════════════════════════════════════════════════════════════════════
# 8. TREE SEARCH  (exact PageIndex prompt format)
# ══════════════════════════════════════════════════════════════════════════════
def tree_search(query, tree, llm):
    """
    PageIndex LLM tree search using the exact prompt from:
    https://docs.pageindex.ai/tutorials/tree-search/llm

    LLM reasons over the tree and returns:
      {"thinking": "...", "node_list": ["0001", "0003"]}
    """
    def flatten(nodes):
        result = []
        for n in nodes:
            result.append(n)
            if n.get("nodes"):
                result.extend(flatten(n["nodes"]))
        return result

    all_nodes = flatten(tree)
    if not all_nodes:
        return []

    def tree_repr(nodes, indent=0):
        lines = []
        for n in nodes:
            prefix = "  " * indent
            lines.append(
                f"{prefix}[{n['node_id']}] p{n['start_index']}-p{n['end_index']} "
                f"| {n['title']}: {n.get('summary','')[:80]}"
            )
            if n.get("nodes"):
                lines.append(tree_repr(n["nodes"], indent + 1))
        return "\n".join(lines)

    # Exact PageIndex tree search prompt from the official docs
    prompt = f"""You are given a query and the tree structure of a document.
You need to find all nodes that are likely to contain the answer.

Query: {query}

Document tree structure:
{tree_repr(tree)}

Reply in the following JSON format:
{{
  "thinking": "<your reasoning about which nodes are relevant>",
  "node_list": ["node_id1", "node_id2"]
}}"""

    res = llm.invoke([HumanMessage(content=prompt)])
    raw = res.content.strip()

    if raw.startswith("```"):
        raw = raw.split("```")[1].lstrip("json").strip()

    try:
        result = json.loads(raw)
        node_ids = result.get("node_list", [])
        thinking = result.get("thinking", "")
        print(f"  [tree_search] Thinking: {thinking[:120]}")
        print(f"  [tree_search] Selected node_ids: {node_ids}")
        return [str(nid) for nid in node_ids]
    except (json.JSONDecodeError, AttributeError):
        found = re.findall(r'\b(\d{4})\b', raw)
        if found:
            print(f"  [tree_search] Recovered node_ids: {found}")
            return found
        print("  [tree_search] Could not parse — using first node")
        return [all_nodes[0]["node_id"]] if all_nodes else []


# ══════════════════════════════════════════════════════════════════════════════
# 9. GET CONTEXT  (node_id -> page range)
# ══════════════════════════════════════════════════════════════════════════════
def get_context(pages, tree, node_ids):
    """
    Retrieves page text using start_index..end_index from matched nodes.
    This mirrors how PageIndex SDK retrieves content per node.
    """
    def flatten(nodes):
        result = []
        for n in nodes:
            result.append(n)
            if n.get("nodes"):
                result.extend(flatten(n["nodes"]))
        return result

    node_map = {n["node_id"]: n for n in flatten(tree)}

    page_ids = set()
    for nid in node_ids:
        node = node_map.get(str(nid))
        if node:
            start = int(node.get("start_index", 1))
            end   = int(node.get("end_index", start))
            for pg in range(start, end + 1):
                page_ids.add(pg)

    if not page_ids:
        page_ids = {p["page"] for p in pages}

    context = ""
    for p in sorted(pages, key=lambda x: x["page"]):
        if p["page"] in page_ids:
            context += f"\n\n=== Page {p['page']} ===\n{p['text']}"

    return context[:20000]


# ══════════════════════════════════════════════════════════════════════════════
# 10. ANSWER QUESTION
# ══════════════════════════════════════════════════════════════════════════════
def answer_question(query, context):
    if not context.strip():
        return "No relevant content was found in the document for this query."

    prompt = f"""You are an expert analyst. Answer using ONLY the context below.
- Extract SPECIFIC facts, figures, names, and dates
- Reference page numbers where possible (e.g. "According to page 2...")
- Give detailed, targeted information — do NOT summarize broadly
- If context lacks sufficient info, state exactly what IS present

Question: {query}

Context:
{context}

Answer:"""
    res = llm.invoke([HumanMessage(content=prompt)])
    return res.content


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    import sys

    pdf_path = sys.argv[1] if len(sys.argv) > 1 else (
        input("Enter PDF path (default: IranWar.pdf): ").strip() or "IranWar.pdf"
    )

    print(f"\n{'='*60}")
    print(f"PageIndex RAG  |  {pdf_path}")
    print(f"{'='*60}")

    # Step 1: Parse
    print(f"\n[1/5] Parsing PDF...")
    pages = parse_pdf(pdf_path)
    all_page_numbers = {p["page"] for p in pages}

    # Step 2: Detect source
    print(f"\n[2/5] Detecting source type...")
    source_type = detect_pdf_source(pages)
    print(f"  -> Detected: '{source_type}'")

    # Step 3: Clean
    print(f"\n[3/5] Cleaning text (mode: {source_type})...")
    for p in pages:
        before = len(p["text"])
        p["text"] = clean_text(p["text"], source_type)
        print(f"  p{p['page']:>2}: {before} -> {len(p['text'])} chars "
              f"({before - len(p['text'])} removed)")

    # Step 4: Build PageIndex tree
    print(f"\n[4/5] Building PageIndex tree (LLM)...")
    raw_tree = build_pageindex_tree(pages, llm, chars_per_page=3000)

    print(f"\n  Raw tree preview (first node):")
    print(json.dumps(raw_tree[0], indent=2)[:400] if raw_tree else "  (empty)")

    print(f"\n  Normalising to PageIndex schema...")
    tree = normalise_tree(raw_tree, pages)

    print(f"\n  Validating...")
    tree = validate_tree(tree, all_page_numbers)
    print(f"  -> {len(tree)} top-level nodes")

    # Save tree (matches PageIndex SDK output format)
    tree_file = pdf_path.replace(".pdf", "_pageindex_tree.json")
    with open(tree_file, "w") as f:
        json.dump(tree, f, indent=2)
    print(f"  -> Tree saved: {tree_file}")

    # Step 5: Quality check
    print(f"\n[5/5] Quality check...")
    verify_tree_quality(tree)

    # Interactive Q&A
    print("=" * 60)
    print("Ready. Type your question (or 'quit' to exit).")
    print("=" * 60 + "\n")

    while True:
        query = input("Ask question: ").strip()
        if not query:
            continue
        if query.lower() in ("quit", "exit", "q"):
            print("Exiting.")
            break

        print(f"\n  Running PageIndex tree search...")
        node_ids = tree_search(query, tree, llm)

        context = get_context(pages, tree, node_ids)
        print(f"  -> Context: {len(context)} chars")

        if not context.strip():
            print("  [!] No context — try rephrasing.\n")
            continue

        print("  Generating answer...\n")
        answer = answer_question(query, context)
        print(f"Answer:\n{answer}\n")
        print("-" * 60 + "\n")

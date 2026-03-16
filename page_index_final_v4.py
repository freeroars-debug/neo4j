"""
RAG Pipeline with Hierarchical Tree Index - v4 (Final)
=======================================================
Key change from v3:
  - Replaced regex heading detection + build_tree with a single
    LLM-based `build_tree_from_raw()` call that reads page text
    directly and produces a semantically correct tree.
  - Added `validate_and_repair_tree()` to guarantee tree structure
    is always correct before navigation.
  - Added `verify_tree_quality()` to print a readable debug view.

Works with ANY PDF source: Wikipedia, academic, legal, news, reports, books.
"""

import re
import json
import pdfplumber
import pytesseract
from langchain_core.messages import HumanMessage
from langchain_community.chat_models import ChatOllama

# ── Model ──────────────────────────────────────────────────────────────────────
llm = ChatOllama(model="qwen3:latest", temperature=0)

# Set tesseract path (Windows) — update if needed
pytesseract.pytesseract.tesseract_cmd = (
    r"C:\Users\user5\Downloads\tesseract-ocr-w64-setup.exe"
)


# ══════════════════════════════════════════════════════════════════════════════
# 1. PARSE PDF
# ══════════════════════════════════════════════════════════════════════════════
def parse_pdf(pdf_path):
    """
    Extract text and char metadata from every page.
    Falls back to OCR for scanned / image pages.
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

            pages.append({
                "page": i + 1,
                "text": text,
                "chars": chars,
            })

    print(f"  [parse_pdf] Extracted {len(pages)} pages")
    return pages


# ══════════════════════════════════════════════════════════════════════════════
# 2. DETECT PDF SOURCE
# ══════════════════════════════════════════════════════════════════════════════
def detect_pdf_source(pages):
    """
    Identify PDF type from content signals in the first two pages.
    Returns: "wikipedia" | "academic" | "legal" | "news" | "report" | "book" | "generic"
    """
    sample = " ".join(p["text"][:600] for p in pages[:2]).lower()

    if "wikipedia" in sample or "wikimedia" in sample:
        return "wikipedia"

    academic = ["abstract", "doi:", "keywords:", "journal of", "university",
                "et al.", "arxiv", "ieee", "springer", "published in"]
    if sum(1 for s in academic if s in sample) >= 2:
        return "academic"

    legal = ["whereas", "hereinafter", "pursuant", "indemnify",
             "notwithstanding", "shall not", "party a", "party b"]
    if sum(1 for s in legal if s in sample) >= 2:
        return "legal"

    news = ["reuters", "associated press", "staff reporter",
            "published:", "updated:", "© ", "all rights reserved"]
    if sum(1 for s in news if s in sample) >= 2:
        return "news"

    report = ["executive summary", "table of contents", "prepared by",
              "prepared for", "confidential", "this report", "findings"]
    if sum(1 for s in report if s in sample) >= 2:
        return "report"

    book = ["chapter ", "preface", "acknowledgement",
            "bibliography", "foreword", "table of contents"]
    if sum(1 for s in book if s in sample) >= 2:
        return "book"

    return "generic"


# ══════════════════════════════════════════════════════════════════════════════
# 3. CLEAN TEXT  (source-aware)
# ══════════════════════════════════════════════════════════════════════════════
def clean_text(text, source_type="generic"):
    """
    Remove noise appropriate to the PDF source type.
    Universal cleaning always applied first.
    """
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
        text = re.sub(r'§\s*(\d)', r'Section \1', text)

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
# 4. BUILD TREE FROM RAW  (LLM-based — replaces regex chunking)
# ══════════════════════════════════════════════════════════════════════════════
def build_tree_from_raw(pages, llm, chars_per_page=3000):
    """
    Feed cleaned page text DIRECTLY to the LLM and ask it to:
      1. Identify real section headings semantically (not by pattern)
      2. Build the full hierarchy in one pass

    This is far more reliable than regex for:
      - Wikipedia browser prints (no font variation, fragments everywhere)
      - Dense reports (numbered sections, acronyms)
      - Any PDF where structural signals are weak

    chars_per_page: how much of each page to include in the prompt.
    Increase if your LLM has a large context window.
    """
    # Build a compact page dump
    page_dump = ""
    for p in pages:
        page_dump += f"\n\n=== PAGE {p['page']} ===\n{p['text'][:chars_per_page]}"

    prompt = f"""You are an expert document analyst. Read the document pages below and build a complete hierarchical index.

TASK:
1. Identify ALL real section headings in the document — based on CONTENT and MEANING, not just formatting
2. Ignore citation fragments like "[9][10]", colon-separated table labels, or partial sentences
3. Group related sections as parent → children based on topic hierarchy
4. Note which page numbers each section spans

OUTPUT FORMAT — return a JSON array (not object). Each node must have EXACTLY these fields:
{{
  "title": "Clean section title without citations or symbols",
  "summary": "1-2 sentences describing what this section actually covers",
  "pages": [list of integer page numbers],
  "children": [list of child nodes with same structure, or empty list]
}}

RULES:
- "title" must be a real section name (e.g. "Nuclear program", "Background", "Casualties")
- "summary" must reflect actual content, not just restate the title
- "pages" must be accurate — list every page the section content appears on
- "children" must follow the same schema recursively
- A node with no children must have "children": []
- Do NOT include page numbers as section titles
- Do NOT invent sections that don't exist in the document

DOCUMENT:
{page_dump}

Return ONLY a valid JSON array. No markdown fences, no explanation, no preamble."""

    print("  [build_tree] Calling LLM to build semantic tree...")
    res = llm.invoke([HumanMessage(content=prompt)])
    raw = res.content

    if isinstance(raw, list):
        raw = raw[0].get("text", "") if raw else ""
    raw = raw.strip()

    # Strip markdown fences if LLM adds them
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
            # LLM returned a single root object — unwrap or wrap in list
            tree = tree.get("children", [tree])
        print(f"  [build_tree] Tree parsed successfully: {len(tree)} top-level nodes")
        return tree
    except json.JSONDecodeError as e:
        print(f"  [build_tree] JSON parse failed: {e}")

    # Attempt 2: extract JSON array from anywhere in the response
    match = re.search(r'\[.*\]', raw, re.DOTALL)
    if match:
        try:
            tree = json.loads(match.group())
            print(f"  [build_tree] Recovered JSON from response: {len(tree)} nodes")
            return tree
        except json.JSONDecodeError:
            pass

    # Attempt 3: retry with simpler prompt
    print("  [build_tree] Retrying with simplified prompt...")
    simple_prompt = f"""Read this document and list its main sections as JSON.

{page_dump[:4000]}

Return a JSON array like:
[
  {{"title": "Section name", "summary": "What it covers", "pages": [1, 2], "children": []}},
  {{"title": "Another section", "summary": "What it covers", "pages": [3], "children": []}}
]

Return ONLY the JSON array."""

    res2 = llm.invoke([HumanMessage(content=simple_prompt)])
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

    # Fallback: flat page-level tree
    print("  [build_tree] All attempts failed. Using page-level fallback.")
    return [
        {
            "title": f"Page {p['page']}",
            "summary": p["text"][:120].replace("\n", " "),
            "pages": [p["page"]],
            "children": [],
        }
        for p in pages
    ]


# ══════════════════════════════════════════════════════════════════════════════
# 5. VALIDATE AND REPAIR TREE
# ══════════════════════════════════════════════════════════════════════════════
def validate_and_repair_tree(tree, all_page_numbers):
    """
    Guarantee the tree is structurally correct before navigation.

    Checks and fixes:
      - Every node has all required fields
      - "pages" contains only valid integers within the document
      - "children" is always a list
      - "title" and "summary" are non-empty strings
      - No node has duplicate titles at the same level
      - Parent "pages" includes all child pages (so navigation finds everything)
      - Removes completely empty / useless nodes
    """

    def repair_node(node, depth=0):
        if not isinstance(node, dict):
            return None

        # Ensure all required fields exist
        node.setdefault("title", f"Section (depth {depth})")
        node.setdefault("summary", "")
        node.setdefault("pages", [])
        node.setdefault("children", [])

        # Clean title
        node["title"] = str(node["title"]).strip()
        if not node["title"]:
            node["title"] = "Untitled section"

        # Clean summary
        node["summary"] = str(node["summary"]).strip()

        # Validate and clean pages list
        raw_pages = node["pages"]
        if not isinstance(raw_pages, list):
            raw_pages = [raw_pages] if raw_pages else []

        valid_pages = []
        for p in raw_pages:
            try:
                pi = int(p)
                if pi in all_page_numbers:
                    valid_pages.append(pi)
            except (TypeError, ValueError):
                pass
        node["pages"] = sorted(set(valid_pages))

        # Repair children recursively
        if not isinstance(node["children"], list):
            node["children"] = []

        repaired_children = []
        seen_titles = set()
        for child in node["children"]:
            repaired = repair_node(child, depth + 1)
            if repaired is None:
                continue
            # Skip duplicate titles at same level
            if repaired["title"] in seen_titles:
                continue
            seen_titles.add(repaired["title"])
            repaired_children.append(repaired)

        node["children"] = repaired_children

        # Ensure parent pages include all child pages
        child_pages = []
        for child in node["children"]:
            child_pages.extend(child["pages"])
        all_node_pages = sorted(set(node["pages"] + child_pages))
        node["pages"] = all_node_pages

        # Drop nodes with no pages AND no useful content
        if not node["pages"] and not node["children"]:
            return None

        return node

    if not isinstance(tree, list):
        tree = [tree] if tree else []

    repaired = []
    seen_top_titles = set()
    for node in tree:
        fixed = repair_node(node, depth=0)
        if fixed is None:
            continue
        if fixed["title"] in seen_top_titles:
            continue
        seen_top_titles.add(fixed["title"])
        repaired.append(fixed)

    # Final safety: if tree is completely empty, return page-level nodes
    if not repaired:
        print("  [validate] Tree empty after repair — returning page stubs")
        repaired = [
            {"title": f"Page {p}", "summary": "", "pages": [p], "children": []}
            for p in sorted(all_page_numbers)
        ]

    return repaired


# ══════════════════════════════════════════════════════════════════════════════
# 6. VERIFY TREE QUALITY  (debug printer)
# ══════════════════════════════════════════════════════════════════════════════
def verify_tree_quality(tree):
    """
    Print a readable tree view and quality report.
    Flags:
      - Nodes with no pages
      - Nodes with empty summaries
      - Nodes whose title looks like a citation fragment
    """
    issues = []

    def print_node(node, indent=0):
        prefix = "  " * indent
        pages_str = str(node.get("pages", []))
        title = node.get("title", "?")
        summary = node.get("summary", "")
        children = node.get("children", [])

        print(f"{prefix}[{pages_str}] {title}")
        if summary:
            preview = summary[:80].replace("\n", " ")
            print(f"{prefix}    → {preview}")

        # Flag problems
        if not node.get("pages"):
            issues.append(f"NO PAGES: '{title}'")
        if not summary:
            issues.append(f"NO SUMMARY: '{title}'")
        if re.search(r'\[\d+\]', title):
            issues.append(f"CITATION IN TITLE: '{title}'")
        if re.match(r'^Page \d+$', title):
            issues.append(f"PAGE-LEVEL FALLBACK: '{title}' — tree may be low quality")

        for child in children:
            print_node(child, indent + 1)

    print("\n" + "=" * 60)
    print("TREE STRUCTURE")
    print("=" * 60)
    for node in tree:
        print_node(node)

    print("\n" + "=" * 60)
    if issues:
        print(f"QUALITY ISSUES ({len(issues)} found):")
        for issue in issues:
            print(f"  [!] {issue}")
    else:
        print("QUALITY CHECK: No issues found")
    print("=" * 60 + "\n")

    return len(issues) == 0


# ══════════════════════════════════════════════════════════════════════════════
# 7. NAVIGATE TREE
# ══════════════════════════════════════════════════════════════════════════════
def navigate_tree(query, tree, llm, top_k=2):
    """
    Flatten entire tree, score all nodes against query,
    return deduplicated page numbers for top_k most relevant nodes.
    Also pulls in children pages of selected nodes.
    """

    def flatten(nodes):
        result = []
        for node in nodes:
            result.append(node)
            if node.get("children"):
                result.extend(flatten(node["children"]))
        return result

    all_nodes = flatten(tree)
    if not all_nodes:
        return [1]

    desc = "\n".join(
        f"{i}. [pages: {n.get('pages', [])}] {n['title']} — {n.get('summary', '')}"
        for i, n in enumerate(all_nodes)
    )

    prompt = f"""User question: {query}

Document sections:
{desc}

Which {top_k} sections are most relevant to answer this question?
Return ONLY a JSON list of their index numbers.
Example: [2, 5]"""

    res = llm.invoke([HumanMessage(content=prompt)])
    raw = res.content.strip()
    raw = re.sub(r'```[a-z]*', '', raw).strip('`').strip()

    try:
        indices = json.loads(raw)
        if not isinstance(indices, list):
            indices = [indices]
    except Exception:
        indices = [int(x) for x in re.findall(r'\d+', raw)][:top_k]
        if not indices:
            indices = [0]

    selected_pages = []
    for idx in indices:
        if 0 <= idx < len(all_nodes):
            node = all_nodes[idx]
            selected_pages.extend(node.get("pages", []))
            for child in node.get("children", []):
                selected_pages.extend(child.get("pages", []))

    return list(dict.fromkeys(selected_pages))  # deduped, order preserved


# ══════════════════════════════════════════════════════════════════════════════
# 8. GET CONTEXT
# ══════════════════════════════════════════════════════════════════════════════
def get_context(pages, page_ids):
    """
    Assemble context from pages matching page_ids.
    Limit: 20,000 chars. Includes page headers for LLM grounding.
    """
    context = ""
    for p in pages:
        if p["page"] in page_ids:
            context += f"\n\n=== Page {p['page']} ===\n{p['text']}"
    return context[:20000]


# ══════════════════════════════════════════════════════════════════════════════
# 9. ANSWER QUESTION
# ══════════════════════════════════════════════════════════════════════════════
def answer_question(query, context):
    """
    Extract a specific, grounded answer from context.
    """
    if not context.strip():
        return "No relevant content was found in the document for this query."

    prompt = f"""You are an expert analyst reviewing a document.
Answer the question using ONLY the context provided below.

Instructions:
- Extract and state SPECIFIC facts, figures, names, and dates from the context
- Reference page numbers where possible (e.g. "According to page 2...")
- If a specific sub-topic is mentioned, describe it in full detail
- Do NOT summarize broadly — give precise, targeted information
- If context lacks sufficient information, state exactly what IS present and what is missing

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

    # Accept PDF path as CLI arg or prompt
    if len(sys.argv) > 1:
        pdf_path = sys.argv[1]
    else:
        pdf_path = input("Enter PDF path (default: IranWar.pdf): ").strip() or "IranWar.pdf"

    # ── Step 1: Parse ──────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"[1/5] Parsing: {pdf_path}")
    print('='*60)
    pages = parse_pdf(pdf_path)
    all_page_numbers = set(p["page"] for p in pages)

    # ── Step 2: Detect source ──────────────────────────────────────────────────
    print(f"\n[2/5] Detecting source type...")
    source_type = detect_pdf_source(pages)
    print(f"  → Detected: '{source_type}'")

    # ── Step 3: Clean ──────────────────────────────────────────────────────────
    print(f"\n[3/5] Cleaning text (mode: {source_type})...")
    for p in pages:
        before = len(p["text"])
        p["text"] = clean_text(p["text"], source_type)
        after = len(p["text"])
        print(f"  p{p['page']:>2}: {before} → {after} chars ({before-after} removed)")

    # ── Step 4: Build tree via LLM ─────────────────────────────────────────────
    print(f"\n[4/5] Building semantic tree (LLM)...")
    print("  Note: LLM reads raw page text — no regex heading detection needed")
    raw_tree = build_tree_from_raw(pages, llm, chars_per_page=3000)

    # ── Step 4b: Validate and repair ───────────────────────────────────────────
    print(f"\n  Validating and repairing tree structure...")
    tree = validate_and_repair_tree(raw_tree, all_page_numbers)
    print(f"  → {len(tree)} top-level nodes after validation")

    # ── Step 5: Verify quality ─────────────────────────────────────────────────
    print(f"\n[5/5] Tree quality check...")
    tree_ok = verify_tree_quality(tree)

    if not tree_ok:
        print("\n  [!] Tree has quality issues (see above).")
        print("  [!] Answers may still be reasonable — quality issues don't always")
        print("  [!] prevent correct navigation. Proceeding anyway.\n")
    else:
        print("  Tree looks good. Ready for queries.\n")

    # ── Interactive Q&A loop ───────────────────────────────────────────────────
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

        print("\n  Navigating tree...")
        page_ids = navigate_tree(query, tree, llm, top_k=2)
        print(f"  → Selected pages: {page_ids}")

        context = get_context(pages, page_ids)
        print(f"  → Context length: {len(context)} chars")

        if not context.strip():
            print("  [!] No context retrieved. Try rephrasing the question.\n")
            continue

        print("  Generating answer...\n")
        answer = answer_question(query, context)
        print(f"Answer:\n{answer}\n")
        print("-" * 60 + "\n")

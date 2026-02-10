#!/usr/bin/env python3
"""
run_comparison.py  –  Optimised contract-comparison pipeline
============================================================
Reads original + amended documents, calls Azure OpenAI, produces Excel.

SPEED OPTIMISATIONS (vs. naïve one-shot approach):
  1. Pre-segment both documents locally with regex.
  2. Auto-align clauses by matching labels  →  detect identical clauses.
  3. Send ONLY unmatched / changed clauses to the LLM  (70-90 % fewer tokens).
  4. Parallel batched LLM calls  →  N batches run concurrently (async).
  5. Normalise input text (collapse whitespace)  →  10-30 % fewer input tokens.
  6. Disk-cache LLM responses by content hash  →  instant re-runs.
  7. Timing instrumentation for every stage.

Use  --full  to bypass pre-segmentation and send everything to the LLM
(original behaviour, useful when the document has unusual formatting).

Usage:
    python run_comparison.py original.txt amended.txt -o comparison.xlsx
    python run_comparison.py original.docx amended.docx -o out.xlsx --save-json aligned.json
    python run_comparison.py orig.txt amd.txt -o out.xlsx --full   # skip pre-segmentation
"""

import argparse
import asyncio
import json
import hashlib
import os
import re
import sys
import time
from pathlib import Path

from dotenv import load_dotenv
from openai import AsyncAzureOpenAI, AzureOpenAI

from build_comparison_xlsx import generate_excel

# Maximum clauses per LLM batch (auto-fallback to 1 call if total ≤ this)
DEFAULT_BATCH_SIZE = 8

# ──────────────────────────────────────────────────────────────────────
# Timing helper
# ──────────────────────────────────────────────────────────────────────

class Timer:
    """Context manager that prints elapsed time for a labelled step."""
    def __init__(self, label: str):
        self.label = label
        self.start = 0.0
    def __enter__(self):
        self.start = time.perf_counter()
        return self
    def __exit__(self, *_):
        elapsed = time.perf_counter() - self.start
        print(f"  ⏱  {self.label}: {elapsed:.2f}s")


# ──────────────────────────────────────────────────────────────────────
# 1. Read & normalise documents
# ──────────────────────────────────────────────────────────────────────

def read_document(file_path: str) -> str:
    p = Path(file_path)
    if p.suffix.lower() == ".docx":
        try:
            from docx import Document as DocxDocument
        except ImportError:
            sys.exit("python-docx required for .docx – pip install python-docx")
        return "\n".join(para.text for para in DocxDocument(str(p)).paragraphs)
    return p.read_text(encoding="utf-8")


def normalise_text(text: str) -> str:
    """Collapse redundant whitespace to save tokens."""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)        # horizontal space
    text = re.sub(r"\n{3,}", "\n\n", text)     # max 2 blank lines
    return text.strip()


# ──────────────────────────────────────────────────────────────────────
# 2. Local clause segmentation
# ──────────────────────────────────────────────────────────────────────

# Matches lines that start with a legal clause number / heading
_CLAUSE_RE = re.compile(
    r"(?:^|\n)"
    r"("
    r"\d+(?:\.\d+)*\.?\s"                           # 1.  1.2  1.2.3
    r"|(?:Article|Section|Clause|Schedule|"
    r"Appendix|Annex|ARTICLE|SECTION|CLAUSE|"
    r"SCHEDULE|APPENDIX|ANNEX)\s+\S+"                # Article 1
    r"|\([a-z]\)\s"                                  # (a)
    r"|\([ivxlc]+\)\s"                               # (i) (ii)
    r")"
)


def segment_document(text: str) -> list[dict]:
    """
    Split a document into clause segments using legal-numbering regex.

    Returns
    -------
    list of {"idx": int, "label": str, "text": str}
    """
    positions = [m.start() for m in _CLAUSE_RE.finditer(text)]
    if not positions:
        # Can't segment → return the whole doc as one block
        return [{"idx": 0, "label": "Full Document", "text": text}]

    # If the doc starts before the first match, capture a "Preamble" block
    segments = []
    if positions[0] > 0:
        preamble = text[: positions[0]].strip()
        if preamble:
            segments.append({"idx": 0, "label": "Preamble", "text": preamble})

    for i, start in enumerate(positions):
        end = positions[i + 1] if i + 1 < len(positions) else len(text)
        chunk = text[start:end].strip()
        # Extract label from the beginning of the chunk
        first_line = chunk.split("\n")[0].strip()
        lm = re.match(
            r"(\d+(?:\.\d+)*\.?|\([a-z]\)|\([ivxlc]+\)"
            r"|(?:Article|Section|Clause|Schedule|Appendix|Annex)\s+\S+)",
            first_line,
            re.IGNORECASE,
        )
        label = lm.group(0).strip().rstrip(".") if lm else first_line[:40]
        segments.append({
            "idx": len(segments),
            "label": label,
            "text": chunk,
        })
    return segments


# ──────────────────────────────────────────────────────────────────────
# 3. Auto-align + identical-clause detection
# ──────────────────────────────────────────────────────────────────────

def auto_align(orig_segs: list[dict], amd_segs: list[dict]):
    """
    Match segments by label.

    Returns
    -------
    aligned          : list[(orig_seg, amd_seg, is_identical)]
    unmatched_orig   : list[orig_seg]   – deleted or renumbered
    unmatched_amd    : list[amd_seg]    – new or renumbered
    """
    amd_by_label: dict[str, list[dict]] = {}
    for seg in amd_segs:
        amd_by_label.setdefault(seg["label"], []).append(seg)

    aligned = []
    used_amd_idx: set[int] = set()
    unmatched_orig = []

    for oseg in orig_segs:
        candidates = amd_by_label.get(oseg["label"], [])
        matched = None
        for c in candidates:
            if c["idx"] not in used_amd_idx:
                matched = c
                used_amd_idx.add(c["idx"])
                break
        if matched:
            identical = oseg["text"].strip() == matched["text"].strip()
            aligned.append((oseg, matched, identical))
        else:
            unmatched_orig.append(oseg)

    unmatched_amd = [s for s in amd_segs if s["idx"] not in used_amd_idx]
    return aligned, unmatched_orig, unmatched_amd


# ──────────────────────────────────────────────────────────────────────
# 4. Disk cache (keyed on content hash)
# ──────────────────────────────────────────────────────────────────────

CACHE_DIR = Path(__file__).parent / ".llm_cache"


def _content_hash(*parts: str) -> str:
    h = hashlib.sha256()
    for p in parts:
        h.update(p.encode())
    return h.hexdigest()[:16]


def _load_cache(key: str) -> dict | None:
    f = CACHE_DIR / f"{key}.json"
    if f.exists():
        print(f"  💾 Cache hit ({key})")
        return json.loads(f.read_text(encoding="utf-8"))
    return None


def _save_cache(key: str, data: dict):
    CACHE_DIR.mkdir(exist_ok=True)
    (CACHE_DIR / f"{key}.json").write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )


# ──────────────────────────────────────────────────────────────────────
# 5. Azure OpenAI calls
# ──────────────────────────────────────────────────────────────────────

def _get_client():
    load_dotenv()
    endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
    api_key = os.getenv("AZURE_OPENAI_API_KEY")
    api_version = os.getenv("AZURE_OPENAI_API_VERSION", "2024-12-01-preview")
    deployment = os.getenv("AZURE_OPENAI_DEPLOYMENT")
    if not all([endpoint, api_key, deployment]):
        sys.exit(
            "Missing Azure OpenAI config.\n"
            "Set AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_API_KEY, "
            "AZURE_OPENAI_DEPLOYMENT in .env or environment."
        )
    client = AzureOpenAI(
        azure_endpoint=endpoint,
        api_key=api_key,
        api_version=api_version,
    )
    return client, deployment


def _extract_json(raw: str) -> dict:
    """Robustly parse JSON from LLM output (handles code fences, etc.)."""
    import re as _re
    m = _re.search(r"```(?:json)?\s*\n?(.*?)```", raw, _re.DOTALL)
    if m:
        return json.loads(m.group(1).strip())
    try:
        return json.loads(raw.strip())
    except json.JSONDecodeError:
        pass
    start, end = raw.find("{"), raw.rfind("}")
    if start != -1 and end != -1:
        return json.loads(raw[start : end + 1])
    raise ValueError("Could not extract JSON from LLM response.")


def _call_llm(client, deployment, prompt: str, max_tokens: int = 16_000) -> dict:
    """Single Azure OpenAI chat call with timing + token stats."""
    t0 = time.perf_counter()
    resp = client.chat.completions.create(
        model=deployment,
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a legal document comparison engine. "
                    "Return ONLY valid JSON. No markdown fences, no prose."
                ),
            },
            {"role": "user", "content": prompt},
        ],
        temperature=0,
        max_tokens=max_tokens,
        response_format={"type": "json_object"},
    )
    elapsed = time.perf_counter() - t0
    raw = resp.choices[0].message.content
    usage = resp.usage
    print(
        f"  ⏱  LLM call: {elapsed:.1f}s  |  "
        f"{usage.prompt_tokens:,} prompt → {usage.completion_tokens:,} completion tokens"
    )
    return _extract_json(raw)


# ──────────────────────────────────────────────────────────────────────
# 5b. Async Azure OpenAI – parallel batched calls
# ──────────────────────────────────────────────────────────────────────

def _get_async_client():
    """Return an async Azure OpenAI client + deployment name."""
    load_dotenv()
    endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
    api_key = os.getenv("AZURE_OPENAI_API_KEY")
    api_version = os.getenv("AZURE_OPENAI_API_VERSION", "2024-12-01-preview")
    deployment = os.getenv("AZURE_OPENAI_DEPLOYMENT")
    if not all([endpoint, api_key, deployment]):
        sys.exit(
            "Missing Azure OpenAI config.\n"
            "Set AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_API_KEY, "
            "AZURE_OPENAI_DEPLOYMENT in .env or environment."
        )
    client = AsyncAzureOpenAI(
        azure_endpoint=endpoint,
        api_key=api_key,
        api_version=api_version,
    )
    return client, deployment


async def _async_call_llm(
    client: AsyncAzureOpenAI,
    deployment: str,
    prompt: str,
    batch_id: int,
    max_tokens: int = 16_000,
) -> dict:
    """Single async LLM call. Returns parsed JSON + timing info."""
    t0 = time.perf_counter()
    resp = await client.chat.completions.create(
        model=deployment,
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a legal document comparison engine. "
                    "Return ONLY valid JSON. No markdown fences, no prose."
                ),
            },
            {"role": "user", "content": prompt},
        ],
        temperature=0,
        max_tokens=max_tokens,
        response_format={"type": "json_object"},
    )
    elapsed = time.perf_counter() - t0
    raw = resp.choices[0].message.content
    usage = resp.usage
    print(
        f"    batch {batch_id}: {elapsed:.1f}s  |  "
        f"{usage.prompt_tokens:,} prompt → {usage.completion_tokens:,} completion"
    )
    return _extract_json(raw)


def _chunk_list(lst: list, n: int) -> list[list]:
    """Split a list into chunks of at most n items."""
    return [lst[i : i + n] for i in range(0, len(lst), n)]


def _build_batch_prompt(
    changed_pairs: list[tuple[dict, dict]],
    unmatched_orig: list[dict],
    unmatched_amd: list[dict],
) -> str:
    """
    Build a lean prompt for a single batch of clauses.
    Same format as _build_lean_prompt but for a subset.
    """
    lines = [
        "You are a legal document comparison engine.",
        "",
        "Below are clauses extracted from an Original and an Amended contract.",
        "These clauses could NOT be auto-aligned or are textually different.",
        "",
        "TASK:",
        "1) Align each Original clause to its Amended counterpart (or mark as deleted/new).",
        "2) Preserve clause text EXACTLY as written.",
        "3) Do NOT interpret or summarise legal meaning.",
        "",
        "OUTPUT JSON ONLY:",
        '{',
        '  \"clauses\": [',
        '    {',
        '      \"clause_key\": \"...\",',
        '      \"clause_label_original\": \"...\",',
        '      \"clause_label_amended\": \"...\",',
        '      \"clause_name\": \"...\",',
        '      \"original_text\": \"FULL TEXT or empty if new clause\",',
        '      \"amended_text\": \"FULL TEXT or empty if deleted clause\",',
        '      \"match_confidence\": 0.0,',
        '      \"match_basis\": \"numbering|heading|text_similarity\"',
        '    }',
        '  ]',
        '}',
        "",
    ]
    if changed_pairs:
        lines.append("=== CHANGED CLAUSES (matched by label, text differs) ===")
        for o, a in changed_pairs:
            lines.append(f'\n--- ORIGINAL [{o["label"]}] ---')
            lines.append(o["text"])
            lines.append(f'\n--- AMENDED  [{a["label"]}] ---')
            lines.append(a["text"])
        lines.append("")
    if unmatched_orig:
        lines.append("=== UNMATCHED ORIGINAL CLAUSES (possibly deleted) ===")
        for seg in unmatched_orig:
            lines.append(f'\n--- ORIG [{seg["label"]}] ---')
            lines.append(seg["text"])
        lines.append("")
    if unmatched_amd:
        lines.append("=== UNMATCHED AMENDED CLAUSES (possibly new) ===")
        for seg in unmatched_amd:
            lines.append(f'\n--- AMD [{seg["label"]}] ---')
            lines.append(seg["text"])
    return "\n".join(lines)


async def _parallel_llm_calls(
    changed_pairs: list[tuple[dict, dict]],
    unmatched_orig: list[dict],
    unmatched_amd: list[dict],
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> list[dict]:
    """
    Split clause work into batches and fire LLM calls concurrently.

    Returns a flat list of clause dicts from all batches, in order.
    """
    # Build work items  –  each is (changed_pairs_chunk, unmatched_orig_chunk, unmatched_amd_chunk)
    changed_chunks = _chunk_list(changed_pairs, batch_size) if changed_pairs else [[]]
    uorig_chunks = _chunk_list(unmatched_orig, batch_size) if unmatched_orig else [[]]
    uamd_chunks = _chunk_list(unmatched_amd, batch_size) if unmatched_amd else [[]]

    # Merge into batch work items: distribute changed, unmatched_orig, unmatched_amd
    # across batches.  Strategy: first fill batches with changed pairs, then
    # spread unmatched across remaining slots.
    batches: list[tuple[list, list, list]] = []

    # Max batches needed
    n_batches = max(len(changed_chunks), len(uorig_chunks), len(uamd_chunks))
    for i in range(n_batches):
        cp = changed_chunks[i] if i < len(changed_chunks) else []
        uo = uorig_chunks[i] if i < len(uorig_chunks) else []
        ua = uamd_chunks[i] if i < len(uamd_chunks) else []
        # Only add batch if there's actual work
        if cp or uo or ua:
            batches.append((cp, uo, ua))

    if not batches:
        return []

    # Single batch → no overhead of async, just use sync
    if len(batches) == 1:
        cp, uo, ua = batches[0]
        prompt = _build_batch_prompt(cp, uo, ua)
        client_sync, deployment = _get_client()
        print(f"\n📤 Single batch ({len(cp)} changed + {len(uo)} deleted + {len(ua)} new clauses)")
        result = _call_llm(client_sync, deployment, prompt)
        return result.get("clauses", [])

    # Multiple batches → fire concurrently
    print(f"\n📤 Parallel: {len(batches)} batches (batch_size={batch_size})")
    client_async, deployment = _get_async_client()

    prompts = [_build_batch_prompt(cp, uo, ua) for cp, uo, ua in batches]
    for i, p in enumerate(prompts):
        print(f"    batch {i}: {len(p):,} chars")

    t0 = time.perf_counter()
    tasks = [
        _async_call_llm(client_async, deployment, prompt, batch_id=i)
        for i, prompt in enumerate(prompts)
    ]
    results = await asyncio.gather(*tasks)
    elapsed = time.perf_counter() - t0
    print(f"  ⏱  All {len(batches)} batches completed in {elapsed:.1f}s (wall clock)")

    await client_async.close()

    # Flatten clauses in order
    all_clauses: list[dict] = []
    for r in results:
        all_clauses.extend(r.get("clauses", []))
    return all_clauses


# ──────────────────────────────────────────────────────────────────────
# 6. Prompt builders
# ──────────────────────────────────────────────────────────────────────

PROMPT_TEMPLATE_PATH = Path(__file__).parent / "prompt_template.txt"


def _build_full_prompt(orig_text: str, amd_text: str) -> str:
    """Full prompt (original behaviour): send both entire documents."""
    tpl = PROMPT_TEMPLATE_PATH.read_text(encoding="utf-8")
    return tpl.replace("{original_text}", orig_text).replace("{amended_text}", amd_text)


def _build_lean_prompt(
    changed_orig: list[dict],
    changed_amd: list[dict],
    unmatched_orig: list[dict],
    unmatched_amd: list[dict],
) -> str:
    """
    Lean prompt: only include clauses that differ or couldn't be matched.
    Much smaller than the full prompt for typical amendments.
    """
    lines = [
        "You are a legal document comparison engine.",
        "",
        "Below are two sets of clauses extracted from an Original and an Amended contract.",
        "These clauses could NOT be auto-aligned or are textually different.",
        "",
        "TASK:",
        "1) Align each Original clause to its Amended counterpart (or mark as deleted/new).",
        "2) Preserve clause text EXACTLY as written.",
        "3) Do NOT interpret or summarise legal meaning.",
        "",
        "OUTPUT JSON ONLY:",
        '{',
        '  "clauses": [',
        '    {',
        '      "clause_key": "...",',
        '      "clause_label_original": "...",',
        '      "clause_label_amended": "...",',
        '      "clause_name": "...",',
        '      "original_text": "FULL TEXT or empty if new clause",',
        '      "amended_text": "FULL TEXT or empty if deleted clause",',
        '      "match_confidence": 0.0,',
        '      "match_basis": "numbering|heading|text_similarity"',
        '    }',
        '  ]',
        '}',
        "",
    ]

    # Changed clauses (matched by label but text differs)
    if changed_orig:
        lines.append("=== CHANGED CLAUSES (matched by label, text differs) ===")
        for o, a in zip(changed_orig, changed_amd):
            lines.append(f'\n--- ORIGINAL [{o["label"]}] ---')
            lines.append(o["text"])
            lines.append(f'\n--- AMENDED  [{a["label"]}] ---')
            lines.append(a["text"])
        lines.append("")

    # Unmatched originals (possibly deleted or renumbered)
    if unmatched_orig:
        lines.append("=== UNMATCHED ORIGINAL CLAUSES (possibly deleted) ===")
        for seg in unmatched_orig:
            lines.append(f'\n--- ORIG [{seg["label"]}] ---')
            lines.append(seg["text"])
        lines.append("")

    # Unmatched amended (possibly new or renumbered)
    if unmatched_amd:
        lines.append("=== UNMATCHED AMENDED CLAUSES (possibly new) ===")
        for seg in unmatched_amd:
            lines.append(f'\n--- AMD [{seg["label"]}] ---')
            lines.append(seg["text"])

    return "\n".join(lines)


# ──────────────────────────────────────────────────────────────────────
# 7. Assemble final output JSON
# ──────────────────────────────────────────────────────────────────────

def _assemble_output(
    identical_aligned: list[tuple[dict, dict]],
    changed_aligned: list[tuple[dict, dict]],
    llm_clauses: list[dict],
) -> dict:
    """Merge locally-aligned clauses with LLM-aligned clauses."""
    clauses = []

    # Identical clauses (no LLM needed)
    for oseg, aseg in identical_aligned:
        clauses.append({
            "clause_key": f"{oseg['label']}|{oseg['label']}",
            "clause_label_original": oseg["label"],
            "clause_label_amended": aseg["label"],
            "clause_name": oseg["label"],
            "original_text": oseg["text"],
            "amended_text": aseg["text"],
            "match_confidence": 1.0,
            "match_basis": "label_match_identical",
        })

    # Changed clauses that were label-matched but text differs
    # The LLM may have re-aligned them, so use LLM output for these
    # (LLM receives both changed + unmatched in one call)

    # LLM-aligned clauses
    clauses.extend(llm_clauses)

    return {
        "meta": {
            "original_title": "",
            "amended_title": "",
            "excluded_rule": "Exclude anything marked Commented",
        },
        "clauses": clauses,
    }


# ──────────────────────────────────────────────────────────────────────
# 8. Main pipeline
# ──────────────────────────────────────────────────────────────────────

def run_pipeline(
    original_path: str,
    amended_path: str,
    output_path: str = "comparison.xlsx",
    save_json: str | None = None,
    full_mode: bool = False,
    batch_size: int = DEFAULT_BATCH_SIZE,
):
    t_total = time.perf_counter()

    # ── Read ─────────────────────────────────────────────────────────
    with Timer("Read documents"):
        orig_raw = read_document(original_path)
        amd_raw = read_document(amended_path)

    # ── Normalise ────────────────────────────────────────────────────
    with Timer("Normalise text"):
        orig = normalise_text(orig_raw)
        amd = normalise_text(amd_raw)
        saved_chars = (len(orig_raw) + len(amd_raw)) - (len(orig) + len(amd))
        if saved_chars > 0:
            print(f"       ↳ stripped {saved_chars:,} redundant characters")

    # ── Cache check ──────────────────────────────────────────────────
    cache_key = _content_hash(orig, amd, "full" if full_mode else "fast")
    cached = _load_cache(cache_key)
    if cached:
        with Timer("Generate Excel (from cache)"):
            generate_excel(cached, output_path)
        print(f"\n✅ Done in {time.perf_counter() - t_total:.1f}s → {output_path}")
        return

    client, deployment = _get_client()

    if full_mode:
        # ── FULL MODE: send everything to LLM ────────────────────────
        print("\n📤 Full mode: sending both complete documents to LLM…")
        prompt = _build_full_prompt(orig, amd)
        with Timer("LLM (full)"):
            aligned_json = _call_llm(client, deployment, prompt)

    else:
        # ── FAST MODE: pre-segment + auto-align ──────────────────────
        with Timer("Pre-segment documents"):
            orig_segs = segment_document(orig)
            amd_segs = segment_document(amd)
            print(f"       ↳ original: {len(orig_segs)} segments, amended: {len(amd_segs)} segments")

        with Timer("Auto-align clauses"):
            aligned, unmatched_orig, unmatched_amd = auto_align(orig_segs, amd_segs)
            identical = [(o, a) for o, a, ident in aligned if ident]
            changed = [(o, a) for o, a, ident in aligned if not ident]
            print(
                f"       ↳ {len(identical)} identical (skipped)  |  "
                f"{len(changed)} changed  |  "
                f"{len(unmatched_orig)} deleted?  |  "
                f"{len(unmatched_amd)} new?"
            )

        need_llm = changed or unmatched_orig or unmatched_amd
        llm_clauses = []

        if need_llm:
            changed_pairs = list(changed)  # list of (orig_seg, amd_seg)
            total_items = len(changed_pairs) + len(unmatched_orig) + len(unmatched_amd)
            print(
                f"\n📤 {total_items} clause(s) need LLM alignment "
                f"(batch_size={batch_size})"
            )
            with Timer("LLM (parallel batches)"):
                llm_clauses = asyncio.run(
                    _parallel_llm_calls(
                        changed_pairs, unmatched_orig, unmatched_amd,
                        batch_size=batch_size,
                    )
                )
        else:
            print("\n⚡ All clauses matched identically — no LLM call needed!")

        aligned_json = _assemble_output(identical, changed, llm_clauses)

    # ── Save cache ───────────────────────────────────────────────────
    _save_cache(cache_key, aligned_json)

    # ── Save intermediate JSON ───────────────────────────────────────
    if save_json:
        Path(save_json).write_text(
            json.dumps(aligned_json, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        print(f"💾 JSON saved: {save_json}")

    # ── Generate Excel ───────────────────────────────────────────────
    with Timer("Generate Excel"):
        generate_excel(aligned_json, output_path)

    total = time.perf_counter() - t_total
    print(f"\n✅ Done in {total:.1f}s → {output_path}")


# ──────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(
        description="Contract comparison: documents → Azure OpenAI → Excel (optimised)."
    )
    ap.add_argument("original", help="Path to original document (.txt / .docx)")
    ap.add_argument("amended", help="Path to amended document (.txt / .docx)")
    ap.add_argument("-o", "--output", default="comparison.xlsx", help="Output .xlsx path")
    ap.add_argument("--save-json", metavar="PATH", help="Save intermediate JSON")
    ap.add_argument(
        "--full",
        action="store_true",
        help="Skip pre-segmentation, send full documents to LLM (slower but handles unusual formatting)",
    )
    ap.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        metavar="N",
        help=f"Max clauses per LLM batch (default: {DEFAULT_BATCH_SIZE}). "
             "Batches run in parallel for faster processing.",
    )
    args = ap.parse_args()

    run_pipeline(
        args.original,
        args.amended,
        args.output,
        save_json=args.save_json,
        full_mode=args.full,
        batch_size=args.batch_size,
    )


if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
build_comparison_xlsx.py
========================
Deterministic post-processing: LLM-aligned clause JSON → Excel workbook
with redline-style diff in the Amended column and bullet-point recap.

The Amended Clause column shows a merged "redline" view:
  - Deleted text  → red, strikethrough
  - Inserted text → underlined, bold

The Original Clause column shows verbatim original text (no formatting).

Usage:
    python build_comparison_xlsx.py aligned_output.json comparison.xlsx
"""

import argparse
import json
import difflib
from pathlib import Path

import xlsxwriter


# ---------------------------------------------------------------------------
# Token-level diff  →  single redline stream
# ---------------------------------------------------------------------------

def diff_clause(orig: str, amd: str):
    """
    Produce a redline token stream and a recap list.

    Returns
    -------
    redline : list[tuple[str, str]]
        Each element is (token, style) where style is "equal", "delete",
        or "insert".  The stream reads like the amended text but with
        deleted tokens inserted inline (shown as strikethrough).
    recap   : list[str]
        Human-readable change descriptions (bullet-ready).
    """
    a_toks = orig.split()
    b_toks = amd.split()

    sm = difflib.SequenceMatcher(None, a_toks, b_toks, autojunk=False)

    redline: list[tuple[str, str]] = []
    recap: list[str] = []

    for tag, i1, i2, j1, j2 in sm.get_opcodes():
        if tag == "equal":
            for t in b_toks[j1:j2]:
                redline.append((t, "equal"))

        elif tag == "replace":
            old_span = " ".join(a_toks[i1:i2])
            new_span = " ".join(b_toks[j1:j2])
            recap.append(f'"{old_span}" changed to "{new_span}"')
            # Show deleted tokens (strikethrough) then inserted tokens (underline)
            for t in a_toks[i1:i2]:
                redline.append((t, "delete"))
            for t in b_toks[j1:j2]:
                redline.append((t, "insert"))

        elif tag == "delete":
            removed = " ".join(a_toks[i1:i2])
            recap.append(f'"{removed}" removed')
            for t in a_toks[i1:i2]:
                redline.append((t, "delete"))

        elif tag == "insert":
            added = " ".join(b_toks[j1:j2])
            recap.append(f'"{added}" added')
            for t in b_toks[j1:j2]:
                redline.append((t, "insert"))

    return redline, recap


# ---------------------------------------------------------------------------
# Rich-text helper for xlsxwriter
# ---------------------------------------------------------------------------

def redline_to_rich(redline, del_fmt, ins_fmt, normal_fmt):
    """
    Convert a redline token stream into xlsxwriter ``write_rich_string`` args.

    Returns a list of alternating [format, string, format, string, ...].
    """
    if not redline:
        return []

    fragments: list = []
    for i, (tok, style) in enumerate(redline):
        text = tok if i == 0 else " " + tok
        if style == "delete":
            fragments.append(del_fmt)
            fragments.append(text)
        elif style == "insert":
            fragments.append(ins_fmt)
            fragments.append(text)
        else:
            fragments.append(normal_fmt)
            fragments.append(text)
    return fragments


def _rich_string_count(fragments):
    """Count the number of text fragments (strings) in a rich-string arg list."""
    return sum(1 for x in fragments if isinstance(x, str))


# ---------------------------------------------------------------------------
# Excel generation
# ---------------------------------------------------------------------------

def generate_excel(aligned_json: dict, output_path: str):
    wb = xlsxwriter.Workbook(output_path, {"strings_to_urls": False})

    # ---- Header formats -------------------------------------------------
    header_green = wb.add_format({
        "bold": True, "font_color": "#FFFFFF", "bg_color": "#1F5E43",
        "border": 1, "text_wrap": True, "valign": "vcenter",
        "font_size": 11, "font_name": "Calibri",
    })
    header_green2 = wb.add_format({
        "bold": True, "font_color": "#FFFFFF", "bg_color": "#2E7D5A",
        "border": 1, "text_wrap": True, "valign": "vcenter",
        "font_size": 11, "font_name": "Calibri",
    })
    header_orange = wb.add_format({
        "bold": True, "font_color": "#FFFFFF", "bg_color": "#B25A1B",
        "border": 1, "text_wrap": True, "valign": "vcenter",
        "font_size": 11, "font_name": "Calibri",
    })
    header_orange2 = wb.add_format({
        "bold": True, "font_color": "#FFFFFF", "bg_color": "#C46A22",
        "border": 1, "text_wrap": True, "valign": "vcenter",
        "font_size": 11, "font_name": "Calibri",
    })

    # ---- Cell formats ---------------------------------------------------
    cell_wrap = wb.add_format({
        "text_wrap": True, "valign": "top", "border": 1,
        "font_size": 10, "font_name": "Calibri",
    })
    recap_wrap = wb.add_format({
        "text_wrap": True, "valign": "top", "border": 1,
        "font_size": 9, "font_color": "#333333", "font_name": "Calibri",
    })

    # ---- Rich-text fragment formats (used inside write_rich_string) -----
    normal_fmt = wb.add_format({
        "font_size": 10, "font_name": "Calibri",
    })
    del_fmt = wb.add_format({
        "font_strikeout": True,
        "font_color": "#B91C1C",        # red
        "bg_color": "#FDE8E8",          # light red background
        "font_size": 10, "font_name": "Calibri",
    })
    ins_fmt = wb.add_format({
        "underline": True,
        "bold": True,
        "font_color": "#065F46",         # dark green
        "bg_color": "#FFF7CC",           # light yellow background
        "font_size": 10, "font_name": "Calibri",
    })
    # For entire-clause-deleted: all text strikethrough
    full_del_fmt = wb.add_format({
        "font_strikeout": True,
        "font_color": "#B91C1C",
        "bg_color": "#FDE8E8",
        "text_wrap": True, "valign": "top", "border": 1,
        "font_size": 10, "font_name": "Calibri",
    })
    # For entire-clause-added: all text underlined
    full_ins_fmt = wb.add_format({
        "underline": True,
        "bold": True,
        "font_color": "#065F46",
        "bg_color": "#FFF7CC",
        "text_wrap": True, "valign": "top", "border": 1,
        "font_size": 10, "font_name": "Calibri",
    })

    ws = wb.add_worksheet("Comparison")

    # Column widths
    ws.set_column(0, 0, 38)
    ws.set_column(1, 1, 75)
    ws.set_column(2, 2, 75)
    ws.set_column(3, 3, 45)

    # Headers
    ws.write(0, 0, "Clause Name", header_green)
    ws.write(0, 1, "Original Clause", header_green2)
    ws.write(0, 2, "Amended Clause", header_orange)
    ws.write(0, 3, "Change in Clause", header_orange2)
    ws.freeze_panes(1, 0)

    row = 1
    for c in aligned_json.get("clauses", []):
        clause_name = c.get("clause_name") or ""
        label_o = c.get("clause_label_original") or ""
        label_a = c.get("clause_label_amended") or ""

        display_name = f"{label_o} {clause_name}".strip() if label_o else clause_name
        if label_a and label_a != label_o:
            display_name = f"{display_name} (→ {label_a})"

        orig = c.get("original_text") or ""
        amd = c.get("amended_text") or ""

        ws.write(row, 0, display_name, cell_wrap)

        # ---- Entire clause deleted ----------------------------------
        # Original col: plain text.  Amended col: full text strikethrough.
        if orig.strip() and not amd.strip():
            ws.write(row, 1, orig, cell_wrap)
            ws.write(row, 2, orig, full_del_fmt)
            ws.write(row, 3, "Entire clause deleted", recap_wrap)
            ws.set_row(row, 160)
            row += 1
            continue

        # ---- New clause added ---------------------------------------
        # Original col: empty.  Amended col: full text underlined.
        if amd.strip() and not orig.strip():
            ws.write(row, 1, "", cell_wrap)
            ws.write(row, 2, amd, full_ins_fmt)
            ws.write(row, 3, "New clause added", recap_wrap)
            ws.set_row(row, 160)
            row += 1
            continue

        # ---- Both present: diff them --------------------------------
        redline, recap = diff_clause(orig, amd)
        has_changes = any(s != "equal" for _, s in redline)

        # Original column: always plain text
        ws.write(row, 1, orig, cell_wrap)

        if not has_changes:
            # No differences — plain text in Amended too
            ws.write(row, 2, amd, cell_wrap)
            ws.write(row, 3, "No change", recap_wrap)
        else:
            # Build rich-text redline for the Amended column
            rich = redline_to_rich(redline, del_fmt, ins_fmt, normal_fmt)

            if _rich_string_count(rich) >= 2:
                ws.write_rich_string(row, 2, *rich, cell_wrap)
            else:
                ws.write(row, 2, amd, cell_wrap)

            # Recap as bullet list
            recap_text = "\n".join(f"• {line}" for line in recap)
            ws.write(row, 3, recap_text, recap_wrap)

        # Row-height heuristic
        approx = max(len(orig), len(amd))
        ws.set_row(row, min(360, max(90, int(approx / 4))))
        row += 1

    wb.close()
    return output_path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(
        description="Convert LLM-aligned clause JSON to a 4-column comparison Excel workbook."
    )
    ap.add_argument("input_json", help="Path to LLM output JSON")
    ap.add_argument("output_xlsx", help="Path to output .xlsx file")
    args = ap.parse_args()

    with open(args.input_json, "r", encoding="utf-8") as f:
        data = json.load(f)

    out = generate_excel(data, args.output_xlsx)
    print(f"✅ Saved: {out}")


if __name__ == "__main__":
    main()
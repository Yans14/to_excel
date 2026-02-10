#!/usr/bin/env python3
"""
build_comparison_xlsx.py
========================
Deterministic post-processing: LLM-aligned clause JSON → Excel workbook
with bold-highlighted token diffs and line-by-line recap.

Usage:
    python build_comparison_xlsx.py aligned_output.json comparison.xlsx
"""

import argparse
import json
import difflib
from pathlib import Path

import xlsxwriter


# ---------------------------------------------------------------------------
# Token-level diff
# ---------------------------------------------------------------------------

def diff_clause(orig: str, amd: str):
    """
    Token-level diff between two clause texts.

    Returns
    -------
    a_tokens   : list[str]   – tokens for the Original column
    a_changed  : list[bool]  – True where the token differs
    b_tokens   : list[str]   – tokens for the Amended column
    b_changed  : list[bool]  – True where the token differs
    recap      : list[str]   – human-readable change descriptions
    """
    a_toks = orig.split()
    b_toks = amd.split()

    sm = difflib.SequenceMatcher(None, a_toks, b_toks, autojunk=False)

    a_tokens: list[str] = []
    a_changed: list[bool] = []
    b_tokens: list[str] = []
    b_changed: list[bool] = []
    recap: list[str] = []

    for tag, i1, i2, j1, j2 in sm.get_opcodes():
        if tag == "equal":
            for t in a_toks[i1:i2]:
                a_tokens.append(t)
                a_changed.append(False)
            for t in b_toks[j1:j2]:
                b_tokens.append(t)
                b_changed.append(False)

        elif tag == "replace":
            old_span = " ".join(a_toks[i1:i2])
            new_span = " ".join(b_toks[j1:j2])
            recap.append(f'"{old_span}" changed to "{new_span}"')
            for t in a_toks[i1:i2]:
                a_tokens.append(t)
                a_changed.append(True)
            for t in b_toks[j1:j2]:
                b_tokens.append(t)
                b_changed.append(True)

        elif tag == "delete":
            removed = " ".join(a_toks[i1:i2])
            recap.append(f'"{removed}" removed')
            for t in a_toks[i1:i2]:
                a_tokens.append(t)
                a_changed.append(True)

        elif tag == "insert":
            added = " ".join(b_toks[j1:j2])
            recap.append(f'"{added}" added')
            for t in b_toks[j1:j2]:
                b_tokens.append(t)
                b_changed.append(True)

    return a_tokens, a_changed, b_tokens, b_changed, recap


# ---------------------------------------------------------------------------
# Rich-text helper for xlsxwriter
# ---------------------------------------------------------------------------

def tokens_to_rich(tokens: list[str], changed: list[bool], bold_fmt, normal_fmt):
    """
    Build an argument list for ``worksheet.write_rich_string()``.

    xlsxwriter rich strings alternate *format, string* pairs.
    A leading format applies to the first fragment.
    """
    if not tokens:
        return []

    fragments: list = []
    for i, (tok, is_bold) in enumerate(zip(tokens, changed)):
        text = tok if i == 0 else " " + tok
        if is_bold:
            fragments.append(bold_fmt)
            fragments.append(text)
        else:
            fragments.append(normal_fmt)
            fragments.append(text)
    return fragments


# ---------------------------------------------------------------------------
# Excel generation
# ---------------------------------------------------------------------------

def generate_excel(aligned_json: dict, output_path: str):
    wb = xlsxwriter.Workbook(output_path, {"strings_to_urls": False})

    # ---- Formats --------------------------------------------------------
    header_green = wb.add_format({
        "bold": True, "font_color": "#FFFFFF", "bg_color": "#4472C4",
        "border": 1, "text_wrap": True, "valign": "vcenter",
        "font_size": 11,
    })
    header_green2 = wb.add_format({
        "bold": True, "font_color": "#FFFFFF", "bg_color": "#548235",
        "border": 1, "text_wrap": True, "valign": "vcenter",
        "font_size": 11,
    })
    header_orange = wb.add_format({
        "bold": True, "font_color": "#FFFFFF", "bg_color": "#C55A11",
        "border": 1, "text_wrap": True, "valign": "vcenter",
        "font_size": 11,
    })
    header_orange2 = wb.add_format({
        "bold": True, "font_color": "#FFFFFF", "bg_color": "#BF8F00",
        "border": 1, "text_wrap": True, "valign": "vcenter",
        "font_size": 11,
    })
    cell_wrap = wb.add_format({
        "text_wrap": True, "valign": "top", "border": 1,
        "font_size": 10,
    })
    recap_wrap = wb.add_format({
        "text_wrap": True, "valign": "top", "border": 1,
        "font_size": 9, "font_color": "#333333",
    })
    bold_fmt = wb.add_format({
        "bold": True, "font_color": "#C00000",
        "text_wrap": True, "valign": "top",
        "font_size": 10,
    })
    normal_fmt = wb.add_format({
        "text_wrap": True, "valign": "top",
        "font_size": 10,
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
        if orig.strip() and not amd.strip():
            ws.write(row, 1, orig, cell_wrap)
            ws.write(row, 2, "", cell_wrap)
            ws.write(row, 3, "Entire clause deleted", recap_wrap)
            ws.set_row(row, 160)
            row += 1
            continue

        # ---- New clause added ---------------------------------------
        if amd.strip() and not orig.strip():
            ws.write(row, 1, "", cell_wrap)
            ws.write(row, 2, amd, cell_wrap)
            ws.write(row, 3, "New clause added", recap_wrap)
            ws.set_row(row, 160)
            row += 1
            continue

        # ---- Both present: diff them --------------------------------
        a_tokens, a_changed, b_tokens, b_changed, recap = diff_clause(orig, amd)

        # If no changes at all → plain write (write_rich_string needs ≥2 fragments)
        if not any(a_changed) and not any(b_changed):
            ws.write(row, 1, orig, cell_wrap)
            ws.write(row, 2, amd, cell_wrap)
            ws.write(row, 3, "No change", recap_wrap)
        else:
            rich_a = tokens_to_rich(a_tokens, a_changed, bold_fmt, normal_fmt)
            rich_b = tokens_to_rich(b_tokens, b_changed, bold_fmt, normal_fmt)

            # write_rich_string needs at least 2 string fragments
            if len([x for x in rich_a if isinstance(x, str)]) >= 2:
                ws.write_rich_string(row, 1, *rich_a, cell_wrap)
            else:
                ws.write(row, 1, orig, cell_wrap)

            if len([x for x in rich_b if isinstance(x, str)]) >= 2:
                ws.write_rich_string(row, 2, *rich_b, cell_wrap)
            else:
                ws.write(row, 2, amd, cell_wrap)

            ws.write(row, 3, "\n".join(recap), recap_wrap)

        # Simple row-height heuristic
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

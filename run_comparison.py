#!/usr/bin/env python3
"""
run_comparison.py
=================
End-to-end pipeline:
  1. Read original + amended documents (plain text or .docx)
  2. Call Azure OpenAI (one-shot) to get clause-aligned JSON
  3. Feed JSON into build_comparison_xlsx to produce the Excel workbook

Usage:
    python run_comparison.py original.txt amended.txt -o comparison.xlsx

    # Or with .docx files (requires python-docx):
    python run_comparison.py original.docx amended.docx -o comparison.xlsx

Environment variables (set in .env or export):
    AZURE_OPENAI_ENDPOINT     – e.g. https://my-resource.openai.azure.com/
    AZURE_OPENAI_API_KEY      – your Azure OpenAI key
    AZURE_OPENAI_API_VERSION  – e.g. 2024-12-01-preview
    AZURE_OPENAI_DEPLOYMENT   – your model deployment name (e.g. gpt-4o)
"""

import argparse
import json
import re
import sys
from pathlib import Path

from dotenv import load_dotenv
import os

from openai import AzureOpenAI

from build_comparison_xlsx import generate_excel

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

PROMPT_TEMPLATE = Path(__file__).parent / "prompt_template.txt"


def read_document(file_path: str) -> str:
    """Read a .txt or .docx file and return its text content."""
    p = Path(file_path)

    if p.suffix.lower() == ".docx":
        try:
            from docx import Document as DocxDocument
        except ImportError:
            sys.exit(
                "python-docx is required for .docx files. "
                "Install it: pip install python-docx"
            )
        doc = DocxDocument(str(p))
        return "\n".join(para.text for para in doc.paragraphs)

    # Default: plain text
    return p.read_text(encoding="utf-8")


def build_prompt(original_text: str, amended_text: str) -> str:
    """Load the prompt template and inject both documents."""
    template = PROMPT_TEMPLATE.read_text(encoding="utf-8")
    prompt = template.replace("{original_text}", original_text)
    prompt = prompt.replace("{amended_text}", amended_text)
    return prompt


def extract_json(raw_response: str) -> dict:
    """
    Robustly extract JSON from the LLM response.
    Handles markdown code fences, leading prose, etc.
    """
    # Try to find a JSON code block first
    m = re.search(r"```(?:json)?\s*\n?(.*?)```", raw_response, re.DOTALL)
    if m:
        return json.loads(m.group(1).strip())

    # Try the whole string
    try:
        return json.loads(raw_response.strip())
    except json.JSONDecodeError:
        pass

    # Find the first { ... last }
    start = raw_response.find("{")
    end = raw_response.rfind("}")
    if start != -1 and end != -1:
        return json.loads(raw_response[start : end + 1])

    raise ValueError("Could not extract valid JSON from LLM response.")


# ---------------------------------------------------------------------------
# Azure OpenAI call
# ---------------------------------------------------------------------------

def call_azure_openai(prompt: str) -> dict:
    """
    Send the comparison prompt to Azure OpenAI and return parsed JSON.
    """
    load_dotenv()

    endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
    api_key = os.getenv("AZURE_OPENAI_API_KEY")
    api_version = os.getenv("AZURE_OPENAI_API_VERSION", "2024-12-01-preview")
    deployment = os.getenv("AZURE_OPENAI_DEPLOYMENT")

    if not endpoint or not api_key or not deployment:
        sys.exit(
            "Missing Azure OpenAI configuration.\n"
            "Set AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_API_KEY, and "
            "AZURE_OPENAI_DEPLOYMENT in your .env file or environment."
        )

    client = AzureOpenAI(
        azure_endpoint=endpoint,
        api_key=api_key,
        api_version=api_version,
    )

    print("📤 Sending documents to Azure OpenAI…")
    response = client.chat.completions.create(
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
        max_tokens=16_000,
        response_format={"type": "json_object"},  # enforces JSON output
    )

    raw = response.choices[0].message.content
    print(f"📥 Received {len(raw):,} characters from Azure OpenAI.")
    return extract_json(raw)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(
        description=(
            "End-to-end contract comparison: "
            "documents → Azure OpenAI → clause-aligned Excel workbook."
        )
    )
    ap.add_argument("original", help="Path to the original document (.txt or .docx)")
    ap.add_argument("amended", help="Path to the amended document (.txt or .docx)")
    ap.add_argument(
        "-o", "--output",
        default="comparison.xlsx",
        help="Output Excel file path (default: comparison.xlsx)",
    )
    ap.add_argument(
        "--save-json",
        metavar="PATH",
        help="Optionally save the raw LLM JSON to a file for inspection.",
    )
    args = ap.parse_args()

    # 1. Read documents
    print(f"📄 Reading original: {args.original}")
    original_text = read_document(args.original)
    print(f"📄 Reading amended:  {args.amended}")
    amended_text = read_document(args.amended)

    # 2. Build prompt & call Azure OpenAI
    prompt = build_prompt(original_text, amended_text)
    aligned_json = call_azure_openai(prompt)

    # 3. Optionally save the intermediate JSON
    if args.save_json:
        Path(args.save_json).write_text(
            json.dumps(aligned_json, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        print(f"💾 Intermediate JSON saved: {args.save_json}")

    # 4. Generate Excel
    out = generate_excel(aligned_json, args.output)
    print(f"✅ Excel workbook saved: {out}")


if __name__ == "__main__":
    main()

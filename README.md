# Contract Comparison Pipeline

**One-call LLM → deterministic post-processing → Excel workbook**

Produces a 4-column comparison spreadsheet:

| Column | Content |
|--------|---------|
| **Clause Name** | Clause label + heading |
| **Original Clause** | Full original text (changed tokens in **bold red**) |
| **Amended Clause** | Full amended text (changed tokens in **bold red**) |
| **Change in Clause** | Line-by-line recap: added / removed / changed to |

---

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Copy & configure Azure OpenAI credentials
cp .env.example .env
# edit .env with your Azure OpenAI endpoint, key, and deployment name
```

---

## Two Workflows

### A) End-to-end (documents → Azure OpenAI → Excel)

```bash
python run_comparison.py original.txt amended.txt -o comparison.xlsx

# With .docx files (install python-docx first):
pip install python-docx
python run_comparison.py original.docx amended.docx -o comparison.xlsx

# Save intermediate JSON for inspection:
python run_comparison.py original.txt amended.txt -o comparison.xlsx --save-json aligned.json
```

### B) Two-step (manual LLM call + Excel generation)

```bash
# 1. Paste both documents into the prompt in prompt_template.txt
#    Send to any LLM and save the JSON response as aligned_output.json

# 2. Generate Excel from JSON
python build_comparison_xlsx.py aligned_output.json comparison.xlsx
```

### Test with sample data (no Azure credentials needed)

```bash
python build_comparison_xlsx.py sample_input.json sample_output.xlsx
```

---

## Azure OpenAI Configuration

Set these in `.env` (or as environment variables):

| Variable | Example |
|----------|---------|
| `AZURE_OPENAI_ENDPOINT` | `https://my-resource.openai.azure.com/` |
| `AZURE_OPENAI_API_KEY` | `abc123...` |
| `AZURE_OPENAI_API_VERSION` | `2024-12-01-preview` |
| `AZURE_OPENAI_DEPLOYMENT` | `gpt-4o` |

---

## Files

| File | Purpose |
|------|---------|
| `run_comparison.py` | End-to-end pipeline: read docs → call Azure OpenAI → generate Excel |
| `build_comparison_xlsx.py` | Standalone JSON → Excel converter (no LLM needed) |
| `prompt_template.txt` | The one-shot LLM prompt with `{original_text}` / `{amended_text}` placeholders |
| `sample_input.json` | Example clause-aligned JSON for testing |
| `.env.example` | Template for Azure OpenAI credentials |
| `requirements.txt` | Python dependencies |

---

## JSON Schema (LLM Output)

```json
{
  "meta": {
    "original_title": "...",
    "amended_title": "...",
    "excluded_rule": "Exclude anything marked Commented"
  },
  "clauses": [
    {
      "clause_key": "1|General Terms|1.2(a)(i)",
      "clause_label_original": "1.2(a)(i)",
      "clause_label_amended": "1.2(a)(i)",
      "clause_name": "General Terms",
      "original_text": "Full clause text from original…",
      "amended_text": "Full clause text from amended…",
      "match_confidence": 0.95,
      "match_basis": "numbering"
    }
  ]
}
```

---

## Optional Add-ons

- **Match confidence column**: Each clause includes a `match_confidence` score from the LLM. Add a 5th column to surface suspicious alignments.
- **`.docx` support**: Install `python-docx` to read Word files directly.

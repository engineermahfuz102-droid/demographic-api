# Insighta Labs - Intelligence Query Engine (Stage 2)

An advanced demographic intelligence API built with FastAPI and SQLAlchemy. This system supports complex filtering, sorting, pagination, and a rule-based Natural Language Processing (NLP) engine.

## 🚀 Getting Started

1. **Install dependencies:** `pip install -r requirements.txt`
2. **Data Seeding:** Place `profiles.json` (containing 2026 records) in the root directory.
3. **Run the server:** `python -m uvicorn main:app --reload`

## 🧠 Natural Language Parser Approach

The search endpoint (`/api/profiles/search`) uses a **Rule-Based Keyword Parser** built with Regular Expressions (Regex). 

### How it Works:
1. **Tokenization:** The query string is normalized to lowercase and stripped of extra whitespace.
2. **Keyword Extraction:** The engine scans for specific "trigger words" to map to database filters:
   - **Gender:** Detects `male`, `males`, `female`, or `females`.
   - **Age Groups:** Matches stored categories like `child`, `teenager`, `adult`, or `senior`.
   - **"Young" Logic:** Per project requirements, the keyword `young` automatically maps to a `min_age` of 16 and a `max_age` of 24.
   - **Numeric Age Logic:** Identifies logic like `above`, `older than`, `under`, or `younger than` followed by numeric values.
   - **Location:** Detects country names and maps them to their respective ISO `country_id`.

### Limitations:
- **Complex Negation:** The parser does not currently support "exclude" logic (e.g., "not from Nigeria").
- **Ambiguous Context:** It treats multiple age triggers as additive, which may lead to empty results if the query is contradictory.
- **Strict Spelling:** It relies on correct spelling of country names and keywords to trigger filters.

## 📊 Database Schema
The system utilizes **UUID v7** for primary keys to ensure time-ordered sorting and performance. Key columns including `gender`, `age`, and `country_id` are indexed to prevent full-table scans during heavy querying.

## 🛠 Features
- **Automatic Seeding:** Injects 2026 profiles on startup if the database is empty.
- **Combined Filtering:** All filters are combinable and results must match every condition.
- **CORS Enabled:** Fully accessible for automated grading scripts.

# NYC Taxi ETL Pipeline

A group project implementing a simple data pipeline and ETL (Extract, Transform, Load) process using **Python**, **Pandas**, and **PostgreSQL**.

**Dataset:** NYC Yellow Taxi Trip Data — 2025 Sample

---

## Group Responsibilities

| Member | Role |
|--------|------|
| **Member 1** | Dataset Inspection, Data Quality Validation, Pandas Transformations |
| **Member 2** | PostgreSQL Database Schema Design |
| **Member 3** | Extraction, Loading, and Pipeline Integration |

---

## Project Structure

```
nyc-taxi-etl-pipeline/
├── data/
│   ├── raw/          # Raw dataset files (gitignored — do not commit)
│   └── processed/    # Cleaned/transformed outputs (gitignored)
├── scripts/
│   └── inspect_dataset.py   # Member 1: Dataset inspection script
├── src/
│   ├── __init__.py
│   └── transform.py         # Member 1: Validation & transformation functions
├── tests/
│   ├── __init__.py
│   └── test_transform.py    # Member 1: Unit tests
├── docs/
│   └── dataset.md           # Member 1: Dataset documentation
├── requirements.txt
├── .gitignore
└── README.md
```

---

## Setup

```bash
# 1. Clone the repository
git clone https://github.com/NardosShumete/nyc-taxi-etl-pipeline.git
cd nyc-taxi-etl-pipeline

# 2. Create a virtual environment
python -m venv venv
# Windows:
venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Place the dataset in data/raw/
#    (see docs/dataset.md for sourcing instructions)
```

---

## Running the Inspection Script

```bash
python scripts/inspect_dataset.py --input data/raw/<your_dataset_file>
```

The script auto-detects CSV or Parquet format and reports quality metrics without loading the full file into memory.

---

## Running Unit Tests

```bash
pytest tests/ -v
```

---

## Notes

- **Never commit** raw dataset files, `.env`, or database credentials.
- Each member works on a separate branch and opens a Pull Request into `main`.
- The `data/` directory is fully gitignored.

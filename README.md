# AIIMS Bhubaneswar — Hospital Patient Analytics

**Author:** Pracheeta Parida | **Roll No:** 2330173

## Project Overview
End-to-end data analytics pipeline for hospital patient data.
Data originates from SAP S/4HANA FI Module (Transaction FB60) and is
processed through an ETL pipeline before analysis and visualization.

##  Project Structure
```
HospitalAnalytics/
├── data/
│   └── patients.csv          ← Raw dataset (SAP FB60 export)
├── src/
│   ├── etl_pipeline.py       ← Extract → Transform → Load
│   ├── analysis.py           ← Pandas analysis + Charts
│   └── queries.sql           ← All SQL queries (27 queries)
├── output/
│   ├── hospital.db           ← SQLite database 
│   ├── charts/               ← All chart images 
│   └── etl_pipeline.log      ← ETL run log 
├── requirements.txt
```

## How to Run

### Step 1 — Install dependencies
```bash
pip install pandas matplotlib seaborn
```

### Step 2 — Run ETL Pipeline first
```bash
cd HospitalAnalytics
python src/etl_pipeline.py
```

### Step 3 — Run Analysis
```bash
python src/analysis.py
```

### Step 4 — View SQL Queries
Open `src/queries.sql` in any SQL editor or DB Browser for SQLite.

## 🔗 SAP Integration
Patient billing data is sourced from **SAP S/4HANA**:
| Data Field | SAP Source | Transaction |
|---|---|---|
| treatment_cost | FI — Billing Document | FB60 |
| insurance_covered | FI — Insurance Posting | F-02 |
| doctor | HR — Employee Master | PA20 |
| patient_id | SD — Customer Master | XD03 |

## Tech Stack
- **Python** — Pandas, Matplotlib, Seaborn
- **SQL** — SQLite (with advanced window functions, CTEs)
- **ETL** — Custom pipeline with data quality checks
- **SAP S/4HANA** — Data source (FI Module)

## 📈 Key Insights
- Heart Disease has the highest average treatment cost (₹50,000+)
- Dr. Mohanty handles the most patients and generates highest revenue
- Bhubaneswar has the highest patient footfall
- 30% of patients have less than 50% insurance coverage

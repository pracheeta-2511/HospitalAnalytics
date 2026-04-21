-- =============================================================
-- AIIMS Bhubaneswar Patient Analytics — SQL Queries
-- =============================================================
-- Project  : Hospital Patient Data Analysis
-- Author   : Pracheeta Parida | Roll No: 2330173
-- Batch    : SAP Data Analytics
-- Database : SQLite (hospital.db)
-- Table    : patients
-- =============================================================

-- ── BASIC QUERIES ────────────────────────────────────────────

-- Q1. Display all patient records
SELECT * FROM patients;

-- Q2. Display first 10 records
SELECT * FROM patients LIMIT 10;

-- Q3. Count total number of patients
SELECT COUNT(*) AS total_patients FROM patients;

-- ── DISEASE ANALYSIS ─────────────────────────────────────────

-- Q4. Count patients by disease
SELECT disease,
       COUNT(*) AS patient_count
FROM patients
GROUP BY disease
ORDER BY patient_count DESC;

-- Q5. Most common disease
SELECT disease,
       COUNT(*) AS frequency
FROM patients
GROUP BY disease
ORDER BY frequency DESC
LIMIT 1;

-- Q6. Average treatment cost per disease
SELECT disease,
       ROUND(AVG(treatment_cost), 2) AS avg_cost,
       ROUND(MIN(treatment_cost), 2) AS min_cost,
       ROUND(MAX(treatment_cost), 2) AS max_cost
FROM patients
GROUP BY disease
ORDER BY avg_cost DESC;

-- Q7. Disease count by category (Chronic vs Acute)
SELECT category,
       COUNT(*) AS patient_count,
       ROUND(AVG(treatment_cost), 2) AS avg_treatment_cost
FROM patients
GROUP BY category
ORDER BY patient_count DESC;

-- ── DOCTOR ANALYSIS ──────────────────────────────────────────

-- Q8. Doctor treating the most patients
SELECT doctor,
       COUNT(*) AS patients_treated
FROM patients
GROUP BY doctor
ORDER BY patients_treated DESC;

-- Q9. Total revenue generated per doctor
SELECT doctor,
       COUNT(*) AS patients,
       SUM(treatment_cost) AS total_revenue,
       ROUND(AVG(treatment_cost), 2) AS avg_revenue
FROM patients
GROUP BY doctor
ORDER BY total_revenue DESC;

-- Q10. Doctor and their department
SELECT DISTINCT doctor, department
FROM patients
ORDER BY department;

-- ── COST ANALYSIS ────────────────────────────────────────────

-- Q11. Total treatment revenue
SELECT SUM(treatment_cost)    AS total_revenue,
       SUM(insurance_covered) AS total_insured,
       SUM(out_of_pocket)     AS total_out_of_pocket
FROM patients;

-- Q12. Average treatment cost
SELECT ROUND(AVG(treatment_cost), 2) AS avg_treatment_cost
FROM patients;

-- Q13. Most expensive treatment (highest cost patient)
SELECT patient_name, disease, doctor, treatment_cost
FROM patients
ORDER BY treatment_cost DESC
LIMIT 1;

-- Q14. Least expensive treatment
SELECT patient_name, disease, doctor, treatment_cost
FROM patients
ORDER BY treatment_cost ASC
LIMIT 1;

-- Q15. Patients with treatment cost above average
SELECT patient_name, disease, doctor, treatment_cost
FROM patients
WHERE treatment_cost > (SELECT AVG(treatment_cost) FROM patients)
ORDER BY treatment_cost DESC;

-- ── CITY ANALYSIS ────────────────────────────────────────────

-- Q16. Total revenue by city
SELECT city,
       COUNT(*) AS patients,
       SUM(treatment_cost) AS total_revenue
FROM patients
GROUP BY city
ORDER BY total_revenue DESC;

-- Q17. Average treatment cost by city
SELECT city,
       ROUND(AVG(treatment_cost), 2) AS avg_cost
FROM patients
GROUP BY city
ORDER BY avg_cost DESC;

-- ── DEMOGRAPHIC ANALYSIS ─────────────────────────────────────

-- Q18. Patient count by gender
SELECT gender,
       COUNT(*) AS count
FROM patients
GROUP BY gender;

-- Q19. Average age of patients
SELECT ROUND(AVG(age), 1) AS average_age,
       MIN(age)            AS youngest,
       MAX(age)            AS oldest
FROM patients;

-- Q20. Patient count by age group
SELECT age_group,
       COUNT(*) AS patient_count,
       ROUND(AVG(treatment_cost), 2) AS avg_cost
FROM patients
GROUP BY age_group
ORDER BY avg_cost DESC;

-- ── ADVANCED SQL (Data Engineering Level) ────────────────────

-- Q21. Window Function — Rank doctors by total revenue
SELECT doctor,
       SUM(treatment_cost) AS total_revenue,
       RANK() OVER (ORDER BY SUM(treatment_cost) DESC) AS revenue_rank
FROM patients
GROUP BY doctor;

-- Q22. Window Function — Running total of treatment costs by date
SELECT admission_date,
       patient_name,
       treatment_cost,
       SUM(treatment_cost) OVER (ORDER BY admission_date) AS running_total
FROM patients
ORDER BY admission_date;

-- Q23. Subquery — Patients treated by the busiest doctor
SELECT patient_name, disease, treatment_cost
FROM patients
WHERE doctor = (
    SELECT doctor
    FROM patients
    GROUP BY doctor
    ORDER BY COUNT(*) DESC
    LIMIT 1
);

-- Q24. CTE — Monthly revenue summary
WITH monthly_revenue AS (
    SELECT admission_month,
           COUNT(*)                    AS patient_count,
           SUM(treatment_cost)         AS total_revenue,
           ROUND(AVG(treatment_cost), 2) AS avg_cost
    FROM patients
    GROUP BY admission_month
)
SELECT * FROM monthly_revenue
ORDER BY total_revenue DESC;

-- Q25. Insurance coverage analysis — patients with less than 50% coverage
SELECT patient_name,
       disease,
       treatment_cost,
       insurance_covered,
       ROUND((insurance_covered * 100.0 / treatment_cost), 1) AS coverage_pct
FROM patients
WHERE (insurance_covered * 1.0 / treatment_cost) < 0.5
ORDER BY coverage_pct ASC;

-- ── SAP INTEGRATION QUERIES ───────────────────────────────────

-- Q26. SAP Billing document reference lookup
SELECT sap_billing_doc,
       patient_name,
       treatment_cost,
       admission_date
FROM patients
ORDER BY sap_billing_doc;

-- Q27. Total billed amount per SAP document range
SELECT COUNT(sap_billing_doc) AS total_documents,
       SUM(treatment_cost)    AS total_billed_amount
FROM patients
WHERE sap_billing_doc LIKE 'SAP-FB60-%';

-- =============================================================
-- END OF SQL QUERIES
-- =============================================================

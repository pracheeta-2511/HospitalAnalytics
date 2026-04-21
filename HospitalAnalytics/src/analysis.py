"""
=============================================================
AIIMS Bhubaneswar Patient Analytics — Analysis & Visualization
=============================================================
Project     : Hospital Patient Data Analysis
Author      : [Your Name] | Roll No: [Your Roll No]
Batch       : [Your Batch]
Description : Pandas analysis + Matplotlib/Seaborn charts
              Data sourced from SAP S/4HANA FI Module (FB60)
=============================================================
"""

import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import seaborn as sns
import os
import warnings
warnings.filterwarnings("ignore")

# ── Style Setup ────────────────────────────────────────────
sns.set_theme(style="whitegrid", palette="Set2")
plt.rcParams.update({
    "font.family": "DejaVu Sans",
    "figure.facecolor": "white",
    "axes.facecolor": "#f8f9fa",
    "axes.titlesize": 14,
    "axes.titleweight": "bold",
})

DB_PATH      = "output/hospital.db"
CHARTS_DIR   = "output/charts"
os.makedirs(CHARTS_DIR, exist_ok=True)

# =============================================================
# LOAD DATA FROM DATABASE
# =============================================================
def load_data():
    print("\n" + "="*60)
    print("  AIIMS BHUBANESWAR — PATIENT ANALYTICS REPORT")
    print("="*60)
    conn = sqlite3.connect(DB_PATH)
    df   = pd.read_sql("SELECT * FROM patients", conn)
    conn.close()
    print(f"\n✅ Data loaded from SQLite DB: {len(df)} patient records")
    return df

# =============================================================
# SECTION 1: BASIC EXPLORATION
# =============================================================
def explore(df):
    print("\n── SECTION 1: DATA EXPLORATION ─────────────────────")
    print("\n📋 First 5 Records:")
    print(df.head().to_string(index=False))

    print("\n📊 Dataset Structure:")
    print(df.dtypes)

    print("\n📈 Summary Statistics:")
    print(df[["age", "treatment_cost", "insurance_covered",
              "out_of_pocket", "length_of_stay"]].describe().round(2))

    print(f"\n📌 Total Patients    : {len(df)}")
    print(f"📌 Unique Diseases   : {df['disease'].nunique()}")
    print(f"📌 Unique Doctors    : {df['doctor'].nunique()}")
    print(f"📌 Cities Covered    : {df['city'].nunique()}")
    print(f"📌 Total Revenue     : ₹{df['treatment_cost'].sum():,.0f}")

# =============================================================
# SECTION 2: DISEASE ANALYSIS
# =============================================================
def disease_analysis(df):
    print("\n── SECTION 2: DISEASE ANALYSIS ─────────────────────")

    disease_count = df["disease"].value_counts()
    disease_cost  = df.groupby("disease")["treatment_cost"].mean().round(2).sort_values(ascending=False)

    print("\n🦠 Disease Frequency:")
    print(disease_count.to_string())

    print("\n💰 Average Treatment Cost by Disease:")
    print(disease_cost.to_string())

    # Chart 1 — Pie Chart: Disease Distribution
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle("AIIMS Bhubaneswar — Disease Analysis", fontsize=16, fontweight="bold", color="#2c3e50")

    colors = ["#e74c3c", "#3498db", "#2ecc71", "#f39c12", "#9b59b6"]
    axes[0].pie(disease_count.values, labels=disease_count.index,
                autopct="%1.1f%%", colors=colors, startangle=140,
                wedgeprops={"edgecolor": "white", "linewidth": 2})
    axes[0].set_title("Disease Distribution Among Patients")

    bars = axes[1].bar(disease_cost.index, disease_cost.values,
                       color=colors, edgecolor="white", linewidth=1.5)
    axes[1].set_title("Average Treatment Cost by Disease (₹)")
    axes[1].set_xlabel("Disease")
    axes[1].set_ylabel("Avg Treatment Cost (₹)")
    axes[1].tick_params(axis="x", rotation=15)
    for bar in bars:
        axes[1].text(bar.get_x() + bar.get_width()/2,
                     bar.get_height() + 500,
                     f"₹{bar.get_height():,.0f}",
                     ha="center", fontsize=9, fontweight="bold")
    plt.tight_layout()
    plt.savefig(f"{CHARTS_DIR}/1_disease_analysis.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("\n✅ Chart saved: 1_disease_analysis.png")

# =============================================================
# SECTION 3: DOCTOR WORKLOAD ANALYSIS
# =============================================================
def doctor_analysis(df):
    print("\n── SECTION 3: DOCTOR WORKLOAD ANALYSIS ─────────────")

    doctor_patients = df["doctor"].value_counts()
    doctor_revenue  = df.groupby("doctor")["treatment_cost"].sum().sort_values(ascending=False)

    print("\n👨‍⚕️ Patients per Doctor:")
    print(doctor_patients.to_string())

    print(f"\n🏆 Busiest Doctor: {doctor_patients.idxmax()} ({doctor_patients.max()} patients)")
    print(f"💰 Highest Revenue Doctor: {doctor_revenue.idxmax()} (₹{doctor_revenue.max():,.0f})")

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle("AIIMS Bhubaneswar — Doctor Workload Analysis", fontsize=16, fontweight="bold", color="#2c3e50")

    colors = sns.color_palette("Set2", len(doctor_patients))
    axes[0].barh(doctor_patients.index, doctor_patients.values, color=colors, edgecolor="white")
    axes[0].set_title("Patient Count per Doctor")
    axes[0].set_xlabel("Number of Patients")
    for i, v in enumerate(doctor_patients.values):
        axes[0].text(v + 0.1, i, str(v), va="center", fontweight="bold")

    axes[1].bar(doctor_revenue.index, doctor_revenue.values,
                color=sns.color_palette("Set1", len(doctor_revenue)), edgecolor="white")
    axes[1].set_title("Total Revenue Generated per Doctor (₹)")
    axes[1].set_ylabel("Total Revenue (₹)")
    axes[1].tick_params(axis="x", rotation=15)
    for bar in axes[1].patches:
        axes[1].text(bar.get_x() + bar.get_width()/2,
                     bar.get_height() + 1000,
                     f"₹{bar.get_height():,.0f}",
                     ha="center", fontsize=8, fontweight="bold")
    plt.tight_layout()
    plt.savefig(f"{CHARTS_DIR}/2_doctor_workload.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("✅ Chart saved: 2_doctor_workload.png")

# =============================================================
# SECTION 4: CITY-WISE ANALYSIS
# =============================================================
def city_analysis(df):
    print("\n── SECTION 4: CITY-WISE ANALYSIS ───────────────────")

    city_patients = df["city"].value_counts()
    city_revenue  = df.groupby("city")["treatment_cost"].sum().sort_values(ascending=False)
    city_avg_cost = df.groupby("city")["treatment_cost"].mean().round(2)

    print("\n🏙️ Patients by City:")
    print(city_patients.to_string())
    print("\n💰 Total Revenue by City:")
    print(city_revenue.to_string())

    fig, axes = plt.subplots(1, 3, figsize=(16, 5))
    fig.suptitle("AIIMS Bhubaneswar — City-wise Patient Analysis", fontsize=16, fontweight="bold", color="#2c3e50")

    colors = ["#3498db", "#e74c3c", "#2ecc71"]
    axes[0].bar(city_patients.index, city_patients.values, color=colors, edgecolor="white", linewidth=1.5)
    axes[0].set_title("Patient Count by City")
    axes[0].set_ylabel("Number of Patients")
    for bar in axes[0].patches:
        axes[0].text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1,
                     str(int(bar.get_height())), ha="center", fontweight="bold")

    axes[1].pie(city_revenue.values, labels=city_revenue.index,
                autopct="%1.1f%%", colors=colors,
                wedgeprops={"edgecolor": "white", "linewidth": 2})
    axes[1].set_title("Revenue Share by City")

    axes[2].bar(city_avg_cost.index, city_avg_cost.values, color=colors, edgecolor="white")
    axes[2].set_title("Average Treatment Cost by City (₹)")
    axes[2].set_ylabel("Avg Cost (₹)")
    for bar in axes[2].patches:
        axes[2].text(bar.get_x() + bar.get_width()/2, bar.get_height() + 200,
                     f"₹{bar.get_height():,.0f}", ha="center", fontsize=9, fontweight="bold")

    plt.tight_layout()
    plt.savefig(f"{CHARTS_DIR}/3_city_analysis.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("✅ Chart saved: 3_city_analysis.png")

# =============================================================
# SECTION 5: TREATMENT COST ANALYSIS
# =============================================================
def cost_analysis(df):
    print("\n── SECTION 5: TREATMENT COST ANALYSIS ──────────────")

    print(f"\n💰 Total Treatment Revenue : ₹{df['treatment_cost'].sum():,.0f}")
    print(f"💰 Average Treatment Cost  : ₹{df['treatment_cost'].mean():,.2f}")
    print(f"💰 Highest Treatment Cost  : ₹{df['treatment_cost'].max():,.0f}")
    print(f"💰 Lowest Treatment Cost   : ₹{df['treatment_cost'].min():,.0f}")
    print(f"🛡️  Total Insurance Covered : ₹{df['insurance_covered'].sum():,.0f}")
    print(f"💸 Total Out of Pocket     : ₹{df['out_of_pocket'].sum():,.0f}")

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle("AIIMS Bhubaneswar — Treatment Cost Analysis", fontsize=16, fontweight="bold", color="#2c3e50")

    axes[0].hist(df["treatment_cost"], bins=10, color="#3498db",
                 edgecolor="white", linewidth=1.5, alpha=0.85)
    axes[0].set_title("Treatment Cost Distribution")
    axes[0].set_xlabel("Treatment Cost (₹)")
    axes[0].set_ylabel("Number of Patients")
    axes[0].axvline(df["treatment_cost"].mean(), color="red",
                    linestyle="--", linewidth=2, label=f"Mean: ₹{df['treatment_cost'].mean():,.0f}")
    axes[0].legend()

    categories = ["Total Cost", "Insurance Covered", "Out of Pocket"]
    values     = [df["treatment_cost"].sum(), df["insurance_covered"].sum(), df["out_of_pocket"].sum()]
    bar_colors = ["#3498db", "#2ecc71", "#e74c3c"]
    bars = axes[1].bar(categories, values, color=bar_colors, edgecolor="white", linewidth=1.5)
    axes[1].set_title("Total Cost vs Insurance vs Out-of-Pocket (₹)")
    axes[1].set_ylabel("Amount (₹)")
    for bar in bars:
        axes[1].text(bar.get_x() + bar.get_width()/2, bar.get_height() + 2000,
                     f"₹{bar.get_height():,.0f}", ha="center", fontsize=9, fontweight="bold")

    plt.tight_layout()
    plt.savefig(f"{CHARTS_DIR}/4_cost_analysis.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("✅ Chart saved: 4_cost_analysis.png")

# =============================================================
# SECTION 6: AGE & GENDER ANALYSIS
# =============================================================
def demographic_analysis(df):
    print("\n── SECTION 6: DEMOGRAPHIC ANALYSIS ─────────────────")

    gender_count = df["gender"].value_counts()
    age_group    = df["age_group"].value_counts()

    print(f"\n👥 Gender Distribution:\n{gender_count.to_string()}")
    print(f"\n📊 Age Group Distribution:\n{age_group.to_string()}")

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle("AIIMS Bhubaneswar — Patient Demographics", fontsize=16, fontweight="bold", color="#2c3e50")

    axes[0].pie(gender_count.values, labels=gender_count.index,
                autopct="%1.1f%%", colors=["#3498db", "#e91e8c"],
                wedgeprops={"edgecolor": "white", "linewidth": 2}, startangle=90)
    axes[0].set_title("Gender Distribution")

    age_order = ["Child", "Young Adult", "Middle Aged", "Senior", "Elderly"]
    age_data  = [age_group.get(a, 0) for a in age_order]
    colors    = ["#1abc9c", "#3498db", "#f39c12", "#e74c3c", "#9b59b6"]
    axes[1].bar(age_order, age_data, color=colors, edgecolor="white", linewidth=1.5)
    axes[1].set_title("Patient Count by Age Group")
    axes[1].set_xlabel("Age Group")
    axes[1].set_ylabel("Number of Patients")
    axes[1].tick_params(axis="x", rotation=15)
    for i, v in enumerate(age_data):
        axes[1].text(i, v + 0.1, str(v), ha="center", fontweight="bold")

    plt.tight_layout()
    plt.savefig(f"{CHARTS_DIR}/5_demographics.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("✅ Chart saved: 5_demographics.png")

# =============================================================
# SECTION 7: SAP INTEGRATION SUMMARY
# =============================================================
def sap_summary(df):
    print("\n── SECTION 7: SAP DATA SOURCE SUMMARY ──────────────")
    print("\n📦 SAP Billing Documents in Dataset:")
    print(f"   Total SAP FB60 Documents : {df['sap_billing_doc'].nunique()}")
    print(f"   SAP Document Range       : {df['sap_billing_doc'].min()} → {df['sap_billing_doc'].max()}")
    print("\n🔗 SAP → Analytics Data Mapping:")
    mapping = {
        "treatment_cost"    : "SAP FI — Billing Document (FB60)",
        "insurance_covered" : "SAP FI — Insurance Posting (F-02)",
        "doctor"            : "SAP HR — Employee Master (PA20)",
        "patient_id"        : "SAP SD — Customer Master (XD03)",
        "disease"           : "SAP MM — Service Material (MM03)",
        "admission_date"    : "SAP SD — Sales Order Date (VA01)",
    }
    for col, source in mapping.items():
        print(f"   {col:<22} ← {source}")

# =============================================================
# MAIN
# =============================================================
def run_analysis():
    df = load_data()
    explore(df)
    disease_analysis(df)
    doctor_analysis(df)
    city_analysis(df)
    cost_analysis(df)
    demographic_analysis(df)
    sap_summary(df)

    print("\n" + "="*60)
    print("✅ ALL ANALYSIS COMPLETE!")
    print(f"📊 Charts saved to: {CHARTS_DIR}/")
    print("="*60 + "\n")

if __name__ == "__main__":
    run_analysis()

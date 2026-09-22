import json
import urllib.request
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    count,
    sum,
    desc,
    col
)


# --------------------------------------------------
# PROJECT PATHS
# --------------------------------------------------

BASE_DIR = Path(r"D:\Modern-DE-Project")

LATEST_TARGET_FILE = (
    BASE_DIR / "data" / "output" / "latest_target.txt"
)

AI_DIR = BASE_DIR / "data" / "ai"

METRICS_FILE = AI_DIR / "shipment_metrics.json"
REPORT_FILE = AI_DIR / "shipment_intelligence.md"

OLLAMA_URL = "http://localhost:11434/api/generate"
OLLAMA_MODEL = "qwen3:4b"


# --------------------------------------------------
# READ ACTIVE PARQUET TARGET
# --------------------------------------------------

def get_latest_target():
    if not LATEST_TARGET_FILE.exists():
        raise FileNotFoundError(
            f"Latest target pointer not found: {LATEST_TARGET_FILE}"
        )

    target_path = Path(
        LATEST_TARGET_FILE.read_text(encoding="utf-8").strip()
    )

    if not target_path.exists():
        raise FileNotFoundError(
            f"Target Parquet directory not found: {target_path}"
        )

    return target_path


# --------------------------------------------------
# BUILD BUSINESS METRICS
# --------------------------------------------------

def build_metrics(df):

    total_shipments = df.count()

    average_delivery_days = (
        df.select(avg("DeliveryDays")).first()[0]
    )

    total_shipping_cost = (
        df.select(sum("ShippingCost")).first()[0]
    )

    shipments_by_city = (
        df.groupBy("City")
        .count()
        .orderBy(desc("count"))
        .collect()
    )

    average_delivery_by_city = (
        df.groupBy("City")
        .agg(avg("DeliveryDays").alias("AverageDeliveryDays"))
        .orderBy(desc("AverageDeliveryDays"))
        .collect()
    )

    shipments_by_status = (
        df.groupBy("Status")
        .count()
        .orderBy(desc("count"))
        .collect()
    )

    average_cost_by_city = (
        df.groupBy("City")
        .agg(avg("ShippingCost").alias("AverageShippingCost"))
        .orderBy(desc("AverageShippingCost"))
        .collect()
    )

    weight_category_distribution = (
        df.groupBy("Weight_Category")
        .count()
        .orderBy(desc("count"))
        .collect()
    )
        # --------------------------------------------------
    # ANOMALY DETECTION
    # --------------------------------------------------

    average_shipping_cost = (
        df.select(avg("ShippingCost")).first()[0]
    )

    anomaly_rows = (
        df.filter(
            (col("DeliveryDays") >= 5) |
            (col("ShippingCost") > average_shipping_cost)
        )
        .select(
            "ShipmentID",
            "City",
            "Status",
            "DeliveryDays",
            "ShippingCost"
        )
        .collect()
    )

    anomalies = []

    for row in anomaly_rows:

        reasons = []

        if row["DeliveryDays"] >= 5:
            reasons.append("Long delivery time")

        if row["ShippingCost"] > average_shipping_cost:
            reasons.append("Above-average shipping cost")

        anomalies.append({
            "ShipmentID": row["ShipmentID"],
            "City": row["City"],
            "Status": row["Status"],
            "DeliveryDays": row["DeliveryDays"],
            "ShippingCost": row["ShippingCost"],
            "Reasons": reasons
        })
    metrics = {
        "total_shipments": total_shipments,

        "average_delivery_days": round(
            average_delivery_days, 2
        ) if average_delivery_days is not None else 0,

        "total_shipping_cost": total_shipping_cost or 0,

        "shipments_by_city": {
            row["City"]: row["count"]
            for row in shipments_by_city
        },

        "average_delivery_by_city": {
            row["City"]: round(
                row["AverageDeliveryDays"], 2
            )
            for row in average_delivery_by_city
        },

        "shipments_by_status": {
            row["Status"]: row["count"]
            for row in shipments_by_status
        },

        "average_cost_by_city": {
            row["City"]: round(
                row["AverageShippingCost"], 2
            )
            for row in average_cost_by_city
        },

        "weight_category_distribution": {
            row["Weight_Category"]: row["count"]
            for row in weight_category_distribution
        },
        "anomalies": anomalies
    }

    return metrics


# --------------------------------------------------
# SAVE METRICS
# --------------------------------------------------

def save_metrics(metrics):

    AI_DIR.mkdir(parents=True, exist_ok=True)

    with open(
        METRICS_FILE,
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            metrics,
            file,
            indent=4
        )


# --------------------------------------------------
# SEND METRICS TO OLLAMA
# --------------------------------------------------

def ask_ollama(metrics):

    prompt = f"""
You are a shipment operations analyst.

Analyze the following shipment metrics generated from
a real ETL pipeline.

IMPORTANT RULES:

1. Use only the supplied data.
2. Do not invent shipment problems or causes.
3. Clearly distinguish observations from assumptions.
4. Do not claim real-time information because this is
   historical/processed shipment data.
5. Identify unusual patterns when supported by the data.
6. Give practical operational recommendations.
7. Keep the report concise and business-focused.

Return the report using exactly these sections:

# Executive Summary

# Key Findings

# Potential Anomalies

# Operational Attention Areas

# Recommended Actions

Shipment Metrics:

{json.dumps(metrics, indent=4)}
"""

    payload = {
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False
    }

    data = json.dumps(payload).encode("utf-8")

    request = urllib.request.Request(
        OLLAMA_URL,
        data=data,
        headers={
            "Content-Type": "application/json"
        }
    )

    with urllib.request.urlopen(request) as response:

        result = json.loads(
            response.read().decode("utf-8")
        )

    return result["response"]


# --------------------------------------------------
# SAVE AI REPORT
# --------------------------------------------------

def save_report(report):

    with open(
        REPORT_FILE,
        "w",
        encoding="utf-8"
    ) as file:

        file.write(report)


# --------------------------------------------------
# MAIN
# --------------------------------------------------

def main():

    print("Starting AI shipment intelligence...")

    target_path = get_latest_target()

    print(f"Reading target: {target_path}")

    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("ShipmentAI")
        .getOrCreate()
    )

    try:

        df = spark.read.parquet(str(target_path))

        print(f"Loaded {df.count()} shipments.")

        metrics = build_metrics(df)

        save_metrics(metrics)

        print(f"Metrics saved to: {METRICS_FILE}")

    finally:

        spark.stop()

    print("Sending metrics to Ollama...")

    report = ask_ollama(metrics)

    save_report(report)

    print(f"AI report saved to: {REPORT_FILE}")

    print("\nAI shipment intelligence completed.")


if __name__ == "__main__":
    main()
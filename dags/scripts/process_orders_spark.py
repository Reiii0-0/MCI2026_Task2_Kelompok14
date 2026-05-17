"""
Task 2: Process Orders dengan Apache Spark → Load ke ClickHouse
================================================================
Membaca semua file .parquet dari Data Lake,
melakukan agregasi analitik dengan PySpark,
dan meload hasilnya ke ClickHouse.

Tabel yang dibuat di ClickHouse (database: orders_db & analytics):
  1. order_items                 → Fact table (APPEND harian, ada ingested_date)
  2. top_products                → Seluruh rangking produk (truncate-insert)
  3. department_summary          → Ringkasan per departemen (truncate-insert)
  4. hourly_activity             → Aktivitas per jam (truncate-insert)
  5. daily_summary               → Snapshot harian untuk analisis tren (APPEND)
  6. data_quality_report         → Laporan missing data per kolom (truncate-insert)
  7. user_loyalty_segmentation   → Analisis RFM (Recency & Frequency) untuk deteksi Churn (truncate-insert)
  8. product_cart_priority       → Analisis prioritas keranjang / Market Basket (truncate-insert)

Mode data harian:
  - order_items & daily_summary: APPEND dengan kolom ingested_date
    → mendukung analisis time-series & perubahan dari hari ke hari
  - Tabel agregasi lainnya: TRUNCATE-INSERT
    → selalu menampilkan kondisi terkini

Data Cleaning Pipeline:
  1. Deteksi nilai NULL dan string literal 'missing' per kolom
  2. Ganti string 'missing' → NULL
  3. Hapus baris yang kolom kunci-nya NULL (order_id, product_id, user_id)
  4. Imputasi NULL yang tersisa dengan nilai default
  5. Simpan laporan kualitas ke orders_db.data_quality_report
  6. Caching DataFrame ke memori untuk efisiensi eksekusi DAG Spark
"""

from datetime import date, datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel
from clickhouse_driver import Client
import pandas as pd
import os
import glob


def rename_column_if_needed(client, database, table, old_column, new_column):
    existing_columns = client.execute(
        """
        SELECT name
        FROM system.columns
        WHERE database = %(database)s AND table = %(table)s
        """,
        {"database": database, "table": table},
    )
    existing_columns = {row[0] for row in existing_columns}

    if old_column in existing_columns and new_column not in existing_columns:
        client.execute(
            f"ALTER TABLE {database}.{table} RENAME COLUMN {old_column} TO {new_column}"
        )
        print(f"   Migrasi kolom: {database}.{table}.{old_column} -> {new_column}")


def rebuild_snapshot_table_if_legacy(client, database, table, legacy_columns):
    existing_columns = client.execute(
        """
        SELECT name
        FROM system.columns
        WHERE database = %(database)s AND table = %(table)s
        """,
        {"database": database, "table": table},
    )
    existing_columns = {row[0] for row in existing_columns}

    if existing_columns.intersection(legacy_columns):
        client.execute(f"DROP TABLE IF EXISTS {database}.{table}")
        print(f"   Rebuild snapshot table: {database}.{table}")


def run_spark_analytics():
    # Tanggal ingest hari ini — digunakan sebagai partisi waktu
    today = str(date.today())  # format: YYYY-MM-DD

    spark = SparkSession.builder \
        .appName("Orders_Pipeline_Enterprise_Analytics") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

    print("📖 Membaca seluruh data dari Data Lake...")
    
    # Cek apakah ada data baru untuk diproses
    parquet_files = glob.glob("/opt/airflow/data_lake/orders/*.parquet")
    if not parquet_files:
        print("✅ Tidak ada data pesanan baru (.parquet) di Data Lake. Proses selesai tanpa error.")
        return

    df_raw = spark.read.parquet("file:///opt/airflow/data_lake/orders/")
    
    # Menghitung baris awal sebelum diproses
    total_rows = df_raw.count()
    print(f"📊 Total baris dari Data Lake: {total_rows} | Tanggal ingest: {today}")

    # ══════════════════════════════════════════════════════════════════════
    # 🧹 DATA CLEANING — Missing Value Detection & Imputation
    # ══════════════════════════════════════════════════════════════════════
    print("🔍 Tahap 1: Mendeteksi missing values sebelum cleaning...")

    # Kolom-kolom yang akan diperiksa kualitas datanya
    # (days_since_prior_order dikecualikan karena NULL valid sebagai pesanan pertama)
    COLS_TO_CHECK = [
        "order_id", "user_id", "product_id", "product_name",
        "department", "aisle", "order_hour_of_day",
        "add_to_cart_order", "reordered",
    ]

    # Hitung jumlah NULL + string literal 'missing' per kolom SEBELUM cleaning
    quality_exprs = []
    for col in COLS_TO_CHECK:
        null_count = F.sum(
            F.when(
                F.col(col).isNull() | (F.col(col).cast("string") == "missing"),
                1
            ).otherwise(0)
        ).alias(col)
        quality_exprs.append(null_count)

    quality_before = df_raw.agg(*quality_exprs).toPandas()

    # Susun menjadi format baris: (kolom, jumlah_missing, pct_missing)
    quality_rows = []
    for col in COLS_TO_CHECK:
        missing_count = int(quality_before[col].iloc[0])
        pct = round(missing_count * 100.0 / total_rows, 2) if total_rows > 0 else 0.0
        quality_rows.append({
            "ingested_date": date.today(),
            "column_name":   col,
            "total_rows":    total_rows,
            "missing_count": missing_count,
            "missing_pct":   pct,
        })
        if missing_count > 0:
            print(f"   ⚠️  [{col}] missing: {missing_count} baris ({pct}%)")

    df_quality = pd.DataFrame(quality_rows)
    print(f"   📋 Audit selesai. Kolom dengan missing: "
          f"{sum(1 for r in quality_rows if r['missing_count'] > 0)} dari {len(COLS_TO_CHECK)}")

    # ── Tahap 2 & 3: Imputasi NULL ─────────────────────────────────────────
    print("🔄 Tahap 2 & 3: Melabelkan missing values (tidak ada drop data)...")
    
    # Semua missing values dilabelkan secara eksplisit agar bisa diaudit di Metabase
    impute_map = {
        "product_name":           "missing",
        "department":             "missing",
        "aisle":                  "missing",
        "eval_set":               "missing",
        "order_id":               0,
        "product_id":             0,
        "user_id":                0,
        "reordered":              0,
        "add_to_cart_order":      0,
        "order_hour_of_day":      0,
        "order_dow":              0,
        "order_number":           0,
        "department_id":          0,
        "aisle_id":               0,
    }
    
    df_clean = df_raw.fillna(impute_map)

    # 🚀 OPTIMISASI PERFORMA: Menyimpan hasil cleaning ke memori (Cache)
    # Ini mencegah Spark membaca ulang disk dan melakukan cleaning berulang kali
    # untuk setiap tahapan agregasi di bawah.
    df_clean.persist(StorageLevel.MEMORY_AND_DISK)
    
    # Memicu aksi agar cache dieksekusi (Lazy Evaluation)
    total_clean = df_clean.count()
    print(f"✅ Labeling & Caching selesai: {total_clean} baris dipertahankan utuh.")
    print()

    # ══════════════════════════════════════════════════════════════════════
    # 🧠 PROSES AGREGASI (OLAP PREPARATION)
    # ══════════════════════════════════════════════════════════════════════

    # ── Agregasi 1: Produk (Heavy Hitters) ────────────────────────────────
    print("🔍 Kalkulasi Performa Produk (Tanpa Limitasi)...")
    top_products_df = df_clean.groupBy("product_id", "product_name", "department", "aisle") \
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum(F.when(F.col("reordered") == 1, 1).otherwise(0)).alias("reorder_count"),
        ) \
        .orderBy(F.desc("total_orders"))
    
    top_products = top_products_df.toPandas()

    # ── Agregasi 2: Department Summary ────────────────────────────────────
    print("📦 Kalkulasi ringkasan per departemen...")
    dept_summary_df = df_clean.groupBy("department_id", "department") \
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.count("*").alias("total_items"),
            F.sum(F.when(F.col("reordered") == 1, 1).otherwise(0)).alias("reordered_items"),
        ) \
        .orderBy(F.desc("total_items"))

    dept_summary = dept_summary_df.toPandas()

    # ── Agregasi 3: Hourly Activity & Capacity ────────────────────────────
    print("⏰ Kalkulasi aktivitas & prediksi kapasitas per jam...")
    hourly_df = df_clean.groupBy("order_hour_of_day") \
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.count("*").alias("total_items"),
        ) \
        .orderBy("order_hour_of_day")

    hourly_activity = hourly_df.toPandas()
    
    # Mengadopsi struktur tabel hourly_capacity untuk prediktif logistik
    hourly_capacity = hourly_df.select(
        F.col("order_hour_of_day"),
        F.col("total_orders").alias("predicted_orders"),
        F.col("total_items").alias("predicted_items")
    ).toPandas()

    # ── Agregasi 4: Advanced Products Performance ─────────────────────────
    print("🧠 Kalkulasi metrik lanjutan produk (Avg Cart Position)...")
    products_performance_df = df_clean.groupBy(
        "product_id", "product_name", "department", "aisle"
    ).agg(
        F.count("order_id").alias("total_sold"),
        F.sum(F.when(F.col("reordered") == 1, 1).otherwise(0)).alias("total_reordered"),
        F.round(F.avg("add_to_cart_order"), 2).alias("avg_cart_position"),
    ).orderBy(F.desc("total_sold"))
    
    products_performance = products_performance_df.toPandas()

    # ── Agregasi 5: Sales Forecasting ─────────────────────────────────────
    print("🔮 Kalkulasi estimasi penjualan departemen...")
    sales_forecasting_df = df_clean.groupBy("department").agg(
        F.countDistinct("order_id").alias("current_total_orders"),
        F.round(F.avg(F.col("reordered").cast("double")), 4).alias("reorder_probability"),
    ).withColumn(
        "forecasted_total_orders",
        F.round(
            F.col("current_total_orders") * (F.lit(1.0) + F.col("reorder_probability")),
            0,
        ).cast("int"),
    ).orderBy(F.desc("forecasted_total_orders"))
    
    sales_forecasting = sales_forecasting_df.toPandas()

    # ── Agregasi 6: User Loyalty Segmentation (Deteksi Risiko Churn) ──────
    print("👥 Kalkulasi Segmentasi Loyalitas Pelanggan (RFM Analysis)...")
    user_loyalty_df = df_clean.groupBy("user_id").agg(
        F.max("order_number").alias("total_lifetime_orders"),
        F.coalesce(F.round(F.avg("days_since_prior_order"), 1), F.lit(0.0)).alias("avg_days_between_orders"),
        F.countDistinct("product_id").alias("unique_items_bought")
    ).withColumn(
        "loyalty_status",
        F.when(F.col("total_lifetime_orders") >= 10, "Gold / High Loyalty")
         .when(F.col("total_lifetime_orders") >= 4, "Silver / Regular")
         .otherwise("Bronze / New User")
    ).withColumn(
        "churn_risk",
        F.when(F.col("avg_days_between_orders") > 21, "High Risk (Lapsing)")
         .when(F.col("avg_days_between_orders") >= 7, "Medium Risk")
         .otherwise("Low Risk (Active)")
    )
    
    user_loyalty = user_loyalty_df.toPandas()

    # ── Agregasi 7: Product Cart Priority (Market Basket Analysis) ────────
    print("🛒 Kalkulasi Produk Pemicu Pembelian (First-in-Cart)...")
    cart_priority_df = df_clean.filter(F.col("add_to_cart_order") == 1).groupBy(
        "product_id", "product_name", "department"
    ).agg(
        F.count("order_id").alias("times_added_first")
    ).orderBy(F.desc("times_added_first"))
    
    cart_priority = cart_priority_df.toPandas()

    # ── Agregasi 8: History Department Trend (Live Batch Stream) ──────────
    print("📈 Mencatat jejak tren per departemen untuk batch saat ini...")
    dept_trend_df = df_clean.groupBy("department").agg(
        F.countDistinct("order_id").alias("total_orders")
    )
    dept_trend = dept_trend_df.toPandas()
    dept_trend["batch_time"] = datetime.now()
    dept_trend = dept_trend[["batch_time", "department", "total_orders"]]

    # ── Agregasi 9: Daily Summary (Snapshot Tingkat Eksekutif) ────────────
    print("📅 Membuat ringkasan harian performa bisnis...")
    daily_summary_df = df_clean.groupBy() \
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.countDistinct("user_id").alias("unique_users"),
            F.count("*").alias("total_items"),
            F.countDistinct("product_id").alias("unique_products"),
            F.round(F.count("*") / F.countDistinct("order_id"), 2).alias("avg_basket_size"),
            F.round(
                F.sum(F.when(F.col("reordered") == 1, 1).otherwise(0)) * 100.0 / F.count("*"), 1
            ).alias("reorder_rate_pct"),
        )
    daily_summary = daily_summary_df.toPandas()
    daily_summary["ingested_date"] = date.today()
    
    daily_summary = daily_summary[[
        "ingested_date", "total_orders", "unique_users", "total_items", 
        "unique_products", "avg_basket_size", "reorder_rate_pct"
    ]]

    # ── Pengambilan Data Mentah (Fact Table) ──────────────────────────────
    print("📋 Menyiapkan Fact Table order_items...")
    # Memilih kolom secara spesifik untuk meminimalisir memory footprint
    all_items_df = df_clean.select(
        "order_id", "user_id", "order_number", "order_dow", "order_hour_of_day",
        "days_since_prior_order", "eval_set", "product_id", "product_name",
        "aisle_id", "aisle", "department_id", "department", "add_to_cart_order",
        "reordered"
    )
    
    all_items = all_items_df.toPandas()
    all_items["ingested_date"] = date.today()
    
    # 🧹 Bersihkan Cache dari memori karena perhitungan Spark sudah selesai
    df_clean.unpersist()
    spark.stop()


    # ══════════════════════════════════════════════════════════════════════
    # LOAD KE CLICKHOUSE
    # ══════════════════════════════════════════════════════════════════════
    print("🚀 Memuat seluruh DataFrames ke ClickHouse Warehouse...")

    client = Client(
        host="clickhouse-server",
        user="admin",
        password="rahasia",
    )

    client.execute("CREATE DATABASE IF NOT EXISTS orders_db")
    client.execute("CREATE DATABASE IF NOT EXISTS analytics")

    # Migrasi kompatibilitas nama kolom
    rename_column_if_needed(client, "analytics", "sales_forecasting", "current_sales_volume", "current_total_orders")
    rename_column_if_needed(client, "analytics", "sales_forecasting", "forecasted_sales_demand", "forecasted_total_orders")
    rename_column_if_needed(client, "orders_db", "history_department_trend", "total_sold", "total_orders")
    rename_column_if_needed(client, "analytics", "history_department_trend", "total_sold", "total_orders")
    rebuild_snapshot_table_if_legacy(client, "analytics", "sales_forecasting", {"current_sales_volume", "forecasted_sales_demand"})

    # ── Tabel 1: order_items (APPEND harian) ──────────────────────────────
    client.execute("""
        CREATE TABLE IF NOT EXISTS orders_db.order_items (
            order_id               UInt32,
            user_id                UInt32,
            order_number           UInt16,
            order_dow              UInt8,
            order_hour_of_day      UInt8,
            days_since_prior_order Nullable(Float32),
            eval_set               String,
            product_id             UInt32,
            product_name           String,
            aisle_id               UInt16,
            aisle                  String,
            department_id          UInt8,
            department             String,
            add_to_cart_order      UInt8,
            reordered              UInt8,
            ingested_date          Date
        ) ENGINE = ReplacingMergeTree(ingested_date)
        PARTITION BY toYYYYMM(ingested_date)
        ORDER BY (ingested_date, order_id, product_id)
    """)

    if not all_items.empty:
        data_tuples = [tuple(x) for x in all_items.to_numpy()]
        client.execute("INSERT INTO orders_db.order_items VALUES", data_tuples)
    print(f"   ✅ order_items: {len(all_items)} baris di-ingest")

    # ── Tabel 2: top_products ─────────────────────────────────────────────
    client.execute("""
        CREATE TABLE IF NOT EXISTS orders_db.top_products (
            product_id   UInt32,
            product_name String,
            department   String,
            aisle        String,
            total_orders Int32,
            reorder_count Int32
        ) ENGINE = MergeTree()
        ORDER BY total_orders
    """)
    client.execute("TRUNCATE TABLE orders_db.top_products")
    if not top_products.empty:
        client.execute("INSERT INTO orders_db.top_products VALUES", [tuple(x) for x in top_products.to_numpy()])
    
    # ── Tabel 3: department_summary ───────────────────────────────────────
    client.execute("""
        CREATE TABLE IF NOT EXISTS orders_db.department_summary (
            department_id   UInt8,
            department      String,
            total_orders    Int32,
            total_items     Int32,
            reordered_items Int32
        ) ENGINE = MergeTree()
        ORDER BY total_items
    """)
    client.execute("TRUNCATE TABLE orders_db.department_summary")
    if not dept_summary.empty:
        client.execute("INSERT INTO orders_db.department_summary VALUES", [tuple(x) for x in dept_summary.to_numpy()])

    # ── Tabel 4: hourly_activity & analytics.hourly_capacity ──────────────
    client.execute("""
        CREATE TABLE IF NOT EXISTS orders_db.hourly_activity (
            order_hour_of_day UInt8,
            total_orders      Int32,
            total_items       Int32
        ) ENGINE = MergeTree()
        ORDER BY order_hour_of_day
    """)
    client.execute("TRUNCATE TABLE orders_db.hourly_activity")
    if not hourly_activity.empty:
        client.execute("INSERT INTO orders_db.hourly_activity VALUES", [tuple(x) for x in hourly_activity.to_numpy()])

    client.execute("""
        CREATE TABLE IF NOT EXISTS analytics.hourly_capacity (
            order_hour_of_day  UInt8,
            predicted_orders   Int32,
            predicted_items    Int32
        ) ENGINE = MergeTree()
        ORDER BY order_hour_of_day
    """)
    client.execute("TRUNCATE TABLE analytics.hourly_capacity")
    if not hourly_capacity.empty:
        client.execute("INSERT INTO analytics.hourly_capacity VALUES", [tuple(x) for x in hourly_capacity.to_numpy()])

    # ── Tabel 5: analytics.products_performance ───────────────────────────
    client.execute("""
        CREATE TABLE IF NOT EXISTS analytics.products_performance (
            product_id         UInt32,
            product_name       String,
            department         String,
            aisle              String,
            total_sold         Int32,
            total_reordered    Int32,
            avg_cart_position  Float64
        ) ENGINE = MergeTree()
        ORDER BY (total_sold, product_id)
    """)
    client.execute("TRUNCATE TABLE analytics.products_performance")
    if not products_performance.empty:
        client.execute("INSERT INTO analytics.products_performance VALUES", [tuple(x) for x in products_performance.to_numpy()])

    # ── Tabel 6: analytics.sales_forecasting ──────────────────────────────
    client.execute("""
        CREATE TABLE IF NOT EXISTS analytics.sales_forecasting (
            department               String,
            current_total_orders     Int32,
            reorder_probability      Float64,
            forecasted_total_orders  Int32
        ) ENGINE = MergeTree()
        ORDER BY (forecasted_total_orders, department)
    """)
    client.execute("TRUNCATE TABLE analytics.sales_forecasting")
    if not sales_forecasting.empty:
        client.execute("INSERT INTO analytics.sales_forecasting VALUES", [tuple(x) for x in sales_forecasting.to_numpy()])

    # ── Tabel 7: analytics.user_loyalty_segmentation ──────────────────────
    client.execute("""
        CREATE TABLE IF NOT EXISTS analytics.user_loyalty_segmentation (
            user_id                  UInt32,
            total_lifetime_orders    Int32,
            avg_days_between_orders  Float64,
            unique_items_bought      Int32,
            loyalty_status           String,
            churn_risk               String
        ) ENGINE = MergeTree()
        ORDER BY user_id
    """)
    client.execute("TRUNCATE TABLE analytics.user_loyalty_segmentation")
    if not user_loyalty.empty:
        client.execute("INSERT INTO analytics.user_loyalty_segmentation VALUES", [tuple(x) for x in user_loyalty.to_numpy()])

    # ── Tabel 8: analytics.product_cart_priority ──────────────────────────
    client.execute("""
        CREATE TABLE IF NOT EXISTS analytics.product_cart_priority (
            product_id          UInt32,
            product_name        String,
            department          String,
            times_added_first   Int32
        ) ENGINE = MergeTree()
        ORDER BY (times_added_first, product_id)
    """)
    client.execute("TRUNCATE TABLE analytics.product_cart_priority")
    if not cart_priority.empty:
        client.execute("INSERT INTO analytics.product_cart_priority VALUES", [tuple(x) for x in cart_priority.to_numpy()])

    # ── Tabel 9: daily_summary (APPEND harian) ────────────────────────────
    client.execute("""
        CREATE TABLE IF NOT EXISTS orders_db.daily_summary (
            ingested_date    Date,
            total_orders     Int32,
            unique_users     Int32,
            total_items      Int32,
            unique_products  Int32,
            avg_basket_size  Float32,
            reorder_rate_pct Float32
        ) ENGINE = ReplacingMergeTree(ingested_date)
        ORDER BY ingested_date
    """)
    if not daily_summary.empty:
        client.execute("INSERT INTO orders_db.daily_summary VALUES", [tuple(x) for x in daily_summary.to_numpy()])

    # ── Tabel 10: history_department_trend (REALTIME APPEND) ──────────────
    client.execute("""
        CREATE TABLE IF NOT EXISTS analytics.history_department_trend (
            batch_time   DateTime,
            department   String,
            total_orders Int32
        ) ENGINE = MergeTree()
        ORDER BY (batch_time, department)
    """)
    if not dept_trend.empty:
        # Kita simpan di dua database agar Metabase bebas memanggil dari mana saja
        client.execute("CREATE TABLE IF NOT EXISTS orders_db.history_department_trend AS analytics.history_department_trend")
        client.execute("INSERT INTO analytics.history_department_trend VALUES", [tuple(x) for x in dept_trend.to_numpy()])
        client.execute("INSERT INTO orders_db.history_department_trend VALUES", [tuple(x) for x in dept_trend.to_numpy()])

    # ── Tabel 11: data_quality_report ─────────────────────────────────────
    client.execute("""
        CREATE TABLE IF NOT EXISTS orders_db.data_quality_report (
            ingested_date  Date,
            column_name    String,
            total_rows     Int32,
            missing_count  Int32,
            missing_pct    Float32
        ) ENGINE = MergeTree()
        ORDER BY (ingested_date, column_name)
    """)
    client.execute(f"ALTER TABLE orders_db.data_quality_report DELETE WHERE ingested_date = '{today}'")
    if not df_quality.empty:
        client.execute("INSERT INTO orders_db.data_quality_report VALUES", [tuple(x) for x in df_quality.to_numpy()])

    print("✅ Seluruh agregasi berhasil diproses dan disuntikkan ke Warehouse.")

    # ── Bersihkan file parquet yang sudah diproses ────────────────────────
    print("🧹 Membersihkan antrean Data Lake (menghapus Parquet yang selesai)...")
    files = glob.glob("/opt/airflow/data_lake/orders/*.parquet")
    for f in files:
        try:
            os.remove(f)
        except OSError as e:
            print(f"Error saat menghapus {f} : {e.strerror}")

    print(f"🎉 Pipeline Batch Selesai! [{today}]")


if __name__ == "__main__":
    run_spark_analytics()
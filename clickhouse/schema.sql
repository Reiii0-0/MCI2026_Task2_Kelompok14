-- ============================================================
-- ClickHouse Schema: E-Grocery Orders Analytics Warehouse
-- Dataset: http://96.9.212.102:8000/orders
--
-- File ini berfungsi sebagai dokumentasi resmi skema DDL.
-- Seluruh tabel dibuat secara otomatis oleh Spark Pipeline
-- (process_orders_spark.py) untuk memastikan integritas tipe data.
-- ============================================================

-- ============================================================
-- 1. DATABASE DECLARATION
-- ============================================================
CREATE DATABASE IF NOT EXISTS orders_db;
CREATE DATABASE IF NOT EXISTS analytics;


-- ============================================================
-- 2. DATABASE: orders_db (Core Core Warehouse Layer)
-- ============================================================

-- 2.1 Tabel: order_items (Fact Table Denormalized)
-- Satu baris merepresentasikan satu item produk di dalam pesanan.
-- Menggunakan ReplacingMergeTree untuk mendukung dedup data berdasarkan versi ingested_date terbaru.
CREATE TABLE IF NOT EXISTS orders_db.order_items
(
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
ORDER BY (ingested_date, order_id, product_id);

-- 2.2 Tabel: top_products (Heavy Hitters Aggregation)
-- Menyimpan metrik total akumulasi penjualan dan reorder rate produk secara berkala.
CREATE TABLE IF NOT EXISTS orders_db.top_products
(
    product_id             UInt32,
    product_name           String,
    department             String,
    aisle                  String,
    total_orders           Int32,
    reorder_count          Int32
) ENGINE = MergeTree()
ORDER BY total_orders;

-- 2.3 Tabel: department_summary (Kategori Performa)
-- Ringkasan metrik volume item, transaksi, dan tingkat pembelian ulang per departemen ritel.
CREATE TABLE IF NOT EXISTS orders_db.department_summary
(
    department_id          UInt8,
    department             String,
    total_orders           Int32,
    total_items            Int32,
    reordered_items        Int32
) ENGINE = MergeTree()
ORDER BY total_items;

-- 2.4 Tabel: hourly_activity (Time Pattern Metrics)
-- Profil distribusi aktivitas transaksi pelanggan di setiap jam operasional.
CREATE TABLE IF NOT EXISTS orders_db.hourly_activity
(
    order_hour_of_day      UInt8,
    total_orders           Int32,
    total_items            Int32
) ENGINE = MergeTree()
ORDER BY order_hour_of_day;

-- 2.5 Tabel: daily_summary (Executive Business Snapshot)
-- Menyimpan rekaman time-series performa harian untuk kebutuhan pelacakan KPI utama.
CREATE TABLE IF NOT EXISTS orders_db.daily_summary
(
    ingested_date          Date,
    total_orders           Int32,
    unique_users           Int32,
    total_items            Int32,
    unique_products        Int32,
    avg_basket_size        Float32,
    reorder_rate_pct       Float32
) ENGINE = ReplacingMergeTree(ingested_date)
ORDER BY ingested_date;

-- 2.6 Tabel: history_department_trend (Time-Series Operational Tracking)
-- Duplikasi dari skema tren riwayat departemen untuk optimasi query dashboard di database orders_db.
CREATE TABLE IF NOT EXISTS orders_db.history_department_trend
(
    batch_time             DateTime,
    department             String,
    total_orders           Int32
) ENGINE = MergeTree()
ORDER BY (batch_time, department);

-- 2.7 Tabel: data_quality_report (Automated Quality Pipeline Assurance)
-- Menyimpan metrik audit persentase kekosongan data sebelum tahap perbaikan (imputasi/labeling).
CREATE TABLE IF NOT EXISTS orders_db.data_quality_report
(
    ingested_date          Date,
    column_name            String,
    total_rows             Int32,
    missing_count          Int32,
    missing_pct            Float32
) ENGINE = MergeTree()
ORDER BY (ingested_date, column_name);


-- ============================================================
-- 3. DATABASE: analytics (Advanced & Enterprise Predictive Layer)
-- ============================================================

-- 3.1 Tabel: products_performance (Deep Product & Position Insights)
-- Agregasi tingkat lanjut performa produk beserta posisi urutan peletakan di keranjang.
CREATE TABLE IF NOT EXISTS analytics.products_performance
(
    product_id             UInt32,
    product_name           String,
    department             String,
    aisle                  String,
    total_sold             Int32,
    total_reordered        Int32,
    avg_cart_position      Float64
) ENGINE = MergeTree()
ORDER BY (total_sold, product_id);

-- 3.2 Tabel: sales_forecasting (Demand Projections Layer)
-- Menyimpan metrik probabilitas reorder untuk peramalan volume penjualan siklus berikutnya.
CREATE TABLE IF NOT EXISTS analytics.sales_forecasting
(
    department             String,
    current_total_orders   Int32,
    reorder_probability    Float64,
    forecasted_total_orders Int32
) ENGINE = MergeTree()
ORDER BY (forecasted_total_orders, department);

-- 3.3 Tabel: hourly_capacity (Near Real-Time Logistic Readiness)
-- Prediksi kapasitas volume pesanan dan total barang yang harus ditangani per jam operasional.
CREATE TABLE IF NOT EXISTS analytics.hourly_capacity
(
    order_hour_of_day      UInt8,
    predicted_orders       Int32,
    predicted_items        Int32
) ENGINE = MergeTree()
ORDER BY order_hour_of_day;

-- 3.4 Tabel: history_department_trend (Batch Stream Metrics Log)
-- Tabel logging historis untuk memantau pergerakan stabilitas tren penjualan lintas kategori.
CREATE TABLE IF NOT EXISTS analytics.history_department_trend
(
    batch_time             DateTime,
    department             String,
    total_orders           Int32
) ENGINE = MergeTree()
ORDER BY (batch_time, department);

-- 3.5 Tabel: user_loyalty_segmentation (Behavioral & Churn Analytics)
-- Hasil segmentasi pelanggan berbasis kerangka kerja RFM untuk mendeteksi risiko drop-off pengguna.
CREATE TABLE IF NOT EXISTS analytics.user_loyalty_segmentation
(
    user_id                UInt32,
    total_lifetime_orders  Int32,
    avg_days_between_orders Float64,
    unique_items_bought    Int32,
    loyalty_status         String,
    churn_risk             String
) ENGINE = MergeTree()
ORDER BY user_id;

-- 3.6 Tabel: product_cart_priority (Market Basket MBA Trigger Product)
-- Menyimpan akumulasi seberapa sering suatu produk ditempatkan pada urutan pertama di dalam keranjang.
CREATE TABLE IF NOT EXISTS analytics.product_cart_priority
(
    product_id             UInt32,
    product_name           String,
    department             String,
    times_added_first      Int32
) ENGINE = MergeTree()
ORDER BY (times_added_first, product_id);


-- ============================================================
-- 4. WAREHOUSE INTEGRITY VERIFICATION QUERIES
-- ============================================================
-- Query pengujian internal untuk memvalidasi keberhasilan pemuatan data:
--
-- SELECT count() FROM orders_db.order_items;
-- SELECT * FROM orders_db.data_quality_report ORDER BY ingested_date DESC LIMIT 10;
-- SELECT * FROM analytics.sales_forecasting ORDER BY forecasted_total_orders DESC;
-- SELECT * FROM analytics.user_loyalty_segmentation LIMIT 5;
-- ============================================================
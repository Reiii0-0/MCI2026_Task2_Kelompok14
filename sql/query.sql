-- =============================================================================
-- Order Analytics Dashboard Queries
-- Source restored from project documentation and aligned to current schema.
-- =============================================================================

-- =============================================================================
-- TAB 1 - General Overview
-- =============================================================================

-- 1.1 Total Order
SELECT count(DISTINCT order_id) AS total_orders
FROM orders_db.order_items

-- 1.2 Total Unique Products
SELECT count(DISTINCT product_id) AS unique_products
FROM orders_db.order_items

-- 1.3 Total Unique Customers
SELECT count(DISTINCT user_id) AS unique_users
FROM orders_db.order_items

-- 1.4 Overall Reorder Rate
SELECT round(countIf(reordered = 1) / count(*) * 100, 1) AS reorder_rate_pct
FROM orders_db.order_items

-- 1.5 Best Selling Products
SELECT product_name, total_orders, reorder_count
FROM orders_db.top_products
ORDER BY total_orders DESC
LIMIT 10

-- 1.6 Most Popular Aisles
SELECT aisle, count(*) AS total_items
FROM orders_db.order_items
GROUP BY aisle
ORDER BY total_items DESC
LIMIT 10

-- 1.7 Reorder vs New Purchase
SELECT
    IF(reordered = 1, 'Reorder', 'Baru') AS tipe,
    count(*) AS jumlah
FROM orders_db.order_items
GROUP BY tipe

-- 1.8 Top 3 vs Bottom 3 Aisle
SELECT
    concat(aisle, ' (', department, ')') AS aisle_label,
    count(DISTINCT product_id) AS jumlah_produk_unik,
    count(*) AS total_orders,
    round(count(*) / count(DISTINCT product_id), 1) AS orders_per_produk,
    'Top Performer' AS kategori
FROM orders_db.order_items
GROUP BY aisle, department
HAVING total_orders >= 10
ORDER BY orders_per_produk DESC
LIMIT 3

UNION ALL

SELECT
    concat(aisle, ' (', department, ')') AS aisle_label,
    count(DISTINCT product_id) AS jumlah_produk_unik,
    count(*) AS total_orders,
    round(count(*) / count(DISTINCT product_id), 1) AS orders_per_produk,
    'Bottom Performer' AS kategori
FROM orders_db.order_items
GROUP BY aisle, department
HAVING total_orders >= 10
ORDER BY orders_per_produk ASC
LIMIT 3

-- =============================================================================
-- TAB 2 - Knowledge Detail
-- =============================================================================

-- 2.1 Order Distribution per Day
SELECT
    order_dow,
    CASE order_dow
        WHEN 0 THEN 'Minggu'
        WHEN 1 THEN 'Senin'
        WHEN 2 THEN 'Selasa'
        WHEN 3 THEN 'Rabu'
        WHEN 4 THEN 'Kamis'
        WHEN 5 THEN 'Jumat'
        WHEN 6 THEN 'Sabtu'
    END AS hari,
    COUNT(DISTINCT order_id) AS total_orders
FROM orders_db.order_items
GROUP BY order_dow
ORDER BY order_dow ASC


-- 2.2 Hourly Order Activity
SELECT
    order_hour_of_day AS jam,
    count(DISTINCT order_id) AS total_orders
FROM orders_db.order_items
GROUP BY jam
ORDER BY total_orders ASC
LIMIT 6;

-- 2.3 Total Items per Department
SELECT department, total_items, reordered_items
FROM orders_db.department_summary
ORDER BY total_items DESC

-- 2.4 Shopping Distribution Frequency
SELECT
    CASE
        WHEN days_since_prior_order IS NULL THEN 'First Order'
        WHEN days_since_prior_order <= 7 THEN '0-7 hari'
        WHEN days_since_prior_order <= 14 THEN '8-14 hari'
        WHEN days_since_prior_order <= 21 THEN '15-21 hari'
        ELSE '22-30 hari'
    END AS frequency_bucket,
    count(DISTINCT order_id) AS total_orders
FROM orders_db.order_items
GROUP BY frequency_bucket
ORDER BY total_orders DESC

-- 2.5 Time Pattern
SELECT
    order_hour_of_day AS jam,
    uniqExactIf(order_id, order_dow = 0) AS Minggu,
    uniqExactIf(order_id, order_dow = 1) AS Senin,
    uniqExactIf(order_id, order_dow = 2) AS Selasa,
    uniqExactIf(order_id, order_dow = 3) AS Rabu,
    uniqExactIf(order_id, order_dow = 4) AS Kamis,
    uniqExactIf(order_id, order_dow = 5) AS Jumat,
    uniqExactIf(order_id, order_dow = 6) AS Sabtu
FROM orders_db.order_items
GROUP BY jam
ORDER BY jam;


-- =============================================================================
-- TAB 3 - Predictive Analytics & Forecasting
-- =============================================================================

-- 3.1 Churn Risk Segmentation
SELECT 
    loyalty_status,
    churn_risk,
    COUNT(user_id) AS total_users,
    ROUND(AVG(avg_days_between_orders), 1) AS avg_days_gap
FROM analytics.user_loyalty_segmentation
GROUP BY loyalty_status, churn_risk
ORDER BY total_users DESC;

-- 3.2 Next Cycle Demand Prediction (Stockout/Overstock Warning)
SELECT 
    department,
    current_total_orders,
    forecasted_total_orders,
    (forecasted_total_orders - current_total_orders) AS gap_prediksi,
    CASE 
        WHEN forecasted_total_orders > current_total_orders THEN 'Potential Overstock'
        WHEN forecasted_total_orders < current_total_orders THEN 'Potential Stockout'
        ELSE 'Balanced'
    END AS status_prediksi
FROM analytics.sales_forecasting
ORDER BY ABS(gap_prediksi) DESC;

-- 3.3 Inventory Risk Matrix
SELECT
    department,
    current_total_orders AS volume_berjalan,
    ROUND(reorder_probability * 100, 2) AS peluang_terjual_kembali_persen,
    CASE
        WHEN reorder_probability < 0.25 THEN 'Tinggi (Tahan Supply!)'
        WHEN reorder_probability < 0.45 THEN 'Sedang (Pantau)'
        ELSE 'Aman'
    END AS tingkat_risiko_overstock
FROM analytics.sales_forecasting
ORDER BY peluang_terjual_kembali_persen ASC;

-- 3.4 Smoothed Future Trend (NRT Moving Average)
WITH GlobalSales AS (
    SELECT 
        batch_time,
        SUM(total_orders) AS total_orders_asli
    FROM analytics.history_department_trend
    WHERE toDate(batch_time) = today()
    GROUP BY batch_time
)
SELECT 
    batch_time,
    total_orders_asli,
    ROUND(AVG(total_orders_asli) OVER (
        ORDER BY batch_time ASC 
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ), 2) AS tren_total_orders_dihaluskan
FROM GlobalSales
ORDER BY batch_time ASC;

-- 3.5 First-In-Cart Priority (Trigger Product)
SELECT 
    product_name,
    department,
    times_added_first
FROM analytics.product_cart_priority
ORDER BY times_added_first DESC
LIMIT 15;

-- 3.6 RFM Lifetime Value Distribution
SELECT 
    loyalty_status,
    ROUND(AVG(total_lifetime_orders), 1) AS rata_rata_pembelian_seumur_hidup,
    ROUND(AVG(unique_items_bought), 1) AS rata_rata_variasi_barang
FROM analytics.user_loyalty_segmentation
GROUP BY loyalty_status
ORDER BY rata_rata_pembelian_seumur_hidup DESC;


-- =============================================================================
-- TAB 4 - Rekomendasi Improvements
-- =============================================================================

-- 4.1 One Hit Wonder Products
SELECT
    product_name,
    department,
    aisle,
    count(*) AS total_dibeli,
    countIf(reordered = 1) AS total_reorder,
    round(countIf(reordered = 1) / count(*) * 100, 1) AS reorder_rate_pct
FROM orders_db.order_items
GROUP BY product_name, department, aisle
HAVING total_dibeli >= 2 AND reorder_rate_pct = 0
ORDER BY total_dibeli DESC
LIMIT 20

-- 4.2 Loyal Favorites
SELECT
    product_name,
    department,
    aisle,
    count(*) AS total_orders,
    round(countIf(reordered = 1) / count(*) * 100, 1) AS reorder_rate_pct
FROM orders_db.order_items
GROUP BY product_name, department, aisle
HAVING total_orders >= 2 AND reorder_rate_pct >= 80
ORDER BY reorder_rate_pct DESC, total_orders DESC
LIMIT 15

-- 4.3 Customer Churn
SELECT
    user_id,
    max(order_number) AS order_terakhir_ke,
    max(days_since_prior_order) AS hari_absen_terakhir,
    count(DISTINCT order_id) AS total_orders_recorded
FROM orders_db.order_items
WHERE days_since_prior_order IS NOT NULL
GROUP BY user_id
HAVING hari_absen_terakhir > 21
ORDER BY hari_absen_terakhir DESC

-- 4.4 Shopping Frequency Segment
SELECT
    CASE
        WHEN days_since_prior_order IS NULL THEN 'First Order'
        WHEN days_since_prior_order <= 7   THEN '01. Weekly (≤7 hari)'
        WHEN days_since_prior_order <= 14  THEN '02. Biweekly (8-14 hari)'
        WHEN days_since_prior_order <= 21  THEN '03. 3x/bulan (15-21 hari)'
        ELSE                                    '04. Jarang (>21 hari)'
    END AS segmen_frekuensi,
    count(DISTINCT order_id) AS total_orders,
    round(count(DISTINCT order_id) * 100.0 /
          (SELECT count(DISTINCT order_id) FROM orders_db.order_items), 1) AS pct
FROM orders_db.order_items
GROUP BY segmen_frekuensi
ORDER BY segmen_frekuensi

-- 4.5 Loyalty: Engagement per Nth Order
SELECT
    order_number AS order_ke,
    count(DISTINCT order_id) AS jumlah_order,
    round(avg(items_per_order), 1) AS avg_items,
    round(avg(reorder_pct), 1) AS avg_reorder_pct
FROM (
    SELECT order_id, order_number,
        count(*) AS items_per_order,
        countIf(reordered = 1) * 100.0 / count(*) AS reorder_pct
    FROM orders_db.order_items
    GROUP BY order_id, order_number
)
GROUP BY order_ke
HAVING order_ke <= 30
ORDER BY order_ke

-- 4.6 Afterthought Products
SELECT
    product_name, department, aisle,
    count(*) AS frequency,
    round(avg(add_to_cart_order), 1) AS avg_cart_position,
    round(countIf(reordered = 1) * 100.0 / count(*), 1) AS reorder_rate_pct
FROM orders_db.order_items
GROUP BY product_name, department, aisle
HAVING frequency >= 2
ORDER BY avg_cart_position DESC
LIMIT 15

-- 4.7 Basket Position vs Reorder Rate
SELECT
    CASE
        WHEN add_to_cart_order <= 3 THEN 'Top 3 (kebutuhan utama)'
        WHEN add_to_cart_order <= 7 THEN 'Tengah (4-7)'
        ELSE 'Akhir (8+, afterthought)'
    END AS posisi_keranjang,
    CASE
        WHEN add_to_cart_order <= 3 THEN 1
        WHEN add_to_cart_order <= 7 THEN 2
        ELSE 3
    END AS sort_order,
    count(*) AS total_items,
    round(countIf(reordered = 1) * 100.0 / count(*), 1) AS reorder_rate_pct
FROM orders_db.order_items
GROUP BY posisi_keranjang, sort_order
ORDER BY sort_order ASC

-- 4.8 Department Segmentation
SELECT
    department,
    count(*) AS total_items,
    round(countIf(reordered = 1) * 100.0 / count(*), 1) AS reorder_rate_pct,
    count(DISTINCT product_id) AS jumlah_produk,
    CASE
        WHEN count(*) < 30 AND countIf(reordered=1)*100/count(*) > 60
        THEN 'Perlu dipromosikan!'
        WHEN count(*) >= 30 AND countIf(reordered=1)*100/count(*) > 60
        THEN 'Sudah baik, pertahankan'
        WHEN count(*) >= 30 AND countIf(reordered=1)*100/count(*) < 40
        THEN 'Volume OK tapi reorder rendah'
        ELSE 'Perlu perhatian'
    END AS rekomendasi
FROM orders_db.order_items
GROUP BY department
ORDER BY reorder_rate_pct DESC


-- =============================================================================
-- TAB 5 - Data Governance & Pipeline Health
-- =============================================================================

-- 5.1 Ultimate Data Quality Scorecard
SELECT
    max(total_rows) AS total_baris_raw,
    max(missing_count) AS max_missing_satu_kolom,
    round((1 - max(missing_pct) / 100.0) * 100, 1) AS estimasi_pct_data_bersih,
    countIf(missing_count > 0) AS kolom_bermasalah,
    count(*) AS total_kolom_diperiksa
FROM orders_db.data_quality_report;

-- 5.2 Missing Value Audit Trail
SELECT
    column_name,
    total_rows,
    missing_count,
    missing_pct,
    CASE
        WHEN missing_count = 0 THEN 'AMAN (tidak ada missing data)'
        WHEN column_name IN ('order_id', 'product_id', 'user_id') THEN 'DROP (kolom kunci — baris dihapus)'
        WHEN column_name = 'days_since_prior_order' THEN 'KEEP NULL (first order valid)'
        ELSE 'IMPUTE (diisi nilai default)'
    END AS strategi_cleaning
FROM orders_db.data_quality_report
ORDER BY missing_pct DESC;

-- 5.3 ETL Data Loss Reconciliation (Pipeline Integrity Check)
SELECT 
    (SELECT COUNT(*) FROM orders_db.order_items) AS data_raw_masuk,
    (SELECT SUM(total_items) FROM orders_db.department_summary) AS agregasi_terproses,
    (SELECT COUNT(*) FROM orders_db.order_items) - (SELECT SUM(total_items) FROM orders_db.department_summary) AS deviasi_kehilangan_data;

-- 5.4 Null Imputation Impact (Dampak Data Rusak pada Sales)
SELECT 
    SUM(IF(department = 'missing', total_sold, 0)) AS total_barang_tanpa_departemen,
    ROUND(SUM(IF(department = 'missing', total_sold, 0)) / SUM(total_sold) * 100, 3) AS persen_dampak_terhadap_sales
FROM analytics.products_performance;

-- 5.5 Pipeline Heartbeat (Real-time Load Tracker)
SELECT 
    batch_time AS Waktu_Eksekusi_DAG,
    SUM(total_orders) AS Beban_Order_Diproses
FROM orders_db.history_department_trend
GROUP BY batch_time
ORDER BY batch_time DESC
LIMIT 12;

-- 5.6 Data Freshness / System Latency
SELECT 
    MAX(batch_time) AS update_terakhir,
    dateDiff('minute', MAX(batch_time), now()) AS latensi_menit_dari_sekarang
FROM orders_db.history_department_trend;


-- =============================================================================
-- TAB 6 - AI & Behavioral Prediction Engine (AMAZON STYLE)
-- =============================================================================

-- 6.1 Shopping Persona Matrix (Segmentasi Waktu)
SELECT
    IF(order_dow IN (0, 1), '1. Weekend Shopper', '2. Weekday Planner') AS tipe_hari,
    CASE
        WHEN order_hour_of_day BETWEEN 5 AND 11 THEN '1. Pagi (Morning)'
        WHEN order_hour_of_day BETWEEN 12 AND 16 THEN '2. Siang (Afternoon)'
        WHEN order_hour_of_day BETWEEN 17 AND 21 THEN '3. Malam (Evening)'
        ELSE '4. Larut Malam (Night)'
    END AS jam_belanja,
    COUNT(DISTINCT order_id) AS total_transaksi,
    ROUND(AVG(days_since_prior_order), 1) AS avg_hari_kembali
FROM orders_db.order_items
GROUP BY tipe_hari, jam_belanja
ORDER BY tipe_hari ASC, jam_belanja ASC;

-- 6.2 The Gini/Pareto Curve Logic (Distribusi Kesenjangan Produk)
WITH ProductStats AS (
    SELECT product_name, total_sold
    FROM analytics.products_performance
    WHERE total_sold > 0
),
RankedProducts AS (
    SELECT product_name, total_sold,
        SUM(total_sold) OVER (ORDER BY total_sold DESC) AS cumulative_sold,
        SUM(total_sold) OVER () AS global_sold,
        ROW_NUMBER() OVER (ORDER BY total_sold DESC) AS product_rank,
        COUNT(*) OVER () AS total_products
    FROM ProductStats
)
SELECT 
    product_name,
    ROUND((product_rank / total_products) * 100, 2) AS persentase_populasi_produk,
    ROUND((cumulative_sold / global_sold) * 100, 2) AS persentase_kumulatif_penjualan
FROM RankedProducts
ORDER BY product_rank
LIMIT 100;

-- 6.3 Cart Fatigue Indicator (Kelelahan Belanja Konsumen)
SELECT
    add_to_cart_order AS urutan_masuk_keranjang,
    COUNT(product_id) AS total_items_terjual,
    ROUND(AVG(reordered) * 100, 2) AS probabilitas_reorder_pct
FROM orders_db.order_items
WHERE add_to_cart_order <= 20
GROUP BY urutan_masuk_keranjang
ORDER BY urutan_masuk_keranjang ASC;

-- 6.4 Cross-Selling Department Affinity
SELECT
    a.department AS departemen_awal,
    b.department AS departemen_tujuan_selanjutnya,
    COUNT(DISTINCT a.order_id) AS transaksi_lintas_kategori
FROM orders_db.order_items a
JOIN orders_db.order_items b 
  ON a.order_id = b.order_id AND a.department != b.department
GROUP BY departemen_awal, departemen_tujuan_selanjutnya
ORDER BY transaksi_lintas_kategori DESC
LIMIT 20;

-- 6.5 Algoritma Rekomendasi Amazon (Frequently Bought Together + Confidence)
WITH ProductFrequencies AS (
    SELECT product_name, COUNT(DISTINCT order_id) AS count_A
    FROM orders_db.order_items
    GROUP BY product_name
),
PairFrequencies AS (
    SELECT
        a.product_name AS product_A,
        b.product_name AS product_B,
        COUNT(DISTINCT a.order_id) AS count_AB
    FROM orders_db.order_items a
    JOIN orders_db.order_items b 
      ON a.order_id = b.order_id AND a.product_id != b.product_id
    GROUP BY product_A, product_B
)
SELECT
    pf.product_A AS jika_user_beli_ini,
    pf.product_B AS maka_rekomendasikan_ini,
    pf.count_AB AS frekuensi_beli_bersama,
    ROUND((pf.count_AB / f.count_A) * 100, 2) AS confidence_score_pct
FROM PairFrequencies pf
JOIN ProductFrequencies f ON pf.product_A = f.product_name
WHERE pf.count_AB >= 2 
ORDER BY confidence_score_pct DESC, frekuensi_beli_bersama DESC
LIMIT 15;

-- 6.6 Smart Restock Reminder Engine (Siklus Cepat Habis)
SELECT
    product_name,
    COUNT(order_id) AS total_reorders,
    ROUND(AVG(days_since_prior_order), 1) AS siklus_pembelian_ulang_hari
FROM orders_db.order_items
WHERE reordered = 1 AND days_since_prior_order IS NOT NULL
GROUP BY product_name
HAVING total_reorders > 2
ORDER BY siklus_pembelian_ulang_hari ASC
LIMIT 15;
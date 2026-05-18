# 📊 Dokumentasi Query — Order Analytics Dashboard
> **Database:** ClickHouse (`orders_db.order_items`, dkk)
> **Pipeline:** REST API → PySpark → ClickHouse → Metabase  
> **Dataset:** Dinamis (Di-append & diperbarui setiap 10 menit via Apache Airflow)

---

## 🗂️ Struktur Dashboard

| Tab | Nama | Fokus |
|-----|------|-------|
| Tab 1 | **General Overview** | KPI ringkasan bisnis, performa produk terlaris, & kategori |
| Tab 2 | **Knowledge Detail** | Analisis perilaku waktu, retensi pelanggan, & struktur keranjang |
| Tab 3 | **Rekomendasi Improvements** | Loyalitas produk spesifik, potensi ekspansi departemen/aisle |
| Tab 4 | **Data Quality Audit** | Laporan kesehatan data, missing values, & penanganan anomali |
| Tab 5 | **Real-Time Monitoring** | Pemantauan tren pesanan secara real-time (interval 10 menit) |

---

## 📋 TAB 1 — General Overview

### 🔢 Q1 — Executive KPI Summary

```sql
SELECT
    count(DISTINCT order_id)                               AS total_orders,
    count(DISTINCT user_id)                                AS total_customers,
    count(DISTINCT product_id)                             AS total_products,
    round(count(*) / count(DISTINCT order_id), 1)         AS avg_basket_size,
    round(countIf(reordered=1) * 100.0 / count(*), 1)    AS overall_reorder_rate_pct,
    round(avg(days_since_prior_order), 1)                  AS avg_days_between_orders,
    count(DISTINCT department)                             AS active_departments
FROM orders_db.order_items
WHERE days_since_prior_order IS NOT NULL
```
**Fungsi:** Menghasilkan metrik ringkasan tingkat C-level yang ditampilkan sebagai deretan kartu KPI di baris paling atas dashboard.

### 🛍️ Q2 — Best Selling Products
```sql
SELECT product_name, count(*) AS total_ordered FROM orders_db.order_items GROUP BY product_name ORDER BY total_ordered DESC LIMIT 10
```
**Fungsi:** Mengidentifikasi 10 produk paling laku berdasarkan kemunculan di semua order untuk acuan stok utama.

### 🏪 Q3 — Most Popular Aisle
```sql
SELECT aisle, count(*) AS total_items FROM orders_db.order_items GROUP BY aisle ORDER BY total_items DESC LIMIT 10
```
**Fungsi:** Menunjukkan lorong/kategori produk mana yang paling mendominasi volume transaksi.

### 🔄 Q4 — Reorder vs New Purchase
```sql
SELECT CASE WHEN reordered=1 THEN 'Reorder' ELSE 'Baru' END AS tipe_pembelian, count(*) AS total_items FROM orders_db.order_items GROUP BY tipe_pembelian
```
**Fungsi:** Membandingkan proporsi secara langsung seberapa kuat loyalitas pelanggan secara umum.

### 🏆 Q5 — Top 3 vs Bottom 3 Aisle
```sql
SELECT aisle, count(*) AS total_items, 'Top' AS kategori FROM orders_db.order_items GROUP BY aisle ORDER BY total_items DESC LIMIT 3
UNION ALL
SELECT aisle, count(*) AS total_items, 'Bottom' AS kategori FROM orders_db.order_items GROUP BY aisle ORDER BY total_items ASC LIMIT 3
```
**Fungsi:** Menampilkan kontras ekstrem antara lorong unggulan dan lorong yang paling minim transaksi untuk strategi merchandising.

---

## 📋 TAB 2 — Knowledge Detail

### ⏰ Q6 — Heatmap Jam × Hari (Pivot Table)
```sql
SELECT
    order_hour_of_day AS jam,
    count(DISTINCT CASE WHEN order_dow = 0 THEN order_id END) AS Minggu,
    count(DISTINCT CASE WHEN order_dow = 1 THEN order_id END) AS Senin,
    count(DISTINCT CASE WHEN order_dow = 2 THEN order_id END) AS Selasa,
    count(DISTINCT CASE WHEN order_dow = 3 THEN order_id END) AS Rabu,
    count(DISTINCT CASE WHEN order_dow = 4 THEN order_id END) AS Kamis,
    count(DISTINCT CASE WHEN order_dow = 5 THEN order_id END) AS Jumat,
    count(DISTINCT CASE WHEN order_dow = 6 THEN order_id END) AS Sabtu
FROM orders_db.order_items
GROUP BY jam
ORDER BY jam;
```
**Fungsi:** Memetakan distribusi jumlah pesanan unik berdasarkan kombinasi hari dan jam. Digunakan untuk merencanakan waktu terbaik peluncuran promosi (traffic tertinngi).

### 🌙 Q7 — Jam Paling Sepi (Dead Hours)
```sql
SELECT order_hour_of_day AS jam, count(DISTINCT order_id) AS total_orders FROM orders_db.order_items GROUP BY jam ORDER BY total_orders ASC LIMIT 6
```
**Fungsi:** Menyoroti jam operasional paling "mati". Cocok sebagai basis pelaksanaan "Flash Sale / Midnight Sale".

### 📅 Q8 — Weekday vs Weekend
```sql
SELECT CASE WHEN order_dow IN (0, 6) THEN 'Weekend' ELSE 'Weekday' END AS tipe_hari, count(DISTINCT order_id) AS total_orders, round(avg(total_items_per_order), 1) AS avg_items FROM (SELECT order_dow, order_id, count(*) AS total_items_per_order FROM orders_db.order_items GROUP BY order_dow, order_id) GROUP BY tipe_hari
```
**Fungsi:** Membandingkan gaya konsumsi dan ukuran keranjang belanja antara akhir pekan vs hari kerja biasa.

### ⚠️ Q9 — Customer Churn Detection
```sql
SELECT user_id, max(order_number) AS order_terakhir_ke, max(days_since_prior_order) AS hari_absen_terakhir, count(DISTINCT order_id) AS total_orders_recorded FROM orders_db.order_items WHERE days_since_prior_order IS NOT NULL GROUP BY user_id HAVING hari_absen_terakhir > 21 ORDER BY hari_absen_terakhir DESC
```
**Fungsi:** Mendeteksi spesifik pelanggan yang "menghilang" lebih dari 21 hari sejak pembelian terakhirnya untuk ditarget oleh *win-back campaign*.

### 📊 Q10 — Shopping Frequency Segmentation
```sql
SELECT CASE WHEN days_since_prior_order IS NULL THEN 'First Order' WHEN days_since_prior_order <= 7 THEN '01. Weekly' WHEN days_since_prior_order <= 14 THEN '02. Biweekly' WHEN days_since_prior_order <= 21 THEN '03. 3x/bulan' ELSE '04. Jarang (>21 hari)' END AS segmen, count(DISTINCT order_id) AS total_orders FROM orders_db.order_items GROUP BY segmen ORDER BY segmen
```
**Fungsi:** Mengelompokkan porsi audiens berdasarkan seberapa rutin mereka berbelanja.

### 📈 Q11 — Loyalty: Engagement per Nth Order
```sql
SELECT order_number AS order_ke, count(DISTINCT order_id) AS jumlah_order, round(avg(items_per_order), 1) AS avg_items, round(avg(reorder_pct), 1) AS avg_reorder_pct FROM (SELECT order_id, order_number, count(*) AS items_per_order, countIf(reordered = 1) * 100.0 / count(*) AS reorder_pct FROM orders_db.order_items GROUP BY order_id, order_number) GROUP BY order_ke HAVING order_ke <= 30 ORDER BY order_ke
```
**Fungsi:** Membuktikan hipotesis apakah pelanggan yang kembali belanja berkali-kali akan membentuk keranjang yang lebih besar.

---

## 📋 TAB 3 — Rekomendasi Improvements

### 💀 Q12 — One Hit Wonder Products
```sql
SELECT product_name, department, aisle, count(*) AS total_dibeli, countIf(reordered = 1) AS total_reorder, round(countIf(reordered = 1) / count(*) * 100, 1) AS reorder_rate_pct FROM orders_db.order_items GROUP BY product_name, department, aisle HAVING total_dibeli >= 2 AND reorder_rate_pct = 0 ORDER BY total_dibeli DESC LIMIT 20
```
**Fungsi:** Menemukan produk bermasalah (cukup populer untuk dicoba, tapi tidak ada satupun yang membelinya kembali).

### ⭐ Q13 — Loyal Favorites
```sql
SELECT product_name, department, aisle, count(*) AS total_orders, round(countIf(reordered = 1) / count(*) * 100, 1) AS reorder_rate_pct FROM orders_db.order_items GROUP BY product_name, department, aisle HAVING total_orders >= 2 AND reorder_rate_pct >= 80 ORDER BY reorder_rate_pct DESC, total_orders DESC LIMIT 15
```
**Fungsi:** Pilar utama produk; tingkat pembelian ulangnya nyaris sempurna. Pantang mengalami kekosongan stok.

### 🎯 Q14 — Afterthought Products (Bubble Chart)
```sql
SELECT product_name, department, aisle, count(*) AS frequency, round(avg(add_to_cart_order), 1) AS avg_cart_position, round(countIf(reordered = 1) * 100.0 / count(*), 1) AS reorder_rate_pct FROM orders_db.order_items GROUP BY product_name, department, aisle HAVING frequency >= 2 ORDER BY avg_cart_position DESC LIMIT 15
```
**Fungsi:** Mencari produk "tambahan" yang baru dimasukkan keranjang di saat akhir checkout.

### 📦 Q15 — Basket Position vs Reorder Rate
```sql
SELECT CASE WHEN add_to_cart_order <= 3 THEN 'Top 3 (kebutuhan)' WHEN add_to_cart_order <= 7 THEN 'Tengah (4-7)' ELSE 'Akhir (8+)' END AS posisi_keranjang, count(*) AS total_items, round(countIf(reordered = 1) * 100.0 / count(*), 1) AS reorder_rate_pct FROM orders_db.order_items GROUP BY posisi_keranjang ORDER BY reorder_rate_pct DESC
```
**Fungsi:** Membandingkan loyalitas produk antara barang kebutuhan mutlak vs "impulse buying".

### 🏢 Q16 — Department Segmentation (BCG Matrix)
```sql
SELECT department, count(*) AS total_items, round(countIf(reordered = 1) * 100.0 / count(*), 1) AS reorder_rate_pct, count(DISTINCT product_id) AS jumlah_produk FROM orders_db.order_items GROUP BY department ORDER BY reorder_rate_pct DESC
```
**Fungsi:** Menghasilkan data untuk Bubble Scatter chart guna memetakan potensi tiap departemen.

### ➕ Q17 — Aisle Kekurangan SKU Baru
```sql
SELECT aisle, department, count(DISTINCT product_id) AS jumlah_produk_unik, count(*) AS total_orders, round(count(*) / count(DISTINCT product_id), 1) AS orders_per_produk FROM orders_db.order_items GROUP BY aisle, department HAVING total_orders >= 10 ORDER BY orders_per_produk DESC LIMIT 15
```
**Fungsi:** Mencari *aisle* dengan "permintaan tinggi tapi pilihan barang sangat terbatas", target utama merilis varian rasa/merk baru.

---

## 🛡️ TAB 4 — Data Quality Audit (Missing Values)

### 📊 Q18 — Data Quality KPIs (% Data Bersih)
```sql
SELECT
    max(total_rows) AS total_baris_raw,
    max(missing_count) AS max_missing_satu_kolom,
    round((1 - max(missing_pct) / 100.0) * 100, 1) AS estimasi_pct_data_bersih,
    countIf(missing_count > 0) AS kolom_bermasalah,
    count(*) AS total_kolom_diperiksa
FROM orders_db.data_quality_report
```
**Fungsi:** Menghitung persentase kesehatan agregat aliran data untuk memantau integritas *upstream API*.

### 📉 Q19 — Missing Data per Kolom
```sql
SELECT
    column_name,
    missing_count,
    missing_pct,
    total_rows
FROM orders_db.data_quality_report
ORDER BY missing_count DESC
```
**Fungsi:** Bar chart horizontal untuk melihat volume hilangnya data di tiap variabel secara sepintas.

### 🛡️ Q20 — Detail Audit Trail & Strategi Cleaning
```sql
SELECT
    column_name,
    total_rows,
    missing_count,
    missing_pct,
    CASE
        WHEN missing_count = 0 THEN '✅ AMAN (tidak ada missing data)'
        WHEN column_name IN ('order_id', 'product_id', 'user_id') THEN '⚠️ DROP (kolom kunci — baris dihapus)'
        WHEN column_name = 'days_since_prior_order' THEN 'ℹ️ KEEP NULL (first order valid)'
        ELSE '🔄 IMPUTE (diisi nilai default)'
    END AS strategi_cleaning
FROM orders_db.data_quality_report
ORDER BY missing_pct DESC;
```
**Fungsi:** Mendokumentasikan aturan sistem saat menangani data kosong; menyajikan penjelasan transparan kapan data dibuang, diimputasi, atau dibiarkan.

---

## 📈 TAB 5 — Real-Time Monitoring

### ⏱️ Q21 — Real-Time Department Trend (10-Minute Batch)
```sql
SELECT 
    batch_time,
    department,
    total_orders AS total_order_saat_ini
FROM orders_db.history_department_trend
ORDER BY batch_time DESC, department ASC;
```
**Fungsi:** Melacak performa penjualan tiap departemen secara real-time. Tabel di-refresh secara periodik dari Apache Airflow setiap kali masuk batch 10-menit terbaru.

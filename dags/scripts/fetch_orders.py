"""
Task 1: Fetch Orders dari REST API → Data Lake (.parquet)
==========================================================
Mengambil data dari http://96.9.212.102:8000/orders,
melakukan normalisasi (flatten nested products),
dan menyimpan hasilnya ke Data Lake dalam format Parquet (V1.0).
"""

import requests
import pandas as pd
import os
from datetime import datetime

def fetch_orders():
    print("🔄 Membuka keran data: API Orders (http://96.9.212.102:8000/orders)...")
    API_URL = "http://96.9.212.102:8000/orders"
    
    try:
        # Timeout 30 detik untuk standar industri
        response = requests.get(API_URL, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        orders = data.get("orders", [])
        total = data.get("total_orders", 0)
        print(f"📦 Menerima {total} orders dari API.")
        
        # ── Flatten: 1 baris per produk per order ──────────────────────────
        # Spark sangat membenci nested JSON array. Kita ratakan di sini!
        parsed_data = []
        for order in orders:
            for product in order.get("products", []):
                parsed_data.append({
                    # Order-level
                    "order_id":              order.get("order_id"),
                    "user_id":               order.get("user_id"),
                    "order_number":          order.get("order_number"),
                    "order_dow":             order.get("order_dow"),
                    "order_hour_of_day":     order.get("order_hour_of_day"),
                    "days_since_prior_order": order.get("days_since_prior_order"),
                    "eval_set":              order.get("eval_set", "prior"),
                    # Product-level
                    "product_id":            product.get("product_id"),
                    "product_name":          product.get("product_name"),
                    "aisle_id":              product.get("aisle_id"),
                    "aisle":                 product.get("aisle"),
                    "department_id":         product.get("department_id"),
                    "department":            product.get("department"),
                    "add_to_cart_order":     product.get("add_to_cart_order"),
                    "reordered":             product.get("reordered"),
                })
                
        df = pd.DataFrame(parsed_data)
        
        # ── Simpan ke Data Lake lokal (.parquet) ───────────────────────────
        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = f"/opt/airflow/data_lake/orders/orders_{current_time}.parquet"
        
        # Buat folder jika belum ada
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # 🔥 FIX KRUSIAL: Memaksa Parquet version 1.0 agar dibaca Spark JVM tanpa error
        df.to_parquet(output_path, index=False, engine='pyarrow', version='1.0')
        
        print(f"✅ Sukses menyimpan {len(df)} baris ke {output_path}")
        print(f"   ({len(orders)} orders → {len(df)} order-items yang sudah diratakan)")
        
    except Exception as e:
        print(f"❌ Gagal menarik data: {e}")
        raise

if __name__ == "__main__":
    fetch_orders()
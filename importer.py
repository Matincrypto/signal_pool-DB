# importer.py (نسخه v4.0 - با زمان تهران)

import requests
import mysql.connector
from mysql.connector import Error
from config import db_config
import time
import json
# --- ۱. اضافه کردن کتابخانه‌های زمان ---
from datetime import datetime, timedelta, timezone

API_URLS = [
    "http://103.75.198.172:5005/Internal/arbitrage",
    "http://103.75.198.172:8888/g1/signals", 
    "http://103.75.198.172:8889/computational/signals"
]

def get_signal_grade(profit_percentage):
    try:
        profit = float(profit_percentage)
        if profit >= 7: return 'Q1'
        elif profit >= 5: return 'Q2'
        elif profit >= 3: return 'Q3'
        else: return 'Q4'
    except (ValueError, TypeError):
        return 'N/A'

def get_tehran_time():
    """زمان فعلی را به وقت تهران (UTC+3:30) برمی‌گرداند"""
    # تعریف اختلاف زمانی ایران (3 ساعت و 30 دقیقه جلوتر از UTC)
    tehran_offset = timedelta(hours=3, minutes=30)
    tehran_tz = timezone(tehran_offset)
    
    # گرفتن زمان حال UTC و تبدیل به تهران
    now_tehran = datetime.now(timezone.utc).astimezone(tehran_tz)
    return now_tehran.strftime('%Y-%m-%d %H:%M:%S')

def main():
    inserted_count = 0
    cnx = None
    cursor = None
    
    try:
        cnx = mysql.connector.connect(**db_config)
        cursor = cnx.cursor()
        # نمایش زمان اتصال هم به وقت تهران
        print(f"\n[{get_tehran_time()}] Successfully connected to 'signal_pool' database.")

        # --- ۲. اضافه کردن ستون signal_time به کوئری ---
        insert_query = """
            INSERT INTO signal_pool 
            (signal_time, pair, coin, signal_grade, profit_percent, strategy_name, exchange, entry_price, target_price) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        for url in API_URLS:
            try:
                print(f"\nFetching data from: {url}")
                response = requests.get(url, timeout=10)
                response.raise_for_status() 
                data = response.json()
                
                is_g1_api = "8888/g1/signals" in url
                
                if 'opportunities' in data and isinstance(data['opportunities'], list) and len(data['opportunities']) > 0:
                    
                    signals_to_insert = []
                    for signal in data['opportunities']:
                        
                        # محاسبه زمان تهران برای این سیگنال
                        tehran_now = get_tehran_time()

                        coin = signal.get('asset_name')
                        
                        # پاکسازی نام کوین
                        if is_g1_api and coin:
                            if coin.endswith("USDT"): coin = coin[:-4] 
                            elif coin.endswith("TMN"): coin = coin[:-3] 

                        strategy = signal.get('strategy_name')
                        exchange = signal.get('exchange_name')
                        entry_price = signal.get('entry_price')

                        # منطق PAIR
                        if is_g1_api:
                            pair = 'USDT'
                        elif strategy == 'Internal' or strategy == 'Computiational':
                            pair = 'TMN'
                        else:
                            pair = signal.get('pair', 'TMN')
                        
                        target_price = signal.get('exit_price') or signal.get('take_profit_price')
                        profit = signal.get('expected_profit_percentage') or signal.get('net_profit_percent')
                        
                        grade = get_signal_grade(profit)

                        # --- ۳. اضافه کردن tehran_now به داده‌های ارسالی ---
                        insert_data = (
                            tehran_now, pair, coin, grade, profit, strategy, 
                            exchange, entry_price, target_price
                        )
                        signals_to_insert.append(insert_data)
                    
                    if signals_to_insert:
                        cursor.executemany(insert_query, signals_to_insert)
                        cnx.commit() 
                        count = len(signals_to_insert)
                        inserted_count += count
                        print(f"  [SUCCESS] Inserted {count} signals from this API.")

                else:
                    print("  [INFO] No opportunities found in this response.")

            except requests.RequestException as e:
                print(f"  [ERROR] Error fetching API {url}: {e}")
            except json.JSONDecodeError:
                print(f"  [ERROR] Response was not valid JSON.")
            except Exception as e:
                print(f"  [ERROR] Error processing data from {url}: {e}")

    except Error as e:
        print(f"[DATABASE ERROR] {e}")
    
    finally:
        if cursor: cursor.close()
        if cnx and cnx.is_connected():
            cnx.close()
            print(f"  Database connection closed for this cycle.")
        
        print(f"\n--- Cycle Summary ---")
        print(f"Total Inserted Signals: {inserted_count}")


if __name__ == "__main__":
    print(f"--- Service Started (v4.0 - Tehran Time) ---")
    while True:
        try:
            main() 
            # محاسبه زمان انتظار برای چاپ در لاگ
            print(f"\n[{get_tehran_time()}] Cycle complete. Sleeping for 2 minutes...")
            time.sleep(120) 
        except KeyboardInterrupt:
            print("\nService stopped by user (Ctrl+C). Exiting.")
            break

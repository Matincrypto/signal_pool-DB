# importer.py (نسخه دائم با اجرای ۲ دقیقه‌ای)
#
# این اسکریپت به صورت دائم اجرا می‌شود، هر ۲ دقیقه یکبار
# به API ها وصل شده و تمام سیگنال‌های جدید را در دیتابیس ذخیره می‌کند.

import requests
import mysql.connector
from mysql.connector import Error
from config import db_config  # وارد کردن کانفیگ از فایل کناری
import time                   # <-- کتابخانه زمان برای ایجاد تاخیر اضافه شد

# لیست آدرس‌های API شما
API_URLS = [
    "http://103.75.198.172:5005/Internal/arbitrage",
    "http://api.zerotrade.xyz:8888/g1/signals"
]

def get_signal_grade(profit_percentage):
    """
    بر اساس درصد سود، گرید سیگنال (Q1 تا Q4) را برمی‌گرداند.
    """
    try:
        profit = float(profit_percentage) 
        
        if profit >= 7:
            return 'Q1'
        elif profit >= 5:
            return 'Q2'
        elif profit >= 3:
            return 'Q3'
        else:
            return 'Q4'
    except (ValueError, TypeError):
        return 'N/A' 

def main():
    """
    تابع اصلی برای اتصال به دیتابیس، دریافت API و درج سیگنال‌ها.
    (این تابع یک چرخه کامل ورود داده را انجام می‌دهد)
    """
    inserted_count = 0
    cnx = None
    cursor = None
    
    try:
        # اتصال به دیتابیس با استفاده از اطلاعات فایل کانفیگ
        cnx = mysql.connector.connect(**db_config)
        cursor = cnx.cursor()
        print(f"\n[{time.strftime('%Y-%m-%d %H:%M:%S')}] Successfully connected to 'signal_pool' database.")

        # دستور SQL برای درج داده‌ها
        insert_query = """
            INSERT INTO signal_pool 
            (pair, coin, signal_grade, strategy_name, exchange, entry_price, target_price) 
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        for url in API_URLS:
            try:
                print(f"  Fetching data from: {url}")
                response = requests.get(url, timeout=10)
                response.raise_for_status() 
                
                data = response.json()
                
                if 'opportunities' in data and data.get('opportunities_found', 0) > 0:
                    
                    signals_to_insert = []
                    
                    for signal in data['opportunities']:
                        pair = signal.get('pair')
                        coin = signal.get('asset_name')
                        strategy = signal.get('strategy_name')
                        exchange = signal.get('exchange_name')
                        entry_price = signal.get('entry_price')
                        target_price = signal.get('exit_price')
                        profit = signal.get('expected_profit_percentage')

                        grade = get_signal_grade(profit)

                        insert_data = (
                            pair, coin, grade, strategy, 
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
                    print(f"  No opportunities found in this API response.")

            except requests.RequestException as e:
                print(f"  [ERROR] Error fetching API {url}: {e}")
            except Exception as e:
                print(f"  [ERROR] Error processing data from {url}: {e}")

    except Error as e:
        print(f"[DATABASE ERROR] {e}")
        print("  Please check 'config.py' and MySQL server status.")
    
    finally:
        # بستن اتصالات در پایان هر چرخه
        if cursor:
            cursor.close()
        if cnx and cnx.is_connected():
            cnx.close()
            print(f"  Database connection closed for this cycle.")
        
        print(f"--- Cycle Summary ---")
        print(f"Total Inserted Signals: {inserted_count}")

# ----------------------------------------------------
# بخش اصلی اجرای دائم
# ----------------------------------------------------
if __name__ == "__main__":
    print("--- Service Started ---")
    print("Running initial import...")
    
    while True:
        try:
            # ۱. اجرای تابع اصلی
            main() 
            
            # ۲. نمایش پیام انتظار
            print(f"\n[{time.strftime('%Y-%m-%d %H:%M:%S')}] Cycle complete. Sleeping for 2 minutes (120 seconds)...")
            
            # ۳. تاخیر به مدت ۲ دقیقه
            time.sleep(120) 
        
        except KeyboardInterrupt:
            # این به شما اجازه می‌دهد با Ctrl+C اسکریپت را متوقف کنید
            print("\nService stopped by user (Ctrl+C). Exiting.")
            break
        except Exception as e:
            # در صورت بروز خطای پیش‌بینی نشده در حلقه اصلی
            print(f"[FATAL ERROR] An unexpected error occurred: {e}")
            print("Restarting loop after 2 minutes...")
            time.sleep(120)

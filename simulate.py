import psycopg2
from datetime import datetime, timedelta
from loguru import logger
from simulator import StockTradingSimulator
import os
from itertools import product
import itertools
from itertools import zip_longest
from concurrent.futures import ProcessPoolExecutor, as_completed
from statistic import *

file_name = os.path.splitext(os.path.basename(__file__))[0]
os.makedirs("log", exist_ok=True)
logger.add(sink=f"log/{file_name}.log", level="DEBUG", format="{message}")

def fetch_ticker_id_list(conn):
    with conn.cursor() as cursor:
        cursor.execute('SELECT ticker_id FROM ticker_code_mapping')
        return [row[0] for row in cursor.fetchall()]

def fetch_simulated_metrics_for_time(conn, current_time):
    cursor = conn.cursor()

    data_query = f"""
    SELECT 
        ticker_id, current_price, volume, ask_quantity_total, bid_quantity_total, ask_price_10, bid_price_1
    FROM real_time_sum_interval_{current_time.strftime('%Y%m%d')}
    WHERE "timestamp" = %s;
    """
    cursor.execute(data_query, (current_time.strftime('%Y-%m-%d %H:%M:%S+09'),))
    data = cursor.fetchall()

    simulated_metrics = {}

    for row in data:
        ticker_id = row[0]
        simulated_metrics[ticker_id] = {
            'current_price': row[1], 'volume': row[2],
            'ask_quantity_total': row[3], 'bid_quantity_total': row[4], 
            'ask_price_10': row[5], 'bid_price_1': row[6]
        }

    cursor.close()
    return simulated_metrics

def is_highest_price(conn, ticker_id, price, check_date, n_days, threshold):
    query = """
    WITH last_n_days_data AS (
        SELECT
            MAX("Close") AS max_price
        FROM public.historical_data
        WHERE
            ticker_id = %s
            AND date >= %s::timestamp - INTERVAL '1 day' * %s
            AND date <= %s::timestamp
    )
    SELECT %s >= last_n_days_data.max_price * (1 - %s) AS is_highest
    FROM last_n_days_data;
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query, (ticker_id, check_date, n_days, check_date, price, threshold))
            result = cur.fetchone()
            return result[0]
    except Exception as e:
        logger.info(f"Error while checking if the price is the highest: {e}")
        return True

def is_lowest_price(conn, ticker_id, price, check_date, n_days, threshold):
    query = """
    WITH last_n_days_data AS (
        SELECT
            MIN("Close") AS min_price
        FROM public.historical_data
        WHERE
            ticker_id = %s
            AND date >= %s::timestamp - INTERVAL '1 day' * %s
            AND date <= %s::timestamp
    )
    SELECT %s <= last_n_days_data.min_price * (1 + %s) AS is_lowest
    FROM last_n_days_data;
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query, (ticker_id, check_date, n_days, check_date, price, threshold))
            result = cur.fetchone()
            return result[0]
    except Exception as e:
        logger.info(f"Error while checking if the price is the lowest: {e}")
        return True

def execute_trades(conn, sim, cnt, long_list, short_list):
    if sim.current_time.hour >= 10:
        return
    long_candidates = [(ticker_id, sim.get_current_long_price(ticker_id)) for ticker_id in long_list]
    short_candidates = [(ticker_id, sim.get_current_short_price(ticker_id)) for ticker_id in short_list]

    preferred_long_candidates = sorted([item for item in long_candidates if item[1] > 300], key=lambda x: x[1], reverse=True)
    other_long_candidates = sorted([item for item in long_candidates if item[1] <= 300], key=lambda x: x[1], reverse=True)

    preferred_short_candidates = sorted([item for item in short_candidates if item[1] > 300], key=lambda x: x[1], reverse=True)
    other_short_candidates = sorted([item for item in short_candidates if item[1] <= 300], key=lambda x: x[1], reverse=True)

    long_candidates_combined = preferred_long_candidates + other_long_candidates
    short_candidates_combined = preferred_short_candidates + other_short_candidates

    for long_item, short_item in zip_longest(long_candidates_combined, short_candidates_combined):
        if long_item:
            ticker_id, long_price = long_item
            if is_highest_price(conn, ticker_id, long_price, sim.current_time.strftime('%Y-%m-%d'), sim.params[cnt]['n_days'], sim.params[cnt]['threshold']):
                sim.buy_stock(cnt, ticker_id, sim.params[cnt]['min_trade_qty'], long_price)

        if short_item:
            ticker_id, short_price = short_item
            if is_lowest_price(conn, ticker_id, short_price, sim.current_time.strftime('%Y-%m-%d'), sim.params[cnt]['n_days'], sim.params[cnt]['threshold']):
                sim.short_sell_stock(cnt, ticker_id, sim.params[cnt]['min_trade_qty'], short_price)

def monitor_and_trade(conn, sim, simulated_metrics, current_time):
    sim.set_metrics(simulated_metrics, current_time)

    for cnt, param in sim.params.items():
        if param['stop']:
            continue

        sim.update_trade_tickers(cnt, simulated_metrics)

        for pos in param['pfl']:
            sim.apply_stop_loss_and_take_profit(cnt, pos)
        
        buying_tickers, short_selling_tickers = sim.get_trade_tickers(cnt, simulated_metrics)
        
        execute_trades(conn, sim, cnt, buying_tickers, short_selling_tickers)
    
    if sim.single_mode:
        sim.show_pfl()
    sim.show_estimated_profit()

    return sim

def chunked_iterable(iterable, chunk_size):
    it = iter(iterable)
    while True:
        chunk = list(itertools.islice(it, chunk_size))
        if not chunk:
            break
        yield chunk

def create_simulators(all_combinations, single_mode, ticker_id_list):
    simulators = []
    chunk_size = max(1, len(all_combinations) // os.cpu_count() + 1)

    for chunk in chunked_iterable(all_combinations, chunk_size):
        sim = StockTradingSimulator(chunk, single_mode, ticker_id_list)
        simulators.append(sim)

    return simulators

def simulate(all_combinations, single_mode, proc_mode, year, month, date, interval=8):
    conn = psycopg2.connect(dbname='stock', user='stock', password='stock', host='localhost')

    start_time = datetime(year, month, date, 9, 1, 0) + timedelta(seconds=8 * (-7))
    # start_time = datetime(year, month, date, 12, 1, 0) + timedelta(seconds=8 * (-7))
    end_time = datetime(year, month, date, 14, 59, 0) + timedelta(seconds=8 * (-37))
    # end_time = datetime(year, month, date, 13, 59, 0) + timedelta(seconds=8 * (-37))

    current_time = start_time

    ticker_id_list = fetch_ticker_id_list(conn)

    simulators = create_simulators(all_combinations, single_mode, ticker_id_list)

    is_init = False
    while current_time <= end_time:
        if (current_time.hour == 11 and current_time.minute >= 30) or (current_time.hour == 12 and current_time.minute <= 30):
            current_time += timedelta(seconds=interval)
            for sim in simulators:
                for cnt, param in sim.params.items():
                    if param['stop'] == False:
                        sim.retreat(cnt)
            continue

        if single_mode:
            logger.info(current_time)
        else:
            print(current_time)

        simulated_metrics = fetch_simulated_metrics_for_time(conn, current_time)
        if is_init == False:
            for sim in simulators:
                for cnt, param in sim.params.items():
                    sim.init_trade_tickers(cnt)
            is_init = True

        if proc_mode == False:
            for sim in simulators:
                sim = monitor_and_trade(conn, sim, simulated_metrics, current_time)
        else:
            with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
                futures = [
                    executor.submit(monitor_and_trade, conn, sim, simulated_metrics, current_time)
                    for sim in simulators
                ]

                for i, future in enumerate(as_completed(futures)):
                    simulators[i] = future.result()

        current_time += timedelta(seconds=interval)

    for sim in simulators:
        sim.show_sorted_profit(start_time.strftime('%Y-%m-%d'))

        if single_mode:
            for cnt, param in sim.params.items():
                transaction_summary = {}
                for transaction in param['transactions']:
                    transaction_summary[transaction['transaction_type']] = transaction_summary.get(transaction['transaction_type'], 0) + 1

                for transaction_type, count in transaction_summary.items():
                    sim.log(f"{transaction_type}: {count}")

    conn.close()

def read_data_from_file(file_path):
    date_combinations = []

    with open(file_path, 'r') as file:
        for line in file:
            key, value = line.strip().split(': ', 1)
            if key == 'date_combinations':
                date_combinations = [tuple(map(int, x.strip('()').split(', '))) for x in value.split('), ')]

    return date_combinations

if __name__ == "__main__":
    # date_combinations = read_data_from_file(r"C:\stock\sim_set.txt")
    # date_combinations = read_data_from_file("sim_set.txt")
    # nm = range(12, 35)
    # min_up_down_diff = range(4, 20)
    root = [x * 0.05 for x in range(20, 40)]
    take = range(5, 15, 2)
    time = range(65, 67, 2)
    #time = range(1200, 1201)
    min_trade_qty = range(300, 400, 100)
    thr1 = range(6, 7, 1)
    thr2 = range(1, 2, 1)
    n_days = range(78, 80, 2)
    threshold = [0.007]
    max_decrements = range(5, 6)
    min_decrements = range(2, 3)
    trade_gain_len = range(5, 6)
    min_up_down_diff = range(2002, 2003)
    # date_combinations = [(2024, 9, 26), (2024, 9, 27), (2024, 9, 30), (2024, 10, 1), (2024, 10, 2), (2024, 10, 3), (2024, 10, 4),
    #                      (2024, 10, 7), (2024, 10, 8), (2024, 10, 9), (2024, 10, 10), (2024, 10, 11), (2024, 10, 15), (2024, 10, 16),
    #                      (2024, 10, 29), (2024, 10, 30), (2024, 10, 31), (2024, 11, 1), (2024, 11, 5)]
    date_combinations = [(2024, 10, 17), (2024, 10, 18), (2024, 10, 21), (2024, 10, 22), (2024, 10, 23), (2024, 10, 24), (2024, 10, 25), (2024, 10, 28),
                         (2024, 11, 6), (2024, 11, 8), (2024, 11, 11), (2024, 11, 12), (2024, 11, 13), (2024, 11, 14), (2024, 11, 15)]
    # date_combinations = [(2024, 9, 26), (2024, 9, 27), (2024, 9, 30), (2024, 10, 1), (2024, 10, 2)]
    # date_combinations = [(2024, 10, 17), (2024, 10, 18), (2024, 10, 21), (2024, 10, 22), (2024, 10, 23)]
    # date_combinations = [(2024, 10, 17)]

    all_combinations = list(product(
        root,
        take,
        time,
        n_days,
        threshold,
        min_trade_qty,
        max_decrements,
        min_decrements,
        trade_gain_len,
        min_up_down_diff
    ))

    single_mode = len(date_combinations) <= 1 and len(all_combinations) <= 1

    for year, month, date in date_combinations:
        simulate(all_combinations, single_mode, False, year, month, date, 8)
    
    if single_mode:
        convert_single()

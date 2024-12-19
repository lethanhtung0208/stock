from loguru import logger
from datetime import datetime, timedelta
import os
import numpy as np

file_name = os.path.splitext(os.path.basename(__file__))[0]
os.makedirs("log", exist_ok=True)
logger.add(sink=f"log/{file_name}.log", level="DEBUG", format="{message}")

class StockTradingSimulator:
    def __init__(self, all_combinations, single_mode, ticker_id_list):
        self.fee_percentage = 0.003
        self.tax_rate = 0.2
        self.current_metrics = {}
        self.last_values = {
            'current_prices': {},
            'volume': {},
            'ask_prices': {},
            'bid_prices': {}
        }
        self.max_ask_bid_price_diff = 0.00036
        self.max_price = 15000
        self.gain_len = 3
        self.loss_len = 7
        self.stop_loss_thres = 0.04
        self.take_profit_thres = 0.09
        self.min_volume = 64000
        self.trade_qty = 2500
        self.profit_levels = [0.01, 0.03, 0.05, 0.07, 0.09, 0.11]
        self.params = {}
        self.ticker_id_list = ticker_id_list
        cnt = 0
        for root, take, time, n_days, threshold, min_trade_qty, max_decrements, min_decrements, trade_gain_len, min_up_down_diff in all_combinations:
            self.params[cnt] = {
                'pfl': [],
                'transactions': [],
                'balance': 3500000,
                'init_balance': 3500000,
                'max_profit': {'time': None, 'value': -10000000},
                'real_max_profit': {'time': None, 'value': -10000000},
                'profit': 0,
                'real_profit': 0,
                'ticker_len': 9,
                'trend_data': {},
                'price_history': {},
                'min_diff': 0.0144,
                'max_diff': 0.0225,
                'min_price_diff': 0.00016,
                'max_price_diff': 0.00049,
                'min_trade_price_diff': 0.00009,
                'min_stop_price_diff': 0.00009,
                'min': -9000,
                'take': 4000,
                'take1': take,
                'time': time,
                'root': root,
                'n_days': n_days,
                'threshold': threshold,
                'min_trade_qty': min_trade_qty,
                # 'thr1': thr1,
                # 'thr2': thr2,
                'stop': False,
                'step_thresholds': [(0, 130)],
                # 'step_thresholds': [],
                'profit_thresholds': [],
                'max_decrements': min_decrements + max_decrements,
                'min_decrements': min_decrements,
                'trade_gain_len': trade_gain_len,
                'min_up_down_diff': min_up_down_diff
            }
            #print(self.params[cnt]['step_thresholds'])
            cnt += 1
        
        self.single_mode = single_mode
    
    def log(self, message):
        if self.single_mode:
            logger.info(message)

    def set_metrics(self, simulated_metrics, current_time):
        self.current_time = current_time
        self.current_metrics.clear()
        for ticker_id, metrics in simulated_metrics.items():
            self.current_metrics[ticker_id] = metrics
            current_price = self.get_current_price(ticker_id)
            short_price = self.get_current_short_price(ticker_id)
            long_price = self.get_current_long_price(ticker_id)
            volume = self.get_current_volume(ticker_id)

    def get_metric(self, ticker_id, metric_name):
        return self.current_metrics[ticker_id].get(metric_name, 0)

    def get_current_price(self, ticker_id):
        current_price = self.get_metric(ticker_id, 'current_price')
        if current_price is not None and current_price > 0.001:
            self.last_values['current_prices'][ticker_id] = current_price
        else:
            if ticker_id in self.last_values['current_prices']:
                current_price = self.last_values['current_prices'][ticker_id]
            else:
                if ticker_id in self.last_values['ask_prices'] and ticker_id in self.last_values['bid_prices']:
                    current_price = (self.last_values['bid_prices'][ticker_id] + self.last_values['ask_prices'][ticker_id]) / 2
                else:
                    current_price = None
        return current_price

    def get_current_volume(self, ticker_id):
        volume = self.get_metric(ticker_id, 'volume')
        if volume is not None and volume > 0:
            self.last_values['volume'][ticker_id] = volume
        else:
            if ticker_id in self.last_values['volume']:
                volume = self.last_values['volume'][ticker_id]
            else:
                volume = 1000000000
        return volume

    def get_current_short_price(self, ticker_id):
        bid_price = self.get_metric(ticker_id, 'bid_price_1')
        if bid_price > 0.001:
            self.last_values['bid_prices'][ticker_id] = bid_price
        else:
            if ticker_id in self.last_values['bid_prices']:
                bid_price = self.last_values['bid_prices'][ticker_id]
            else:
                if ticker_id in self.last_values['current_prices']:
                    bid_price = self.last_values['current_prices'][ticker_id] * 0.999
                else:
                    bid_price = -1
        return bid_price

    def get_current_long_price(self, ticker_id):
        ask_price = self.get_metric(ticker_id, 'ask_price_10')
        if ask_price > 0.001:
            self.last_values['ask_prices'][ticker_id] = ask_price
        else:
            if ticker_id in self.last_values['ask_prices']:
                ask_price = self.last_values['ask_prices'][ticker_id]
            else:
                if ticker_id in self.last_values['current_prices']:
                    ask_price = self.last_values['current_prices'][ticker_id] * 1.001
                else:
                    ask_price = 1000000000
        return ask_price

    def buy_stock(self, cnt, ticker_id, qty, price=None):
        price = self.get_current_long_price(ticker_id) if price is None else price

        cost = price * qty
        fee = cost * (self.fee_percentage / 100)
        total_cost = cost + fee

        if self.params[cnt]['balance'] < total_cost and self.calculate_margin_capacity(cnt) < total_cost:
            return
        
        if self.params[cnt]['balance'] >= total_cost:
            self.params[cnt]['balance'] -= total_cost
            is_margin = False
        else:
            is_margin = True
        
        self.log(f"buy_stock: {ticker_id}, quantity: {qty}")
        existing_entry = next((entry for entry in self.params[cnt]['pfl'] if entry['ticker_id'] == ticker_id), None)

        if existing_entry:
            existing_entry['prices'].append({'price': price, 'qty': qty, 'margin': is_margin})
        else:
            self.params[cnt]['pfl'].append({
                'ticker_id': ticker_id,
                'prices': [{'price': price, 'qty': qty, 'margin': is_margin}],
                'pos_type': 'LONG',
                'max_price': price,
                'min_from_max_price': price,
                'min_price': price,
                'down_step': 0,
                'down_from_max_step': 0,
                'up_step': 0,
                'time': self.current_time
            })
        
        if self.single_mode:
            self.params[cnt]['transactions'].append({'ticker_id': ticker_id, 'price': price, 'transaction_type': 'LONG'})

    def sell_stock(self, cnt, ticker_id, qty=0, price=None, price_qty_pair=None):
        price = self.get_current_short_price(ticker_id) if price is None else price
        entry = next((e for e in self.params[cnt]['pfl'] if e['ticker_id'] == ticker_id and e['pos_type'] == 'LONG'), None)
        if not entry:
            return
        
        if qty == 0:
            qty = sum(pair['qty'] for pair in entry['prices'])

        total_revenue = 0
        total_tax = 0
        cost = 0
        remaining_qty = qty

        price_qty_pairs = [price_qty_pair] if price_qty_pair else entry['prices']

        to_remove = []
        for pair in price_qty_pairs:
            available_qty = pair['qty']

            if remaining_qty <= 0:
                break

            sell_qty = min(remaining_qty, available_qty)
            trade_revenue = sell_qty * price
            if pair['margin']:
                cost += sell_qty * pair['price']
            trade_tax = max((price - pair['price']) * sell_qty * self.tax_rate, 0)

            total_revenue += trade_revenue
            total_tax += trade_tax

            pair['qty'] -= sell_qty
            if pair['qty'] == 0:
                to_remove.append(pair)

            remaining_qty -= sell_qty

        for pair in to_remove:
            entry['prices'].remove(pair)

        net_revenue = total_revenue - total_tax
        self.log(f"sell_stock: {ticker_id}, Revenue: {net_revenue}, Tax: {total_tax}")

        self.params[cnt]['balance'] += net_revenue
        self.params[cnt]['balance'] -= cost
        
        if not entry['prices']:
            self.params[cnt]['pfl'].remove(entry)

        if self.single_mode:
            self.params[cnt]['transactions'].append({'ticker_id': ticker_id, 'price': price, 'transaction_type': 'SELL'})

    def short_sell_stock(self, cnt, ticker_id, qty, price=None):
        price = self.get_current_short_price(ticker_id) if price is None else price
        
        potential_revenue = price * qty
        if potential_revenue > self.calculate_margin_capacity(cnt):
            return

        self.log(f"short_sell_stock: {ticker_id}, quantity: {qty}")
        existing_entry = next((entry for entry in self.params[cnt]['pfl'] if entry['ticker_id'] == ticker_id and entry['pos_type'] == 'SHORT'), None)

        if existing_entry:
            existing_entry['prices'].append({'price': price, 'qty': qty, 'margin': True})
        else:
            self.params[cnt]['pfl'].append({
                'ticker_id': ticker_id,
                'prices': [{'price': price, 'qty': qty, 'margin': True}],
                'pos_type': 'SHORT',
                'max_price': price,
                'max_from_min_price': price,
                'min_price': price,
                'down_step': 0,
                'down_from_min_step': 0,
                'up_step': 0,
                'time': self.current_time
            })

        if self.single_mode:
            self.params[cnt]['transactions'].append({'ticker_id': ticker_id, 'price': price, 'transaction_type': 'SHORT'})

    def cover_short(self, cnt, ticker_id, qty=0, price=None, price_qty_pair=None):
        price = self.get_current_long_price(ticker_id) if price is None else price
        entry = next((e for e in self.params[cnt]['pfl'] if e['ticker_id'] == ticker_id and e['pos_type'] == 'SHORT'), None)
        if not entry:
            return
        
        if qty == 0:
            qty = sum(pair['qty'] for pair in entry['prices'])

        total_cost = 0
        total_revenue = 0
        total_tax = 0
        remaining_qty = qty

        price_qty_pairs = [price_qty_pair] if price_qty_pair else entry['prices']

        to_remove = []
        for pair in price_qty_pairs:
            available_qty = pair['qty']
            
            if remaining_qty <= 0:
                break

            cover_qty = min(remaining_qty, available_qty)
            cost = cover_qty * price
            revenue = cover_qty * (pair['price'] - price)
            tax = max((pair['price'] - price) * cover_qty * self.tax_rate, 0)
            
            total_cost += cost
            total_revenue += revenue
            total_tax += tax

            pair['qty'] -= cover_qty
            if pair['qty'] == 0:
                to_remove.append(pair)

            remaining_qty -= cover_qty

        for pair in to_remove:
            entry['prices'].remove(pair)
        
        fee = total_cost * (self.fee_percentage / 100)
        net_revenue = total_revenue - total_tax - fee
        self.params[cnt]['balance'] += net_revenue
        self.log(f"cover_short: {ticker_id}, Net Revenue: {net_revenue}")

        if not entry['prices']:
            self.params[cnt]['pfl'].remove(entry)

        if self.single_mode:
            self.params[cnt]['transactions'].append({'ticker_id': ticker_id, 'price': price, 'transaction_type': 'COVER'})

    def calculate_margin_capacity(self, cnt):
        total_pfl_value = self.pfl_value(cnt)
        total_financial_pos = self.params[cnt]['balance'] + total_pfl_value

        total_margin = 0
        for entry in self.params[cnt]['pfl']:
            for price_qty_pair in entry['prices']:
                if price_qty_pair['margin']:
                    total_margin += price_qty_pair['qty'] * price_qty_pair['price']
        
        remaining_capacity = 3 * total_financial_pos - total_margin
        return max(remaining_capacity, 0)
    
    def init_trade_tickers(self, cnt):
        for ticker_id in self.ticker_id_list:
            self.params[cnt]['trend_data'][ticker_id] = {
                'last_high_total_ask_qty': -1,
                'last_high_bid_price': -1,
                'last_low_total_bid_qty': 1000000000,
                'last_high_total_bid_qty': -1,
                'last_low_ask_price': 1000000000,
                'last_low_total_ask_qty': 1000000000,
                'num_of_ask_qty_inc': 0,
                'num_of_bid_qty_inc': 0
            }
    
    def update_trade_tickers(self, cnt, simulated_metrics):
        for ticker_id, metrics in simulated_metrics.items():
            total_ask_quantity = metrics['ask_quantity_total']
            total_bid_quantity = metrics['bid_quantity_total']
            ask_price = self.get_current_long_price(ticker_id)
            bid_price = self.get_current_short_price(ticker_id)
            
            trend_info = self.params[cnt]['trend_data'][ticker_id]
            min_diff = self.params[cnt]['min_diff']
            max_diff = self.params[cnt]['max_diff']
            min_price_diff = self.params[cnt]['min_price_diff']
            max_price_diff = self.params[cnt]['max_price_diff']

            if (total_ask_quantity > trend_info['last_high_total_ask_qty'] * (1 + min_diff) and 
                total_bid_quantity < trend_info['last_low_total_bid_qty'] * (1 - min_diff) and
                ask_price < trend_info['last_low_ask_price'] * (1 - min_price_diff)):

                trend_info['last_high_total_ask_qty'] = total_ask_quantity
                trend_info['last_low_ask_price'] = ask_price
                trend_info['last_low_total_bid_qty'] = total_bid_quantity
                trend_info['num_of_ask_qty_inc'] = trend_info['num_of_ask_qty_inc'] + 1

            elif (total_ask_quantity < trend_info['last_high_total_ask_qty'] * (1 - min_diff) or
                 total_ask_quantity > trend_info['last_high_total_ask_qty'] * (1 + max_diff) or
                 total_bid_quantity > trend_info['last_low_total_bid_qty'] * (1 + min_diff) or
                 total_bid_quantity < trend_info['last_low_total_bid_qty'] * (1 - max_diff) or
                 ask_price > trend_info['last_low_ask_price'] * (1 + min_price_diff) or
                 ask_price < trend_info['last_low_ask_price'] * (1 - max_price_diff)):

                trend_info['last_high_total_ask_qty'] = -1
                trend_info['last_low_ask_price'] = 1000000000
                trend_info['last_low_total_bid_qty'] = 1000000000
                trend_info['num_of_ask_qty_inc'] = 0

            if (total_bid_quantity > trend_info['last_high_total_bid_qty'] * (1 + min_diff) and
                total_ask_quantity < trend_info['last_low_total_ask_qty'] * (1 - min_diff) and
                bid_price > trend_info['last_high_bid_price'] * (1 + min_price_diff)):

                trend_info['last_high_total_bid_qty'] = total_bid_quantity
                trend_info['last_high_bid_price'] = bid_price
                trend_info['last_low_total_ask_qty'] = total_ask_quantity
                trend_info['num_of_bid_qty_inc'] = trend_info['num_of_bid_qty_inc'] + 1
            
            elif (total_bid_quantity < trend_info['last_high_total_bid_qty'] * (1 - min_diff) or
                 total_bid_quantity > trend_info['last_high_total_bid_qty'] * (1 + max_diff) or
                 total_ask_quantity > trend_info['last_low_total_ask_qty'] * (1 + min_diff) or
                 total_ask_quantity < trend_info['last_low_total_ask_qty'] * (1 - max_diff) or
                 bid_price < trend_info['last_high_bid_price'] * (1 - min_diff) or
                 bid_price > trend_info['last_high_bid_price'] * (1 + max_price_diff)):

                trend_info['last_high_total_bid_qty'] = -1
                trend_info['last_high_bid_price'] = -1
                trend_info['last_low_total_ask_qty'] = 1000000000
                trend_info['num_of_bid_qty_inc'] = 0

    def get_candidate_tickers(self, cnt, simulated_metrics):
        buying_tickers = []
        selling_tickers = []

        pfl_ticker_ids = {entry['ticker_id'] for entry in self.params[cnt]['pfl']}
        ticker_ids_in_price_history = list(self.params[cnt]['price_history'].keys())
        ticker_ids = [ticker_id for ticker_id in simulated_metrics.keys() 
                      if ticker_id not in pfl_ticker_ids and ticker_id not in ticker_ids_in_price_history]
        
        for ticker_id in ticker_ids:
            trend_info = self.params[cnt]['trend_data'][ticker_id]
            price = self.get_current_price(ticker_id)
            
            volume = self.get_current_volume(ticker_id)
            gain_len = self.gain_len
            if (trend_info['num_of_ask_qty_inc'] == gain_len and 
                price < self.max_price and
                volume > self.min_volume):

                selling_tickers.append(ticker_id)

            if (trend_info['num_of_bid_qty_inc'] == gain_len and 
                price < self.max_price and
                volume > self.min_volume):
                
                buying_tickers.append(ticker_id)
        
        return buying_tickers, selling_tickers
    
    def get_trade_tickers(self, cnt, simulated_metrics):
        buying_cad_tickers, selling_cad_tickers = self.get_candidate_tickers(cnt, simulated_metrics)
        min_price_diff = self.params[cnt]['min_trade_price_diff']
        min_decrements = self.params[cnt]['min_decrements']
        max_decrements = self.params[cnt]['max_decrements']
        gain_len = self.params[cnt]['trade_gain_len']

        buying_tickers = []
        selling_tickers = []

        for ticker_id in buying_cad_tickers + selling_cad_tickers:
            if ticker_id not in self.params[cnt]['price_history']:
                ask_price = self.get_current_long_price(ticker_id)
                bid_price = self.get_current_short_price(ticker_id)
                if ticker_id in selling_cad_tickers:
                    trade_type = 'short'
                else:
                    trade_type = 'long'

                self.params[cnt]['price_history'][ticker_id] = {
                    'ask_high_price': ask_price, 'bid_low_price': bid_price, 'trade_type': trade_type, 
                    'ask_increments': 0, 'bid_increments': 0, 'ask_decrements': 0, 'bid_decrements': 0}
        
        ticker_ids_in_price_history = list(self.params[cnt]['price_history'].keys())
        for ticker_id in ticker_ids_in_price_history:
            ask_price = self.get_current_long_price(ticker_id)
            bid_price = self.get_current_short_price(ticker_id)
            price_diff = ask_price - bid_price

            trend_info = self.params[cnt]['trend_data'][ticker_id]
            price_hist = self.params[cnt]['price_history'][ticker_id]
            
            if price_hist['trade_type'] == 'short':
                if bid_price < price_hist['bid_low_price'] * (1 - min_price_diff):
                    price_hist['bid_increments'] += 1
                    price_hist['bid_low_price'] = bid_price
                elif bid_price > price_hist['bid_low_price'] * (1 + min_price_diff):
                    price_hist['bid_decrements'] += 1
                    if price_hist['bid_decrements'] >= max_decrements:
                        del self.params[cnt]['price_history'][ticker_id]
                
                if price_hist['bid_increments'] >= gain_len and price_hist['bid_decrements'] >= min_decrements:
                    if price_diff < self.max_ask_bid_price_diff * ask_price and price_diff > 0.001:
                        selling_tickers.append(ticker_id)

                        try:
                            del self.params[cnt]['price_history'][ticker_id]
                        except KeyError:
                            pass
                        trend_info['last_high_total_ask_qty'] = -1
                        trend_info['last_high_bid_price'] = -1
                        trend_info['last_low_total_bid_qty'] = 1000000000
                        trend_info['num_of_ask_qty_inc'] = 0

            if price_hist['trade_type'] == 'long':
                if ask_price > price_hist['ask_high_price'] * (1 + min_price_diff):
                    price_hist['ask_increments'] += 1
                    price_hist['ask_high_price'] = ask_price
                elif ask_price < price_hist['ask_high_price'] * (1 - min_price_diff):
                    price_hist['ask_decrements'] += 1
                    if price_hist['ask_decrements'] >= max_decrements:
                        del self.params[cnt]['price_history'][ticker_id]
                
                if price_hist['ask_increments'] >= gain_len and price_hist['ask_decrements'] >= min_decrements:
                    if price_diff < self.max_ask_bid_price_diff * ask_price and price_diff > 0.001:
                        buying_tickers.append(ticker_id)

                        try:
                            del self.params[cnt]['price_history'][ticker_id]
                        except KeyError:
                            pass
                        trend_info['last_high_total_bid_qty'] = -1
                        trend_info['last_low_ask_price'] = 1000000000
                        trend_info['last_low_total_ask_qty'] = 1000000000
                        trend_info['num_of_bid_qty_inc'] = 0

        # return buying_tickers, selling_tickers
        return selling_tickers, buying_tickers

    def pfl_value(self, cnt):
        total_value = 0
        for entry in self.params[cnt]['pfl']:
            for price_qty_pair in entry['prices']:
                if not price_qty_pair['margin']:
                    total_value += price_qty_pair['qty'] * self.get_current_price(entry['ticker_id'])
        return total_value

    def show_pfl(self):
        def format_pos(entry, price_qty_pair, target_price, pos_type):
            col_name = 'down_from_max_step' if pos_type  == 'LONG' else 'down_from_min_step'
            return (f"({entry['ticker_id']}, Price: {price_qty_pair['price']}, "
                    f"Current Price: {target_price}, Down Step: {entry['down_step']}, Down From Step: {entry[col_name]}, "
                    f"Up Step: {entry['up_step']}, Quantity: {price_qty_pair['qty']})")

        for cnt, param in self.params.items():
            long_poss = []
            short_poss = []
            for entry in self.params[cnt]['pfl']:
                for price_qty_pair in entry.get('prices', []):
                    if entry['pos_type'] == 'LONG':
                        target_price = self.get_current_short_price(entry['ticker_id'])
                        formatted_position = format_pos(entry, price_qty_pair, target_price, entry['pos_type'])
                        long_poss.append(formatted_position)
                    elif entry['pos_type'] == 'SHORT':
                        target_price = self.get_current_long_price(entry['ticker_id'])
                        formatted_position = format_pos(entry, price_qty_pair, target_price, entry['pos_type'])
                        short_poss.append(formatted_position)

            if long_poss:
                self.log("LONG Positions:")
                for pos in long_poss:
                    self.log(pos)
            if short_poss:
                self.log("SHORT Positions:")
                for pos in short_poss:
                    self.log(pos)
            self.log(f"Balance: {self.params[cnt]['balance']:,.2f}")
    
    def retreat(self, cnt, stop=False):
        long_poss = [entry for entry in self.params[cnt]['pfl'] if entry['pos_type'] == 'LONG']
        for entry in long_poss:
            self.sell_stock(cnt, entry['ticker_id'])
        
        short_poss = [entry for entry in self.params[cnt]['pfl'] if entry['pos_type'] == 'SHORT']
        for entry in short_poss:
            self.cover_short(cnt, entry['ticker_id'])
        
        self.params[cnt]['stop'] = stop
    
    def apply_stop_loss_and_take_profit(self, cnt, pos):
        current_price = self.get_current_short_price(pos['ticker_id']) if pos['pos_type'] == 'LONG' else self.get_current_long_price(pos['ticker_id'])
        volume = self.get_current_volume(pos['ticker_id'])
        root = self.params[cnt]['take1'] if volume > 30000 else self.params[cnt]['take1']
        root_volume = volume ** (1 / root)
        min_price_diff = self.params[cnt]['min_stop_price_diff']
        price_diff_unit = self.params[cnt]['min_stop_price_diff'] * 500

        profit = 0
        for price_qty_pair in pos['prices']:
            profit += self.calculate_profit(pos['pos_type'], current_price, price_qty_pair['price'], price_qty_pair['qty'], price_qty_pair['margin'])

        if pos['pos_type'] == 'LONG':
            if current_price < pos['min_price'] * (1 - min_price_diff):
                pos['down_step'] += max(1, int((pos['min_price'] - current_price) / pos['min_price'] / price_diff_unit) if pos['min_price'] > 1 else 0)
                if pos['down_step'] - pos['up_step'] > self.params[cnt]['min_up_down_diff']:
                    self.sell_stock(cnt, pos['ticker_id'], 0, price=current_price)
            elif current_price > pos['max_price'] * (1 + min_price_diff):
                pos['up_step'] += max(1, int((current_price - pos['max_price']) / pos['max_price'] / price_diff_unit) if pos['max_price'] > 1 else 0)
                pos['down_from_max_step'] = 0
                pos['min_from_max_price'] = current_price
                pos['time'] = self.current_time
                if pos['up_step'] < 6:
                    self.buy_stock(cnt, pos['ticker_id'], self.params[cnt]['min_trade_qty'], current_price)
            
            if current_price < pos['min_from_max_price'] * (1 - min_price_diff):
                pos['down_from_max_step'] += max(1, int((pos['min_from_max_price'] - current_price) / pos['min_from_max_price'] / price_diff_unit) if pos['min_from_max_price'] > 1 else 0)
                pos['min_from_max_price'] = current_price
                for up_limit, down_limit in self.params[cnt]['step_thresholds']:
                    if pos['up_step'] >= up_limit:
                        if pos['down_from_max_step'] >= down_limit:
                            self.sell_stock(cnt, pos['ticker_id'], 0, price=current_price)
                        break
            
            step = pos['up_step'] if profit > -100000000000 else max(pos['down_from_max_step'], pos['down_step'])
            step = step ** self.params[cnt]['root']
            if self.current_time - pos['time'] > timedelta(seconds=(self.params[cnt]['time'] - step * root_volume) * 60):
                self.sell_stock(cnt, pos['ticker_id'], 0, price=current_price)

            pos['min_price'] = min(pos['min_price'], current_price)
            pos['max_price'] = max(pos['max_price'], current_price)
        
        elif pos['pos_type'] == 'SHORT':
            if current_price > pos['max_price'] * (1 + min_price_diff):
                pos['down_step'] += max(1, int((current_price - pos['max_price']) / pos['max_price'] / price_diff_unit) if pos['max_price'] > 1 else 0)
                if pos['down_step'] - pos['up_step'] > self.params[cnt]['min_up_down_diff']:
                    self.cover_short(cnt, pos['ticker_id'], 0, price=current_price)
            elif current_price < pos['min_price'] * (1 - min_price_diff):
                pos['up_step'] += max(1, int((pos['min_price'] - current_price) / pos['min_price'] / price_diff_unit))
                pos['down_from_min_step'] = 0
                pos['max_from_min_price'] = current_price
                pos['time'] = self.current_time
                if pos['up_step'] < 6:
                    self.short_sell_stock(cnt, pos['ticker_id'], self.params[cnt]['min_trade_qty'], current_price)
            
            if current_price > pos['max_from_min_price'] * (1 + min_price_diff):
                pos['down_from_min_step'] += max(1, int((current_price - pos['max_from_min_price']) / pos['max_from_min_price'] / price_diff_unit) if pos['max_from_min_price'] > 1 else 0)
                pos['max_from_min_price'] = current_price
                for up_limit, down_limit in self.params[cnt]['step_thresholds']:
                    if pos['up_step'] >= up_limit:
                        if pos['down_from_min_step'] >= down_limit:
                            self.cover_short(cnt, pos['ticker_id'], 0, price=current_price)
                        break
            
            step = pos['up_step'] if profit > -100000000000 else max(pos['down_from_min_step'], pos['down_step'], pos['up_step'])
            step = step ** self.params[cnt]['root']
            if self.current_time - pos['time'] > timedelta(seconds=(self.params[cnt]['time'] - step * root_volume) * 60):
                self.cover_short(cnt, pos['ticker_id'], 0, price=current_price)
            
            pos['min_price'] = min(pos['min_price'], current_price)
            pos['max_price'] = max(pos['max_price'], current_price)

        for price_qty_pair in pos['prices']:
            change_diff_rate = abs(current_price - price_qty_pair['price']) / price_qty_pair['price']
            
            if change_diff_rate > self.take_profit_thres or change_diff_rate < -self.stop_loss_thres:
                qty = price_qty_pair['qty']
                
                if pos['pos_type'] == 'LONG':
                    self.sell_stock(cnt, pos['ticker_id'], qty, price=current_price, price_qty_pair=price_qty_pair)
                elif pos['pos_type'] == 'SHORT':
                    self.cover_short(cnt, pos['ticker_id'], qty, price=current_price, price_qty_pair=price_qty_pair)

        param = self.params[cnt]
        if pos['pos_type'] == 'LONG' and param['trend_data'][pos['ticker_id']]['num_of_ask_qty_inc'] >= self.loss_len:
            self.sell_stock(cnt, pos['ticker_id'], 0, current_price)
        elif pos['pos_type'] == 'SHORT' and param['trend_data'][pos['ticker_id']]['num_of_bid_qty_inc'] >= self.loss_len:
            self.cover_short(cnt, pos['ticker_id'], 0, current_price)
    
    def calculate_profit(self, pos_type, current_price, pos_price, trade_qty, margin):
        price_diff = (current_price - pos_price) if pos_type == 'LONG' else (pos_price - current_price)
        profit = price_diff * trade_qty - max(price_diff * trade_qty * self.tax_rate, 0)
        if margin:
            profit -= current_price * trade_qty * (self.fee_percentage / 100)
        return profit

    def show_estimated_profit(self, reset=False):
        for cnt, param in self.params.items():
            est_balance = self.params[cnt]['balance']

            for entry in self.params[cnt]['pfl']:
                profit = 0
                if entry['pos_type'] == 'LONG':
                    short_price = self.get_current_short_price(entry['ticker_id'])
                    for price_qty_pair in entry['prices']:
                        profit = self.calculate_profit(entry['pos_type'], short_price, price_qty_pair['price'], price_qty_pair['qty'], price_qty_pair['margin'])
                        est_balance += profit
                        if price_qty_pair['margin'] == False:
                            est_balance += price_qty_pair['price'] * price_qty_pair['qty']
                    
                elif entry['pos_type'] == 'SHORT':
                    long_price = self.get_current_long_price(entry['ticker_id'])
                    for price_qty_pair in entry['prices']:
                        profit = self.calculate_profit(entry['pos_type'], long_price, price_qty_pair['price'], price_qty_pair['qty'], price_qty_pair['margin'])
                        est_balance += profit

            profit = est_balance - self.params[cnt]['init_balance']

            if profit > self.params[cnt]['max_profit']['value']:
                self.params[cnt]['max_profit'] = {'time': self.current_time, 'value': profit}
            self.params[cnt]['profit'] = profit

            real_profit = est_balance - 3500000
            
            if real_profit > self.params[cnt]['real_max_profit']['value']:
                self.params[cnt]['real_max_profit'] = {'time': self.current_time, 'value': real_profit}
            self.params[cnt]['real_profit'] = real_profit

            if reset:
                self.params[cnt]['init_balance'] = est_balance
                self.params[cnt]['max_profit'] = {'time': None, 'value': -10000000}
            
            self.log(f"Profit: {profit:,.0f}")
            self.log(f"Real profit: {real_profit:,.0f}")

            if profit <= self.params[cnt]['min'] * 1000 or profit >= self.params[cnt]['take'] * 1000:
                self.retreat(cnt, True)  
    
    def show_sorted_profit(self, time_max):
        sorted_params = sorted(
            self.params.items(),
            key=lambda item: item[1]['real_max_profit']['value'],
            reverse=True
        )

        for counter, param in sorted_params:
            root = param['root']
            take = param['take1']
            time = param['time']
            max_decrements = param['max_decrements']
            min_decrements = param['min_decrements']
            trade_gain_len = param['trade_gain_len']
            min_trade_qty = param['min_trade_qty']
            max_profit_value = param['real_max_profit']['value']
            max_profit_time = param['real_max_profit']['time']
            profit = param['real_profit']

            if self.single_mode:
                logger.info(f"root: {root}, take: {take}, min_trade_qty: {min_trade_qty}, min_decrements: {min_decrements}, max_decrements: {max_decrements}, trade_gain_len: {trade_gain_len}, time: {time}, time_max: {time_max}, profit: {profit:,.0f}, max_profit: {max_profit_value:,.0f}")
            else:
                if max_profit_value > -2000000 and profit > -20000000:
                    logger.info(f"root: {root}, take: {take}, min_trade_qty: {min_trade_qty}, min_decrements: {min_decrements}, max_decrements: {max_decrements}, trade_gain_len: {trade_gain_len}, time: {time}, time_max: {time_max}, profit: {profit:,.0f}, max_profit: {max_profit_value:,.0f}")
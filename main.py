import ccxt
import pandas as pd
import time

pd.set_option('expand_frame_repr', False)


def main():
    # triangular arbitrage strategy
    # definition exchange
    binance_exchange = ccxt.binance({
        'timeout': 15000,
        'enableRateLimit': True
    })

    # load markets data
    markets = binance_exchange.load_markets()

    # ==== step 1 choice two markets
    market_A = 'BTC'
    market_B = 'ETH'

    # ==== step 2 choice one market base_quote base on A & B
    symbols = list(markets.keys())
    # put symbols in dataFrame
    symbols_df = pd.DataFrame(data=symbols, columns=['symbol'])
    # split up base an quote
    base_quote_df = symbols_df['symbol'].str.split(pat='/', expand=True)
    base_quote_df.columns = ['base', 'quote']
    # find quote list base on A and B
    base_a_list = base_quote_df[base_quote_df['quote'] == market_A]['base'].values.tolist()
    base_b_list = base_quote_df[base_quote_df['quote'] == market_B]['base'].values.tolist()
    # find common list base on same quote
    common_base_list = list(set(base_a_list).intersection(set(base_b_list)))
    print('{} and {} have {} base on same quote coin'.format(market_A, market_B, len(common_base_list)))

    # ==== step 3 ==== arbitrage strategy
    # put the list in the dataFrame
    columns = ['Market_A', 'Market_B', 'Market_C', 'P1', 'P2', 'P3', 'Profit%']
    results_df = pd.DataFrame(columns=columns)
    # get the last minute trading pair base on close price
    last_min = binance_exchange.milliseconds() - 60 * 1000
    for base_coin in common_base_list:
        market_C = base_coin
        market_a2b_symbol = '{}/{}'.format(market_B, market_A)
        market_b2c_symbol = '{}/{}'.format(market_C, market_B)
        market_a2c_symbol = '{}/{}'.format(market_C, market_A)
        # get the last minute kline data
        market_a2b_kline = binance_exchange.fetchOHLCV(market_a2b_symbol, since=last_min, limit=1, timeframe='1m')
        market_b2c_kline = binance_exchange.fetchOHLCV(market_b2c_symbol, since=last_min, limit=1, timeframe='1m')
        market_a2c_kline = binance_exchange.fetchOHLCV(market_a2c_symbol, since=last_min, limit=1, timeframe='1m')
        print(market_a2b_kline)

        if len(market_a2b_kline) == 0 or len(market_b2c_kline) == 0 or len(market_a2c_kline) == 0:
            continue
        # get the last minute price or trading pair
        p1 = market_a2b_kline[0][4]
        p2 = market_b2c_kline[0][4]
        p3 = market_a2c_kline[0][4]

        # get profit
        profit = (p3 / (p1 * p2) - 1) * 1000

        results_df = results_df.append({
            'Market_A': market_A,
            'Market_B': market_B,
            'Market_C': market_C,
            'P1': p1,
            'P2': p2,
            'P3': p3,
            'Profit%': profit
        }, ignore_index=True)
        print(results_df.tail(1))

        # Rate limit
        time.sleep(binance_exchange.rateLimit / 1000)

        # ==== step 3 end =====

        results_df.to_csv('./tri_arbitrage_results.csv', index=None)


if __name__ == '__main__':
    main()

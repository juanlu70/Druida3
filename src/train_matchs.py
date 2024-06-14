import libdb_mysql
import libstats
import train_utils


sts = libstats.LibStats()
tu = train_utils.TrainUtils()
db = libdb_mysql.LibDBMysql()
# db.debug_mode = 2


class TrainMatchs:
    def __init__(self):
        return

    # -- main maximum-minimum daily --
    def main_maxmin(self, ticker: str, day_data: tuple, curr_date: str) -> None:
        rows = []
        begin_secs = int(tu.calc_dates(curr_date, 0)[1])

        # -- delete data from this date --
        print("--> Launching main_maxmin with "+curr_date)
        tu.delete_curr_date(ticker, curr_date, "main_maxmin")

        # -- loop to get maximum and minimum for all day prices --
        for row in day_data['curr_date']:
            rows.append({
                'ticker': ticker,
                'fecha': curr_date,
                'hora': str(row['hora']),
                'secs': row['secs'],
                'daysecs': row['secs'] - begin_secs,
                'maxtrend': sts.get_multiplier(ticker, row['daymax'] - row['close'], 1),
                'mintrend': sts.get_multiplier(ticker, row['daymin'] - row['close'], 1)
            })

        db.row_insert(rows, "main_maxmin", "statsrt")

        return

    # -- process close last day --
    def ld_close(self, ticker: str, day_data: tuple, curr_date: str) -> None:
        rows = []
        begin_secs = int(tu.calc_dates(curr_date, 0)[1])

        # -- delete data from this date --
        print("--> Launching ld_close with "+curr_date)
        tu.delete_curr_date(ticker, curr_date, "match_close")

        # -- get last close and delete previous dates from list --
        last_close = tu.get_past_value(ticker, "ld_close", day_data)

        # -- loop to get differences between last open and current data for all day --
        for row in day_data['curr_date']:
            last_price = sts.get_multiplier(ticker, row['close'], 1)
            diff_num_close = last_price - last_close
            diff_pct_close = round((diff_num_close / last_close) * 100.00, 2)

            # print(str(row['hora'])+" "+str(diff_num_close)+" "+str(diff_pct_close)+"%")

            rows.append({
                'ticker': "KR-XBTUSD",
                'fecha': str(row['fecha']),
                # 'hora': str(row['hora']),
                'secs': row['secs'],
                'daysecs': row['secs'] - begin_secs,
                'close_diff': diff_num_close,
                'close_pct_diff': diff_pct_close
            })

        db.row_insert(rows, "match_close", "statsrt")

        return

    # -- process open last day --
    def ld_open(self, ticker: str, day_data: tuple, curr_date: str) -> None:
        rows = []
        begin_secs = int(tu.calc_dates(curr_date, 0)[1])

        # -- delete data from this date --
        print("--> Launching ld_open with "+curr_date)
        tu.delete_curr_date(ticker, curr_date, "match_close")

        # -- get last open and delete previous dates from list --
        last_open = tu.get_past_value(ticker, "ld_open", day_data)

        # -- loop to get differences between last open and current data for all day--
        for row in day_data['curr_date']:
            last_price = sts.get_multiplier(ticker, row['close'], 1)
            diff_num_open = last_price - last_open
            # diff_pct_open = round((diff_num_open / last_open) * 100.00, 2)
            diff_pct_open = round((last_price * 100.00) / last_open, 2)

            # print(str(row['hora'])+" "+str(diff_num_open)+" "+str(diff_pct_open)+"%")

            rows.append({
                'ticker': "KR-XBTUSD",
                'fecha': str(row['fecha']),
                # 'hora': str(row['hora']),
                'secs': row['secs'],
                'daysecs': row['secs'] - begin_secs,
                'open_diff': diff_num_open,
                'open_pct_diff': diff_pct_open
            })

        db.row_insert(rows, "match_open", "statsrt")

        return

    # -- process high last day --
    def ld_high(self, ticker: str, day_data: tuple, curr_date: str) -> None:
        rows = []
        begin_secs = int(tu.calc_dates(curr_date, 0)[1])

        # -- delete data from this date --
        print("--> Launching ld_high with "+curr_date)
        tu.delete_curr_date(ticker, curr_date, "match_high")

        # -- get last high and delete previous dates from list --
        last_high = tu.get_past_value(ticker, "ld_high", day_data)

        # -- loop to get differences between last high and current data for all day--
        for row in day_data['curr_date']:
            last_price = sts.get_multiplier(ticker, row['close'], 1)
            diff_num_high = last_price - last_high
            diff_pct_high = round((diff_num_high / last_high) * 100.00, 2)

            # print(str(row['hora'])+" "+str(diff_num_high)+" "+str(diff_pct_high)+"%")

            rows.append({
                'ticker': "KR-XBTUSD",
                'fecha': str(row['fecha']),
                # 'hora': str(row['hora']),
                'secs': row['secs'],
                'daysecs': row['secs'] - begin_secs,
                'high_diff': diff_num_high,
                'high_pct_diff': diff_pct_high
            })

        db.row_insert(rows, "match_high", "statsrt")

        return

    # -- process low last day --
    def ld_low(self, ticker: str, day_data: tuple, curr_date: str) -> None:
        rows = []
        begin_secs = int(tu.calc_dates(curr_date, 0)[1])

        # -- delete data from this date --
        print("--> Launching ld_low with "+curr_date)
        tu.delete_curr_date(ticker, curr_date, "match_low")

        # -- get last low and delete previous dates from list --
        last_low = tu.get_past_value(ticker, "ld_low", day_data)

        # -- loop to get differences between last low and current data for all day--
        for row in day_data['curr_date']:
            last_price = sts.get_multiplier(ticker, row['close'], 1)
            diff_num_low = last_low - last_price
            diff_pct_low = round((diff_num_low / last_low) * 100.00, 2)

            # print(str(row['hora'])+" "+str(diff_num_low)+" "+str(diff_pct_low)+"%")

            rows.append({
                'ticker': "KR-XBTUSD",
                'fecha': str(row['fecha']),
                # 'hora': str(row['hora']),
                'secs': row['secs'],
                'daysecs': row['secs'] - begin_secs,
                'low_diff': diff_num_low,
                'low_pct_diff': diff_pct_low
            })

        db.row_insert(rows, "match_low", "statsrt")

        return

    # -- process close last last day --
    def lld_close(self, ticker: str, day_data: tuple, curr_date: str) -> None:
        rows = []
        begin_secs = int(tu.calc_dates(curr_date, 0)[1])

        # -- delete data from this date --
        print("--> Launching lld_close with "+curr_date)
        tu.delete_curr_date(ticker, curr_date, "match_llclose")

        # -- get last last close and delete previous dates from list --
        ll_close = tu.get_past_value(ticker, "lld_close", day_data)

        # -- loop to get differences between last last close and current data for all day--
        for row in day_data['curr_date']:
            last_price = sts.get_multiplier(ticker, row['close'], 1)
            diff_num_close = ll_close - last_price
            diff_pct_close = round((diff_num_close / ll_close) * 100.00, 2)

            # print(str(row['hora'])+" "+str(diff_num_close)+" "+str(diff_pct_close)+"%")

            rows.append({
                'ticker': "KR-XBTUSD",
                'fecha': str(row['fecha']),
                # 'hora': str(row['hora']),
                'secs': row['secs'],
                'daysecs': row['secs'] - begin_secs,
                'llclose_diff': diff_num_close,
                'llclose_pct_diff': diff_pct_close
            })

        db.row_insert(rows, "match_llclose", "statsrt")

        return

    # -- process open last last day --
    def lld_open(self, ticker: str, day_data: tuple, curr_date: str) -> None:
        rows = []
        begin_secs = int(tu.calc_dates(curr_date, 0)[1])

        # -- delete data from this date --
        print("--> Launching lld_open with "+curr_date)
        tu.delete_curr_date(ticker, curr_date, "match_llopen")

        # -- get last last open and delete previous dates from list --
        ll_open = tu.get_past_value(ticker, "lld_open", day_data)

        # -- loop to get differences between last last open and current data for all day--
        for row in day_data['curr_date']:
            last_price = sts.get_multiplier(ticker, row['close'], 1)
            diff_num_open = ll_open - last_price
            diff_pct_open = round((diff_num_open / ll_open) * 100.00, 2)

            # print(str(row['hora'])+" "+str(diff_num_open)+" "+str(diff_pct_open)+"%")

            rows.append({
                'ticker': "KR-XBTUSD",
                'fecha': str(row['fecha']),
                # 'hora': str(row['hora']),
                'secs': row['secs'],
                'daysecs': row['secs'] - begin_secs,
                'llopen_diff': diff_num_open,
                'llopen_pct_diff': diff_pct_open
            })

        db.row_insert(rows, "match_llopen", "statsrt")

        return

    # -- process high last last day --
    def lld_high(self, ticker: str, day_data: tuple, curr_date: str) -> None:
        rows = []
        begin_secs = int(tu.calc_dates(curr_date, 0)[1])

        # -- delete data from this date --
        print("--> Launching lld_high with "+curr_date)
        tu.delete_curr_date(ticker, curr_date, "match_llhigh")

        # -- get last last high and delete previous dates from list --
        ll_high = tu.get_past_value(ticker, "lld_high", day_data)

        # -- loop to get differences between last last high and current data for all day--
        for row in day_data['curr_date']:
            last_price = sts.get_multiplier(ticker, row['close'], 1)
            diff_num_high = last_price - ll_high
            diff_pct_high = round((diff_num_high / ll_high) * 100.00, 2)

            # print(str(row['hora'])+" "+str(diff_num_high)+" "+str(diff_pct_high)+"%")

            rows.append({
                'ticker': "KR-XBTUSD",
                'fecha': str(row['fecha']),
                # 'hora': str(row['hora']),
                'secs': row['secs'],
                'daysecs': row['secs'] - begin_secs,
                'llhigh_diff': diff_num_high,
                'llhigh_pct_diff': diff_pct_high
            })

        db.row_insert(rows, "match_llhigh", "statsrt")

        return

    # -- process low last last day --
    def lld_low(self, ticker: str, day_data: tuple, curr_date: str) -> None:
        rows = []
        begin_secs = int(tu.calc_dates(curr_date, 0)[1])

        # -- delete data from this date --
        print("--> Launching lld_low with "+curr_date)
        tu.delete_curr_date(ticker, curr_date, "match_lllow")

        # -- get last last low and delete previous dates from list --
        ll_low = tu.get_past_value(ticker, "lld_low", day_data)

        # -- loop to get differences between last last low and current data for all day--
        for row in day_data['curr_date']:
            last_price = sts.get_multiplier(ticker, row['close'], 1)
            diff_num_low = last_price - ll_low
            diff_pct_low = round((diff_num_low / ll_low) * 100.00, 2)

            # print(str(row['hora'])+" "+str(diff_num_high)+" "+str(diff_pct_high)+"%")

            rows.append({
                'ticker': "KR-XBTUSD",
                'fecha': str(row['fecha']),
                # 'hora': str(row['hora']),
                'secs': row['secs'],
                'daysecs': row['secs'] - begin_secs,
                'lllow_diff': diff_num_low,
                'lllow_pct_diff': diff_pct_low
            })

        db.row_insert(rows, "match_lllow", "statsrt")

        return

    # -- process up points in the day --
    def ld_ups(self, ticker: str, day_data: tuple, curr_date: str) -> None:
        rows = []
        begin_secs = int(tu.calc_dates(curr_date, 0)[1])
        value_sum = 0

        # -- delete data from this date --
        print("--> Launching ld_ups with "+curr_date)
        tu.delete_curr_date(ticker, curr_date, "match_ups")
        last_price = sts.get_multiplier(ticker, day_data['curr_date'][0]['close'], 1)

        # -- loop to get differences between last last low and current data for all day--
        for row in day_data['curr_date']:
            ups_close = sts.get_multiplier(ticker, row['close'], 1)
            diff_with_last = sts.get_multiplier(ticker, ups_close - last_price, 1)

            if diff_with_last > 0:
                value_sum += 1

            # print(str(row['hora'])+" "+str(diff_num_high)+" "+str(diff_pct_high)+"%")

            rows.append({
                'ticker': "KR-XBTUSD",
                'fecha': str(row['fecha']),
                # 'hora': str(row['hora']),
                'secs': row['secs'],
                'daysecs': row['secs'] - begin_secs,
                'sum_num': value_sum
            })
            last_price = sts.get_multiplier(ticker, row['close'], 1)

        db.row_insert(rows, "match_ups", "statsrt")

        return

    # -- process down points in the day --
    def ld_downs(self, ticker: str, day_data: tuple, curr_date: str) -> None:
        rows = []
        begin_secs = int(tu.calc_dates(curr_date, 0)[1])
        sub = 0

        # -- delete data from this date --
        print("--> Launching ld_down with "+curr_date)
        tu.delete_curr_date(ticker, curr_date, "match_downs")
        last_price = sts.get_multiplier(ticker, day_data['curr_date'][0]['close'], 1)

        # -- loop to get differences between last last low and current data for all day--
        for row in day_data['curr_date']:
            ups_close = sts.get_multiplier(ticker, row['close'], 1)
            diff_with_last = sts.get_multiplier(ticker, ups_close - last_price, 1)

            if diff_with_last < 0:
                sub += 1

            # print(str(row['hora'])+" "+str(diff_num_high)+" "+str(diff_pct_high)+"%")

            rows.append({
                'ticker': "KR-XBTUSD",
                'fecha': str(row['fecha']),
                # 'hora': str(row['hora']),
                'secs': row['secs'],
                'daysecs': row['secs'] - begin_secs,
                'sub_num': sub
            })
            last_price = sts.get_multiplier(ticker, row['close'], 1)

        db.row_insert(rows, "match_downs", "statsrt")

        return

    # -- process curr highs in the day --
    def ld_currhigh(self, ticker: str, day_data: tuple, curr_date: str) -> None:
        rows = []
        begin_secs = int(tu.calc_dates(curr_date, 0)[1])

        # -- delete data from this date --
        print("--> Launching ld_currhigh with "+curr_date)
        tu.delete_curr_date(ticker, curr_date, "match_currhigh")

        # -- loop to get differences between current highs and current data for all day--
        for row in day_data['curr_date']:
            diff_currhigh = sts.get_multiplier(ticker, row['daymax'] - row['close'], 1)
            diff_pct_currhigh = round((diff_currhigh / row['close']) * 100.00, 2)

            # print(str(row['hora'])+" "+str(diff_currhigh)+" "+str(diff_pct_currhigh)+"%")

            rows.append({
                'ticker': "KR-XBTUSD",
                'fecha': str(row['fecha']),
                # 'hora': str(row['hora']),
                'secs': row['secs'],
                'daysecs': row['secs'] - begin_secs,
                'currhigh_diff': diff_currhigh,
                'currhigh_pct_diff': diff_pct_currhigh
            })

        db.row_insert(rows, "match_currhigh", "statsrt")

        return

    # -- process curr lows in the day --
    def ld_currlow(self, ticker: str, day_data: tuple, curr_date: str) -> None:
        rows = []
        begin_secs = int(tu.calc_dates(curr_date, 0)[1])

        # -- delete data from this date --
        print("--> Launching ld_currlow with "+curr_date)
        tu.delete_curr_date(ticker, curr_date, "match_currlow")

        # -- loop to get differences between current lows and current data for all day--
        for row in day_data['curr_date']:
            diff_currlow = sts.get_multiplier(ticker, row['daymin'] - row['close'], 1)
            diff_pct_currlow = round((diff_currlow / row['close']) * 100.00, 2)

            # print(str(row['hora'])+" "+str(diff_currlow)+" "+str(diff_pct_currlow)+"%")

            rows.append({
                'ticker': "KR-XBTUSD",
                'fecha': str(row['fecha']),
                # 'hora': str(row['hora']),
                'secs': row['secs'],
                'daysecs': row['secs'] - begin_secs,
                'currlow_diff': diff_currlow,
                'currlow_pct_diff': diff_pct_currlow
            })

        db.row_insert(rows, "match_currlow", "statsrt")

        return

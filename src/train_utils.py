import time
from operator import itemgetter

import libdb_mysql
import libstats


sts = libstats.LibStats()
db = libdb_mysql.LibDBMysql()


class TrainUtils:
    def __init__(self):
        return

    # -- calculate dates --
    def calc_dates(self, orig_date: str, operation: int) -> list:
        tmpsecs = time.strptime(orig_date + " 00:00:00", "%Y-%m-%d %H:%M:%S")
        secs = time.mktime(tmpsecs)
        secs = secs + (86400 * operation)
        final_date = str(time.strftime("%Y-%m-%d", time.localtime(secs)))

        return [final_date, secs]

    # -- delete day in database for the specified table --
    def delete_curr_date(self, ticker: str, curr_date: str, table: str) -> None:
        # -- delete data from this date --
        params = [
            "ticker='"+ticker+"'",
            "fecha='"+curr_date+"'"
        ]
        db.delete(params, table, "statsrt")

        return

    # -- get past value --
    def get_past_value(self, ticker: str, value: str, day_data: tuple):
        data = []
        last_day = ""
        return_value = 0

        # -- get return value according to value --
        if value == "ld_close":
            if len(day_data['l_day']) > 0:
                return_value = sts.get_multiplier(ticker, day_data['l_day'][-1]['close'], 1)
            else:
                return_value = sts.get_multiplier(ticker, day_data['curr_date'][-1]['close'], 1)
        if value == "ld_open":
            if len(day_data['l_day']) > 0:
                return_value = sts.get_multiplier(ticker, day_data['l_day'][0]['open'], 1)
            else:
                return_value = sts.get_multiplier(ticker, day_data['curr_date'][-1]['open'], 1)
        if value == "ld_high":
            if len(day_data['l_day']) > 0:
                sort_data = sorted(day_data['l_day'], key=itemgetter('high'))
                return_value = sts.get_multiplier(ticker, sort_data[0]['high'], 1)
            else:
                return_value = sts.get_multiplier(ticker, day_data['curr_date'][-1]['high'], 1)
        if value == "ld_low":
            if len(day_data['l_day']) > 0:
                sort_data = sorted(day_data['l_day'], key=itemgetter('low'))
                return_value = sts.get_multiplier(ticker, sort_data[-1]['low'], 1)
            else:
                return_value = sts.get_multiplier(ticker, day_data['curr_date'][-1]['low'], 1)
        if value == "lld_close":
            if len(day_data['ll_day']) > 0:
                return_value = sts.get_multiplier(ticker, day_data['ll_day'][-1]['close'], 1)
            else:
                return_value = sts.get_multiplier(ticker, day_data['l_day'][-1]['close'], 1)
        if value == "lld_open":
            if len(day_data['ll_day']) > 0:
                return_value = sts.get_multiplier(ticker, day_data['ll_day'][0]['open'], 1)
            else:
                return_value = sts.get_multiplier(ticker, day_data['l_day'][0]['open'], 1)
        if value == "lld_high":
            if len(day_data['ll_day']) > 0:
                sort_data = sorted(day_data['ll_day'], key=itemgetter('high'))
                return_value = sts.get_multiplier(ticker, sort_data[0]['high'], 1)
            else:
                sort_data = sorted(day_data['l_day'], key=itemgetter('high'))
                return_value = sts.get_multiplier(ticker, sort_data[0]['high'], 1)
        if value == "lld_low":
            if len(day_data['ll_day']) > 0:
                sort_data = sorted(day_data['ll_day'], key=itemgetter('low'))
                return_value = sts.get_multiplier(ticker, sort_data[-1]['low'], 1)
            else:
                sort_data = sorted(day_data['l_day'], key=itemgetter('low'))
                return_value = sts.get_multiplier(ticker, sort_data[-1]['low'], 1)

        return return_value

    # -- get an array for only one date --
    def get_current_date(self, curr_date: str, day_data: tuple) -> list:
        data = []
        for item in day_data:
             if curr_date in str(item):
                data.append(item)

        return data

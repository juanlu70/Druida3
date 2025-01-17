import logging

import db_mysql
import druida_utils


db = db_mysql.DbMysql()
utils = druida_utils.DruidaUtils()


class DruidaData:
    def __init__(self):
        self.arguments = dict()
        self.database = None
        self.table = "quotes1"
        self.diff_field = None
        self.pct_diff_field = None
        self.class_name = None
        self.last_id = 0
        self.ticker = None
        self.date = None
        self.day_seconds = None
        self.diff_field = None
        self.close = None
        self.old_close = None
        self.seconds = None

        return

    def set_arguments(self, arguments) -> None:
        self.arguments = arguments
        if self.arguments['verbose'] > 0:
            db.debug_mode = self.arguments['verbose']

        return

    def set_close(self, close: int) -> None:
        self.close = close

        return

    def set_seconds(self, seconds) -> None:
        self.seconds = seconds

        return

    def set_old_close(self, old_close: int) -> None:
        self.old_close = old_close

        return

    def clean_newlevels(self):
        db.set_database("statsrt")
        db.set_table("newlevels")

        params = [
            "ticker='" + self.arguments['ticker'] + "'",
            "fecha='" + self.arguments['date'] + "'"
        ]
        db.delete(params)

        return

    def set_matches_values(self, values: dict) -> None:
        self.day_seconds = values['today_seconds']
        self.arguments = values['arguments']
        # self.diff_field = values['diff_field']
        self.pct_diff_field = values['pct_diff_field']
        self.table = values['table']
        self.class_name = values['class_name']

        return

    def get_old_day_data(self) -> list:
        db.set_table("quotes1")
        db.get_bolsa_database(self.arguments['date'])

        params = [
            "ticker='" + self.arguments['ticker'] + "'",
            "fecha='" + self.arguments['date'] + "'",
            "ORDER BY hora"
        ]
        data = db.select(params)

        if len(data) > 0:
            self.last_id = data[-1]['id']

        utils.arguments = self.arguments
        data = utils.get_row_multiplier(data)

        return data

    def get_current_data(self) -> list:
        db.set_table("quotes1")
        db.get_bolsa_database(self.arguments['date'])

        params = [
            "id>"+str(self.last_id),
            "ticker='" + self.arguments['ticker'] + "'",
            "fecha='" + self.arguments['date'] + "'",
            "ORDER BY hora"
        ]
        data = list(db.select(params))
        if len(data) > 0:
            self.last_id = data[-1]['id']

        return data

    def get_max_date(self) -> int:
        max_secs = 0
        db.set_database("statsrt")
        db.set_table("match_close")

        params = [
            "SELECT MAX(secs)",
            "ticker='" + self.arguments['ticker'] + "'"
        ]
        data = db.select(params)

        if len(data) > 0:
            max_secs = data[0]['MAX(secs)']

        return max_secs

    def get_distinct_dates(self) -> list:
        dates = list()
        db.get_bolsa_database(self.arguments['date_ini'])

        params = [
            "SELECT DISTINCT(fecha)",
            "ticker='" + self.arguments['ticker'] + "'",
            "fecha>='" + self.arguments['date_ini'] + "'",
            "fecha<='" + self.arguments['date_end'] + "'",
            "ORDER BY fecha"
        ]
        tmp = db.select(params)

        for item in tmp:
            dates.append(str(item['fecha']))

        return dates

    def get_day_data_from_date(self, date: str) -> list:
        db.get_bolsa_database(date)

        params = [
            "ticker='" + self.arguments['ticker'] + "'",
            "fecha='" + str(date) + "'",
            "ORDER BY fecha, hora"
        ]
        data = db.select(params)

        utils.arguments = self.arguments
        utils.arguments['date'] = date
        day_data = utils.get_row_multiplier(data)

        return day_data

    def get_maxtrend(self, data: dict) -> int:
        utils.arguments = self.arguments
        date = str(data['fecha'])
        db.get_bolsa_database(date)

        params = [
            "SELECT MAX(high)",
            "ticker='" + self.arguments['ticker'] + "'",
            "fecha='" + date + "'",
            "secs>=" + str(data['secs'])
        ]
        db_data = db.select(params)

        try:
            tmp = db_data[0]['MAX(high)']
        except IndexError:
            line = "Can't get maxtrend!"
            logging.info(line)
            print(line)
            exit()

        maxtrend = utils.get_multiplier(tmp)

        return maxtrend

    def get_mintrend(self, data: dict) -> int:
        utils.arguments = self.arguments
        date = str(data['fecha'])
        db.get_bolsa_database(date)

        params = [
            "SELECT MIN(low)",
            "ticker='" + self.arguments['ticker'] + "'",
            "fecha='" + date + "'",
            "secs>=" + str(data['secs'])
        ]
        db_data = db.select(params)

        try:
            tmp = db_data[0]['MIN(low)']
        except IndexError:
            line = "Can't get mintrend!"
            logging.info(line)
            print(line)
            exit()

        mintrend = utils.get_multiplier(tmp)

        return mintrend

    def make_diff_query(self, diff_value: int) -> list:
        db.set_database("statsrt")
        db.set_table(self.table)

        params = [
            "ticker='" + self.arguments['ticker'] + "'",
            "daysecs=" + str(self.day_seconds),
            "fecha<'" + self.arguments['date'] + "'"
            # self.diff_field + "=" + str(diff_value)
        ]
        matches = db.select(params)

        return matches

    def make_pctdiff_query(self, pctdiff_value: int) -> list:
        db.set_database("statsrt")
        db.set_table(self.table)
        low_pct_value = round(pctdiff_value - 0.01, 2)
        high_pct_value = round(pctdiff_value + 0.01, 2)

        params = [
            "ticker='" + self.arguments['ticker'] + "'",
            "daysecs=" + str(self.day_seconds),
            "fecha<'" + self.arguments['date'] + "'",
            self.pct_diff_field + ">" + str(low_pct_value),
            self.pct_diff_field + "<" + str(high_pct_value)
        ]
        matches = db.select(params)

        return matches

    def delete_current_day(self, date: str) -> None:
        db.set_database("statsrt")
        db.set_table(self.table)

        params = [
            "ticker='" + self.arguments['ticker'] + "'",
            "fecha='" + date + "'"
        ]
        db.delete(params)

        return

    def save_match_day(self, match_list: list) -> None:
        db.set_database("statsrt")
        db.set_table(self.table)

        db.row_insert(match_list)

        return

    def check_levels(self, level: int) -> bool:
        db.set_database("statsrt")
        db.set_table("newlevels")
        ret_value = False

        params = [
            "ticker='" + self.arguments['ticker'] + "'",
            "fecha='" + self.arguments['date'] + "'",
            "level=" + str(level)
        ]
        result = db.select(params)

        if len(result) > 0:
            ret_value = True

        return ret_value

    def insert_new_level(self, newlevel: dict) -> None:
        db.set_database("statsrt")
        db.set_table("newlevels")

        db.insert(newlevel)

        return

    def get_level(self, level: int) -> dict:
        db.set_database("statsrt")
        db.set_table("newlevels")

        params = [
            "ticker='" + self.arguments['ticker'] + "'",
            "fecha='" + self.arguments['date'] + "'",
            "level=" + str(level)
        ]
        data = db.select(params)[0]

        return data

    def update_level_num(self, data: dict) -> None:
        db.set_database("statsrt")
        db.set_table("newlevels")

        params1 = [
            "num=" + str(data['num'])
        ]
        params2 = [
            "id=" + str(data['id'])
        ]
        db.update(params1, params2)

        return

    def get_newlevel_highs(self) -> list:
        db.set_database("statsrt")
        db.set_table("newlevels")

        params = [
            "ticker='" + self.arguments['ticker'] + "'",
            "fecha='" + self.arguments['date'] + "'",
            "level>" + str(self.close),
            "burned=0",
            "ORDER BY num DESC, secs ASC"
        ]
        highs = db.select(params)

        return highs

    def get_newlevel_lows(self) -> list:
        db.set_database("statsrt")
        db.set_table("newlevels")

        params = [
            "ticker='" + self.arguments['ticker'] + "'",
            "fecha='" + self.arguments['date'] + "'",
            "level<" + str(self.close),
            "burned=0",
            "ORDER BY num DESC, secs ASC"
        ]
        lows = db.select(params)

        return lows

    def burn_last_levels(self) -> None:
        db.set_database("statsrt")
        db.set_table("newlevels")

        params1 = [
            "burned=1"
        ]
        params2 = [
            "ticker='" + self.arguments['ticker'] + "'",
            "fecha='" + self.arguments['date'] + "'"
        ]

        if self.close > self.old_close:
            params2.append("level>" + str(self.old_close))
            params2.append("level<" + str(self.close))

        if self.close < self.old_close:
            params2.append("level>" + str(self.close))
            params2.append("level<" + str(self.old_close))

        if self.close != self.old_close:
            db.update(params1, params2)

        return

    def burn_hour_ago_levels(self) -> None:
        db.set_database("statsrt")
        db.set_table("newlevels")
        burn_seconds = self.seconds - 3600

        params1 = [
            "burned=1",
            "num=0"
        ]
        params2 = [
            "ticker='" + self.arguments['ticker'] + "'",
            "fecha='" + self.arguments['date'] + "'",
            "secs<" + str(burn_seconds)
        ]
        db.update(params1, params2)

        return

    def get_current_pairs(self) -> list:
        db.set_database("statsrt")
        db.set_table("newpairs")

        params = [
            "ticker='" + self.arguments['ticker'] + "'",
            "fecha='" + self.arguments['date'] + "'"
        ]
        data = db.select(params)

        return data

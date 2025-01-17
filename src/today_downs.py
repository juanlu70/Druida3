import logging
import time
import multiprocessing as mp

import db_mysql
import druida_utils
import druida_data


db = db_mysql.DbMysql()
utils = druida_utils.DruidaUtils()
data = druida_data.DruidaData()


class TodayDowns:
    def __init__(self):
        self.name = "today_downs"
        self.class_name = "TodayDowns"
        self.table = "match_downs"
        self.value_name = "match downs"
        self.field = ""
        self.arguments = dict()
        self.current_date = None
        self.BASE_VALUE = 0
        self.current_close = None
        self.old_close = 0
        self.old_fecha = None
        self.last_value = None
        self.today_seconds = None
        self.diff_field = "sub_num"
        self.pct_diff_field = None
        self.diff_value = None
        self.pctdiff_value = None
        self.matches = list()
        self.maxtrend = None
        self.mintrend = None
        self.old_daymax = None
        self.old_daymin = None
        self.force_base_value = 0

        return

    def set_arguments(self, arguments: dict) -> None:
        self.arguments = arguments

        return

    def set_current_date(self, date: str) -> None:
        self.current_date = date

        return

    def update_base_value(self, row: dict) -> None:
        self.BASE_VALUE = row['downs']

        return

    def update_train_base_value(self, row: dict) -> None:
        fecha = str(row['fecha'])
        if self.old_fecha != fecha:
            self.BASE_VALUE = 0

        if self.old_close is None:
            self.old_close = row['close']

        if self.old_close > row['close']:
            self.BASE_VALUE += 1

        self.old_fecha = fecha
        self.old_close = row['close']

        return

    def get_matches(self, row: dict, sender: mp.Pipe) -> list:
        self.matches = list()
        self.update_base_value(row)
        self.today_seconds = utils.get_day_seconds(str(row['fecha']) + " " + str(row['hora']))
        if self.today_seconds <= 300:
            if self.BASE_VALUE == 0:
                sender.send(self.matches)
                return self.matches

        data.set_matches_values({
            'today_seconds': self.today_seconds,
            'arguments': self.arguments,
            'diff_field': self.diff_field,
            'pct_diff_field': self.pct_diff_field,
            'table': self.table,
            'class_name': self.class_name
        })

        diff_matchs = data.make_diff_query(self.BASE_VALUE)
        self.show_matchs(diff_matchs)
        self.matches.extend(diff_matchs)
        sender.send(self.matches)

        return self.matches

    def show_matchs(self, matchs: list) -> None:
        if matchs is not None:
            if len(matchs) > 0:
                line = (str(len(matchs)) + " match(es) on " + self.value_name)
                logging.info(line)
                print(line)

        return

    def make_training(self, row_data: list) -> None:
        match_list = list()
        self.arguments['date'] = str(row_data[0]['fecha'])
        self.old_close = row_data[0]['close']
        self.old_fecha = str(row_data[0]['fecha'])

        data.set_matches_values({
            'today_seconds': 0,
            'arguments': self.arguments,
            'diff_field': self.diff_field,
            'pct_diff_field': self.pct_diff_field,
            'table': self.table,
            'class_name': self.class_name
        })

        self.maxtrend = row_data[0]['close']
        self.mintrend = row_data[0]['close']
        self.old_daymax = self.maxtrend
        self.old_daymin = self.mintrend

        for row in row_data:
            self.update_train_base_value(row)
            day_seconds = utils.get_day_seconds(str(row['fecha']) + " " + str(row['hora']))

            if row['daymax'] > self.old_daymax:
                self.maxtrend = data.get_maxtrend(row)
                self.old_daymax = row['daymax']
            if row['daymin'] < self.old_daymin:
                self.mintrend = data.get_mintrend(row)
                self.old_daymin = row['daymin']

            single_match = {
                'ticker': self.arguments['ticker'],
                'fecha': str(row['fecha']),
                'secs': row['secs'],
                'daysecs': day_seconds,
                self.diff_field: self.BASE_VALUE,
                'maxtrend': self.maxtrend - row['close'],
                'mintrend': self.mintrend - row['close']
            }
            match_list.append(single_match)

            time.sleep(0.0001)

        data.delete_current_day(self.arguments['date'])
        data.save_match_day(match_list)

        return

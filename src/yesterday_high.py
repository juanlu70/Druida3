import logging
import time
import multiprocessing as mp

import db_mysql
import druida_utils
import druida_data


db = db_mysql.DbMysql()
utils = druida_utils.DruidaUtils()
data = druida_data.DruidaData()


class YesterdayHigh:
    def __init__(self):
        self.name = "yesterday_high"
        self.class_name = "YesterdayHigh"
        self.table = "match_high"
        self.field = "high"
        self.arguments = dict()
        self.current_date = None
        self.BASE_VALUE = None
        self.last_value = None
        self.today_seconds = None
        self.diff_field = "high_diff"
        self.pct_diff_field = "high_pct_diff"
        self.diff_value = None
        self.pctdiff_value = None
        self.value_name = None
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

    def update_base_value(self) -> None:
        if self.force_base_value == 0:
            if self.BASE_VALUE is not None:
                return

        base_value_date = utils.calculate_dates(self.arguments['date'], -1)[0]
        db.get_bolsa_database(base_value_date)
        secs_ini = str(utils.get_timestamp(base_value_date + " 00:00:00"))
        secs_end = str(utils.get_timestamp(base_value_date + " 23:59:59"))

        params = [
            "SELECT MAX(high)",
            "ticker='" + self.arguments['ticker'] + "'",
            "secs>=" + secs_ini,
            "secs<=" + secs_end
        ]
        db_data = db.select(params)

        try:
            self.last_value = db_data[0]['MAX(high)']
        except IndexError:
            print(self.class_name + " Error getting base value!")
            exit()

        utils.arguments = self.arguments
        final_number = utils.get_multiplier(self.last_value)

        self.BASE_VALUE = final_number

        if self.force_base_value == 1:
            self.force_base_value = 0

        return

    def get_diff_formula(self, row: dict) -> None:
        current_value = row[self.field]
        self.diff_value = current_value - self.BASE_VALUE

        return

    def get_pct_diff_formula(self, row: dict) -> None:
        current_value = row[self.field]
        self.diff_value = current_value - self.BASE_VALUE
        self.pctdiff_value = round((self.diff_value * 100.00) / self.BASE_VALUE, 2)

        return

    def get_matches(self, row: dict, sender: mp.Pipe) -> list:
        self.matches = list()
        self.update_base_value()
        self.today_seconds = utils.get_day_seconds(str(row['fecha']) + " " + str(row['hora']))

        self.get_diff_formula(row)
        self.get_pct_diff_formula(row)

        data.set_matches_values({
            'today_seconds': self.today_seconds,
            'arguments': self.arguments,
            'diff_field': self.diff_field,
            'pct_diff_field': self.pct_diff_field,
            'table': self.table,
            'class_name': self.class_name
        })

        diff_matchs = data.make_diff_query(self.diff_value)
        self.value_name = "high diff"
        self.show_matchs(diff_matchs)
        self.matches.extend(diff_matchs)

        pct_diff_matchs = data.make_pctdiff_query(self.pctdiff_value)
        self.value_name = "high pctdiff"
        self.show_matchs(pct_diff_matchs)
        self.matches.extend(pct_diff_matchs)
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
        self.maxtrend = row_data[0]['close']
        self.mintrend = row_data[0]['close']
        self.old_daymax = self.maxtrend
        self.old_daymin = self.mintrend
        data.set_matches_values({
            'today_seconds': 0,
            'arguments': self.arguments,
            'diff_field': self.diff_field,
            'pct_diff_field': self.pct_diff_field,
            'table': self.table,
            'class_name': self.class_name
        })

        self.force_base_value = 1
        self.update_base_value()

        for row in row_data:
            day_seconds = utils.get_day_seconds(str(row['fecha']) + " " + str(row['hora']))

            if row['daymax'] > self.old_daymax:
                self.maxtrend = data.get_maxtrend(row)
                self.old_daymax = row['daymax']
            if row['daymin'] < self.old_daymin:
                self.mintrend = data.get_mintrend(row)
                self.old_daymin = row['daymin']

            self.get_diff_formula(row)
            self.get_pct_diff_formula(row)

            single_match = {
                'ticker': self.arguments['ticker'],
                'fecha': str(row['fecha']),
                'secs': row['secs'],
                'daysecs': day_seconds,
                self.diff_field: self.diff_value,
                self.pct_diff_field: self.pctdiff_value,
                'maxtrend': self.maxtrend - row['close'],
                'mintrend': self.mintrend - row['close']
            }
            match_list.append(single_match)

            time.sleep(0.0001)

        data.delete_current_day(self.arguments['date'])
        data.save_match_day(match_list)

        return

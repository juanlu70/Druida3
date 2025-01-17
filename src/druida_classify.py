import multiprocessing as mp
import time

import druida_data
import db_mysql


data = druida_data.DruidaData()
db = db_mysql.DbMysql()


class DruidaClassify:
    def __init__(self):
        self.arguments = None
        self.close = None
        self.old_close = None
        self.seconds = None
        self.max_level = None
        self.min_level = None
        self.level_up = None
        self.level_dn = None
        self.max_price = None
        self.min_price = None

        return

    def set_arguments(self, arguments) -> None:
        self.arguments = arguments

        return

    def set_close(self, last_price: int) -> None:
        self.close = last_price

        return

    def set_old_close(self, old_close: int) -> None:
        self.old_close = old_close

        return

    def set_seconds(self, seconds: int) -> None:
        self.seconds = seconds

        return

    def make_classifier(self, matchs: list) -> None:
        jobs = list()

        p = mp.Process(target=self.add_levels, args=[matchs])
        jobs.append(p)
        p.start()
        p = mp.Process(target=self.process_burn_levels, args=[])
        jobs.append(p)
        p.start()
        p = mp.Process(target=self.get_pairs)
        jobs.append(p)
        p.start()

        for job in jobs:
            job.join(timeout=None)
            while job.is_alive():
                time.sleep(0.1)

        return

    def add_levels(self, matchs: list) -> None:
        data.set_arguments(self.arguments)

        for match in matchs:
            self.level_up = self.close + match['maxtrend']
            self.level_dn = self.close + match['mintrend']
            new_level = {
                'ticker': self.arguments['ticker'],
                'fecha': self.arguments['date'],
                'highlow': 1,
                'level': self.level_up,
                'num': 1,
                'secs': self.seconds,
                'burned': 0
            }

            self.modify_add_level_high(new_level)
            self.modify_add_level_low(new_level)

        return

    def modify_add_level_high(self, new_level: dict) -> None:
        if data.check_levels(self.level_up) is False:
            new_level['highlow'] = 1
            new_level['level'] = self.level_up
            data.insert_new_level(new_level)
        else:
            level = data.get_level(self.level_up)
            level_data = dict()
            level_data['num'] = level['num'] + 1
            level_data['id'] = level['id']

            data.update_level_num(level_data)

        return

    def modify_add_level_low(self, new_level: dict) -> None:
        if data.check_levels(self.level_dn) is False:
            new_level['highlow'] = -1
            new_level['level'] = self.level_dn
            data.insert_new_level(new_level)
        else:
            level = data.get_level(self.level_dn)
            level_data = dict()
            level_data['num'] = level['num'] + 1
            level_data['id'] = level['id']

            data.update_level_num(level_data)

        return

    def get_maxmin(self) -> list:
        data.set_arguments(self.arguments)
        data.set_old_close(self.old_close)
        data.set_close(self.close)

        highs = data.get_newlevel_highs()
        lows = data.get_newlevel_lows()

        return [highs, lows]

    def process_burn_levels(self) -> None:
        data.set_arguments(self.arguments)
        data.set_old_close(self.old_close)
        data.set_close(self.close)
        data.burn_last_levels()

        data.set_seconds(self.seconds)
        data.burn_hour_ago_levels()

        return

    def get_pairs(self) -> None:
        self.max_price = {'all': 0.00, 'allnum': 0, 'touched': 0.00, 'maxnum': 0.00, 'num': 0}
        self.min_price = {'all': 0.00, 'allnum': 0, 'touched': 0.00, 'maxnum': 0.00, 'num': 0}

        self.burn_past_pairs()
        self.add_new_pairs()

        return

    def burn_past_pairs(self) -> None:
        data.set_arguments(self.arguments)
        pairs = data.get_current_pairs()

        for pair in pairs:
            touched = 0

            if self.close >= self.old_close:
                if self.old_close <= self.max_price <= self.close:
                    touched = -1
                if self.old_close <= self.min_price <= self.close:
                    touched = -1
            if self.close < self.old_close:
                if self.close <= self.max_price <= self.old_close:
                    touched = 1
                if self.close <= self.min_price <= self.old_close:
                    touched = 1

            if touched == -1 or touched == 1:
                if pair['first_mm'] == 0:
                    params1 = {
                        'first_mm': touched,
                        'op_secs': self.seconds,
                    }
                    params2 = ["id="+str(pair['id'])]
                    db.update(params1, params2, "pairs", "statsrt")
                if pair['first_mm'] == -1 and touched == 1:
                    params1 = {
                        'op_finish': 1,
                        'op_secs': self.seconds - pair['op_secs']
                    }
                    params2 = ["id="+str(pair['id'])]
                    db.update(params1, params2, "pairs", "statsrt")

                if pair['first_mm'] == 1 and touched == -1:
                    params1 = {
                        'op_finish': 1,
                        'op_secs': self.seconds - pair['op_secs']
                    }
                    params2 = ["id="+str(pair['id'])]
                    db.update(params1, params2, "pairs", "statsrt")

        return

    def add_new_pairs(self) -> None:

        return

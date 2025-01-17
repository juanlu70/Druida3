import multiprocessing as mp
import time
import os
import logging

import druida_process
import druida_utils
import druida_data
import druida_classify


prcs = druida_process.DruidaProcess()
utils = druida_utils.DruidaUtils()
data = druida_data.DruidaData()
clsfy = druida_classify.DruidaClassify()


class DruidaBase:
    def __init__(self):
        self.data = None
        self.cpus = len(prcs.matchers)
        self.matches = list()
        self.sum_matches = 0
        self.arguments = dict()
        self.matcher_objects = list()
        self.row = None
        self.old_close = None
        self.old_fecha = None
        self.ups = 0
        self.downs = 0
        self.close = None
        self.fecha = None
        self.hora = None

        return

    def set_arguments(self, arguments: dict) -> None:
        self.arguments = arguments

        return

    def set_row(self, row: dict) -> None:
        self.row = row

        return

    def set_old_close(self, old_close: int) -> None:
        self.old_close = old_close

        return

    def set_old_fecha(self, old_fecha: str) -> None:
        self.old_fecha = old_fecha

        return

    def make_previous_db_clean(self):
        data.set_arguments(self.arguments)

        data.clean_newlevels()

        return

    def run_druida_process_old_data(self) -> None:
        data.set_arguments(self.arguments)

        self.matcher_objects = prcs.get_matcher_objects()

        self.data = data.get_old_day_data()
        self.get_matches()

        return

    def run_druida_process_current_data(self) -> None:
        data.set_arguments(self.arguments)

        self.matcher_objects = prcs.get_matcher_objects()

        self.data = data.get_current_data()
        self.get_matches()

        return

    def get_matches(self) -> None:
        jobs = list()
        clsfy.set_arguments(self.arguments)

        for row in self.data:
            self.row = row
            self.add_synthesized_data()

            self.close = row['close']
            self.fecha = row['fecha']
            self.hora = row['hora']
            self.matches = list()
            receiver, sender = mp.Pipe()

            for process in self.matcher_objects:
                process.set_arguments(self.arguments)

                p = mp.Process(target=process.get_matches, args=[self.row, sender])
                jobs.append(p)
                p.start()
                time.sleep(0.1)

            for job in jobs:
                job.join(timeout=None)
                while job.is_alive():
                    time.sleep(0.1)

            for job in jobs:
                self.matches.extend(receiver.recv())
            self.sum_matches += len(self.matches)

            clsfy.set_close(self.row['close'])
            clsfy.set_old_close(self.old_close)
            clsfy.set_seconds((self.row['secs']))
            clsfy.make_classifier(self.matches)

            self.show_levels(self.row)

            jobs = list()

            self.old_close = self.row['close']
            self.old_fecha = str(self.row['fecha'])

        return

    def add_synthesized_data(self) -> None:
        if self.old_close is None:
            self.old_close = self.row['close']
        if self.old_fecha is None:
            self.old_fecha = str(self.row['fecha'])

        if self.old_close < self.row['close']:
            self.ups += 1
        if self.old_close > self.row['close']:
            self.downs += 1

        self.row['ups'] = self.ups
        self.row['downs'] = self.downs

        return

    def show_levels(self, row: dict) -> None:
        clsfy.set_arguments(self.arguments)
        [high, low] = clsfy.get_maxmin()

        utils.set_arguments(self.arguments)

        high_level = utils.get_divider(high[0]['level'])
        low_level = utils.get_divider(low[0]['level'])
        num_high_level = str(high[0]['num'])
        num_low_level = str(low[0]['num'])
        price_range = high_level - low_level

        time_price_line = (self.arguments['ticker'] + " " + self.arguments['date'] + " " + str(self.hora) + " " +
                           str(utils.get_divider(row['close'])) + " - Matches: " + str(self.sum_matches))
        levels_line = (" - Cur.: " + str(high_level) + "-" + str(low_level) + " (" + num_high_level + "-" +
                      num_low_level + ") (" + str(round(price_range, 0)) + ")")

        line = time_price_line + " " + levels_line

        logging.info(line)
        print(line)

        return

    def start_logging(self) -> None:
        if os.path.isfile(self.arguments['log_file']):
            os.remove(self.arguments['log_file'])
        logging.basicConfig(filename=self.arguments['log_file'], level=logging.INFO)

        return

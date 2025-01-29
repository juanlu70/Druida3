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
        self.mpprocess = list()
        self.result_queue = None
        self.task_queues = list()

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
        self.start_matchers_process()
        self.get_matches()

        return

    def run_druida_process_current_data(self) -> None:
        data.set_arguments(self.arguments)

        self.matcher_objects = prcs.get_matcher_objects()

        self.data = data.get_current_data()
        self.get_matches()

        return

    def start_matchers_process(self) -> None:
        self.result_queue = mp.Queue()
        self.task_queues = list()

        for item, process in enumerate(self.matcher_objects):
            task_queue = mp.Queue()
            process.set_arguments(self.arguments)
            p = mp.Process(target=process.get_matches, args=(task_queue, self.result_queue))
            self.mpprocess.append(p)
            self.task_queues.append(task_queue)
            p.start()

        self.start_classifier_process()

        return

    def start_classifier_process(self) -> None:
        # task_queue = mp.Queue()
        # clsfy.set_arguments(self.arguments)
        #
        # p = mp.Process(target=clsfy.add_levels, args=(task_queue, self.result_queue))
        # self.mpprocess.append(p)
        # self.task_queues.append(task_queue)
        # p.start()
        # clsfy.set_process_number(len(self.mpprocess))

        # p = mp.Process(target=clsfy.process_burn_levels, args=(task_queue, self.result_queue))
        # self.mpprocess.append(p)
        # self.task_queues.append(task_queue)
        # p.start()
        # clsfy.set_process_number(len(self.mpprocess))
        #
        # p = mp.Process(target=clsfy.get_pairs, args=(task_queue, self.result_queue))
        # self.mpprocess.append(p)
        # self.task_queues.append(task_queue)
        # task_queue.put(self.data)
        # p.start()
        # clsfy.set_process_number(len(self.mpprocess))

        return

    def get_matches(self) -> None:
        clsfy.set_arguments(self.arguments)

        for row in self.data:
            self.row = row
            self.add_synthesized_data()

            self.close = row['close']
            self.fecha = row['fecha']
            self.hora = row['hora']
            self.matches = list()

            for item, process in enumerate(self.matcher_objects):
                self.task_queues[item].put(self.row)

            for _ in self.mpprocess:
                response = self.result_queue.get()
                self.matches.extend(response)
            self.sum_matches += len(self.matches)
            print("---------------------------------------------")
            print(f"TOTAL MATCHES: {self.sum_matches}")

            print("BEGIN CLASSIFY!")

            clsfy.set_close(self.row['close'])
            clsfy.set_old_close(self.old_close)
            clsfy.set_seconds((self.row['secs']))
            clsfy.make_classifier(self.matches)
            # self.task_queues[clsfy.processes_numbers[-1]].put(self.matches)

            print("END CLASSIFY!")

            self.show_levels(self.row)

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

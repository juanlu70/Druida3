import multiprocessing as mp
import time

import db_mysql
import druida_process
import druida_utils
import druida_data


db = db_mysql.DbMysql()
pr = druida_process.DruidaProcess()
utils = druida_utils.DruidaUtils()
data = druida_data.DruidaData()


class TrainingBase:
    def __init__(self):
        self.arguments = None
        self.dates = list()
        self.date = None
        self.matcher_objects = list()
        self.today_seconds = None
        self.properties = dict()
        self.max_processes = 2048
        self.min_processes = 512

        return

    def run_training(self):
        self.get_different_dates()

        if len(self.matcher_objects) == 0:
            self.matcher_objects = pr.get_matcher_objects()

        self.process_training()

        return

    def get_different_dates(self) -> None:
        data.set_arguments(self.arguments)
        self.dates = data.get_distinct_dates()

        return

    def process_training(self):
        jobs = list()

        for date in self.dates:
            self.date = date
            day_data = data.get_day_data_from_date(date)

            for process in self.matcher_objects:
                process.set_arguments(self.arguments)

                print(f"---> PROCESSING DATE: {date} for {process.class_name}")

                p = mp.Process(target=process.make_training, args=[day_data])
                jobs.append(p)
                p.start()

                process.set_current_date(self.date)
                time.sleep(0.1)

            for job in jobs:
                job.join(timeout=0)
                while job.is_alive():
                    time.sleep(0.1)
                job.terminate()
            jobs = list()

        return

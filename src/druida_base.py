import importlib

import db_mysql
import druida_utils
import druida_data


db = db_mysql.DbMysql()
utils = druida_utils.DruidaUtils()
dd = druida_data.DruidaData()


class DruidaBase:
    def __init__(self):
        self.data = None
        self.matcher = None
        self.matches = list()
        self.arguments = dict()
        self.matcher_objects = list()

        self.matchers = [
            "yesterday_open",
            "yesterday_close",
            # "yesterday_high",
            # "yesterday_low",
            # "yesterday_close",
            # "yesterday_befone_open",
            # "yesterday_befone_high",
            # "yesterday_before_low",
            # "yesterday_before_close",
            # "today_ups",
            # "today_downs",
            # "today_highs",
            # "today_lows"
        ]

        return

    def run_druida_old_data(self) -> None:
        dd.arguments = self.arguments

        if len(self.matcher_objects) == 0:
            self.get_matcher_objects()

        self.data = dd.get_old_day_data()
        self.get_matches()

        return

    def run_druida_current_data(self) -> None:
        dd.arguments = self.arguments

        if len(self.matcher_objects) == 0:
            self.get_matcher_objects()

        self.data = dd.get_current_data()
        self.get_matches()

        return

    def get_matcher_objects(self) -> None:
        for matcher in self.matchers:
            self.matcher = matcher
            matcher_class_name = self.get_class_name_from_matcher()

            module = importlib.import_module(matcher)
            matcher_class = getattr(module, matcher_class_name)
            self.matcher_objects.append(matcher_class())

            print("==> "+matcher)
            print("==> "+matcher_class_name)

        return

    def get_class_name_from_matcher(self) -> str:
        class_name = ""
        last_letter = self.matcher[0]

        for num_matcher in range(len(self.matcher)):
            letter = self.matcher[num_matcher]
            if num_matcher == 0:
                letter = self.matcher[num_matcher].upper()
            if last_letter == "_":
                letter = self.matcher[num_matcher].upper()

            class_name += letter
            last_letter = self.matcher[num_matcher]
            class_name = class_name.replace("_", "")

        return class_name

    def get_matches(self) -> None:
        for data in self.data:
            for matcher in range(len(self.matcher_objects)):
                print("-- MATCHER: " + self.matcher_objects[matcher].name)
                print("-- MATCHER CLASS NAME: " + self.matcher_objects[matcher].class_name)

                self.matcher_objects[matcher].arguments = self.arguments
                self.matches = self.matcher_objects[matcher].get_matches(data)

        return

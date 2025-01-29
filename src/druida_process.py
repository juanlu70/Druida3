import importlib
import logging


class DruidaProcess:
    def __init__(self):
        self.arguments = None
        self.matcher_objects = list()
        self.matcher = None

        self.matchers = [
            "yesterday_open",
            "yesterday_high",
            "yesterday_low",
            "yesterday_close",
            "yesterday_before_open",
            "yesterday_before_high",
            "yesterday_before_low",
            "yesterday_before_close",
            "today_ups",
            "today_downs"
        ]

        return

    def set_arguments(self, arguments: dict) -> None:
        self.arguments = arguments

        return

    def get_matcher_objects(self) -> list:
        if len(self.matcher_objects) > 0:
            return self.matcher_objects

        for matcher in self.matchers:
            matcher_class_name = self.get_class_name_from_matcher(matcher)

            module = importlib.import_module(f"matchers.{matcher}")
            matcher_class = getattr(module, matcher_class_name)
            self.matcher_objects.append(matcher_class())

            line = ("ADDING MODULE ==> "+matcher+"."+matcher_class_name+"()")
            logging.info(line)
            print(line)

        return self.matcher_objects

    def get_class_name_from_matcher(self, matcher: str) -> str:
        class_name = ""
        last_letter = matcher[0]

        for num_matcher in range(len(matcher)):
            letter = matcher[num_matcher]
            if num_matcher == 0:
                letter = matcher[num_matcher].upper()
            if last_letter == "_":
                letter = matcher[num_matcher].upper()

            class_name += letter
            last_letter = matcher[num_matcher]
            class_name = class_name.replace("_", "")

        return class_name

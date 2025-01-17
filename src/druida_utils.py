import time


class DruidaUtils:
    def __init__(self):
        self.arguments = dict()
        self.ticker = None
        self.price = None
        self.head = 1
        self.matcher = None
        self.datehour = None
        self.multiplier = None

        return

    def set_arguments(self, arguments: dict) -> None:
        self.arguments = arguments

        return

    def set_ticker(self, ticker: str) -> None:
        self.ticker = ticker

        return

    def get_multiplier(self, price: float) -> int:
        self.ticker = self.arguments['ticker']
        self.price = price
        self.multiplier = 1

        if self.ticker == "EURUSD":
            self.multiplier = 10000
        if self.ticker == "SP500" \
                or self.ticker == "^GSPCMF" \
                or self.ticker == "BS-BTCUSD" \
                or self.ticker == "KR-XBTUSD" \
                or self.ticker == "XAUUSD":
            self.multiplier = 100

        finalprice = int(self.price) * int(self.multiplier)

        return finalprice

    def get_divider(self, price: float) -> float:
        self.ticker = self.arguments['ticker']
        self.price = price
        self.multiplier = 1

        if self.ticker == "EURUSD":
            self.multiplier = 10000
        if self.ticker == "SP500" \
                or self.ticker == "^GSPCMF" \
                or self.ticker == "BS-BTCUSD" \
                or self.ticker == "KR-XBTUSD" \
                or self.ticker == "XAUUSD":
            self.multiplier = 100

        finalprice = float(self.price / float(self.multiplier))

        return finalprice

    def get_row_multiplier(self, data: list) -> list:
        self.set_ticker(self.arguments['ticker'])
        self.multiplier = 100
        final_data = list()

        if self.ticker == "EURUSD":
            self.multiplier = 10000

        for row_data in data:
            final_data.append(self.dict_multiplier_conversion(row_data))

        return final_data

    def get_row_divider(self, data: list) -> list:
        self.set_ticker(self.arguments['ticker'])
        self.multiplier = 100
        final_data = list()

        if self.ticker == "EURUSD":
            self.multiplier = 10000

        for row_data in data:
            final_data.append(self.dict_multiplier_conversion(row_data))

        return final_data

    def dict_multiplier_conversion(self, data: dict) -> dict:
        for key in data.keys():
            if type(data[key]).__name__ == "float":
                data[key] = self.get_multiplier(data[key])

        return data

    def get_timestamp(self, datehour: str) -> int:
        newhour = ""

        if len(datehour) == 10:
            datehour = datehour + " 00:00:00"
        self.datehour = datehour

        tmp = self.datehour.split(" ")
        date = tmp[0]
        if len(tmp) > 1:
            if len(tmp[1]) < 8:
                newhour = "0" + tmp[1]
            else:
                newhour = tmp[1]

        tmpsecs = time.strptime(date + " " + newhour, "%Y-%m-%d %H:%M:%S")
        secs = time.mktime(tmpsecs)

        return int(secs)

    def get_day_seconds(self, datehour: str) -> int:
        if len(datehour) == 10:
            datehour = datehour + " 00:00:00"

        self.datehour = datehour
        tmp = self.datehour.split(" ")
        fecha = tmp[0]
        hour = tmp[1]
        if len(hour) < 8:
            hour = "0" + hour

        first_time = time.strptime(fecha + " 00:00:00", "%Y-%m-%d %H:%M:%S")
        first_seconds = int(time.mktime(first_time))
        last_time = time.strptime(fecha + " " + hour, "%Y-%m-%d %H:%M:%S")
        last_seconds = int(time.mktime(last_time))

        day_seconds = last_seconds - first_seconds

        return day_seconds

    def calculate_dates(self, orig_date: str, operation: int) -> list:
        tmpsecs = time.strptime(orig_date + " 00:00:00", "%Y-%m-%d %H:%M:%S")
        secs = time.mktime(tmpsecs)
        secs = secs + (86400 * operation)
        final_date = str(time.strftime("%Y-%m-%d", time.localtime(secs)))

        return [final_date, secs]

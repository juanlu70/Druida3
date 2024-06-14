import time


class DruidaUtils:
    def __init__(self):
        self.arguments = dict()
        self.ticker = None
        self.price = None
        self.head = 1
        self.matcher = None
        self.fechahora = None

        return

    def get_multiplier(self) -> int:
        self.ticker = self.arguments['ticker']
        finalprice = self.price
        multiplier = 1

        if self.ticker == "EURUSD":
            multiplier = 10000
        if self.ticker == "SP500" \
                or self.ticker == "^GSPCMF" \
                or self.ticker == "BS-BTCUSD" \
                or self.ticker == "KR-XBTUSD" \
                or self.ticker == "XAUUSD":
            multiplier = 100

        if self.head == 1:
            finalprice = int(self.price * multiplier)
        if self.head == -1:
            finalprice = float(self.price / float(multiplier))

        return finalprice

    def get_row_multiplier(self, data: list) -> list:
        self.ticker = self.arguments['ticker']
        multiplier = 1
        final_data = list()

        if self.ticker == "EURUSD":
            multiplier = 10000
        if self.ticker == "SP500" \
                or self.ticker == "^GSPCMF" \
                or self.ticker == "BS-BTCUSD" \
                or self.ticker == "KR-XBTUSD" \
                or self.ticker == "XAUUSD":
            multiplier = 100

        for row_data in data:
            final_data.append(self.dict_multiplier_conversion(row_data))

        return final_data

    def dict_multiplier_conversion(self, data: dict) -> dict:
        for key in data.keys():
            if type(data[key]).__name__ == "float":
                self.price = data[key]
                data[key] = self.get_multiplier()

        return data

    def get_day_seconds(self) -> int:
        hour = ""
        tmp = self.fechahora.split(" ")
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

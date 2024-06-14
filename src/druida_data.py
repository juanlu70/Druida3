import time

import db_mysql
import druida_utils


db = db_mysql.DbMysql()
utils = druida_utils.DruidaUtils()


class DruidaData:
    def __init__(self):
        self.arguments = dict()
        self.database = None
        self.table = "quotes1"
        self.last_id = 0
        self.ticker = None
        self.date = None

        return

    def get_old_day_data(self) -> list:
        self.date = self.arguments['date']
        self.ticker = self.arguments['ticker']
        db.date = self.date
        db.get_database()

        params = [
            "ticker='" + self.ticker + "'",
            "fecha='" + self.date + "'",
            "ORDER BY hora"
        ]
        data = list(db.select(params))

        if len(data) > 0:
            self.last_id = data[-1]['id']

        utils.arguments = self.arguments
        data = utils.get_row_multiplier(data)

        return data

    def get_current_data(self) -> list:
        self.date = self.arguments['date']
        self.ticker = self.arguments['ticker']
        db.date = self.date
        db.get_database()

        params = [
            "id>"+str(self.last_id),
            "ticker='" + self.ticker + "'",
            "fecha='" + self.date + "'",
            "ORDER BY hora"
        ]
        data = list(db.select(params))
        if len(data) > 0:
            self.last_id = data[-1]['id']

        return data

    # def get_table_database(self) -> None:
    #     db.date = self.date
    #     today = time.strftime("%Y-%m-%d", time.localtime())
    #
    #     if self.database is None:
    #         self.database = db.get_database()
    #     if self.date == today:
    #         self.table = "quoteday"
    #
    #     return

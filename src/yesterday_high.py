import db_mysql
import druida_utils
import druida_data


db = db_mysql.DbMysql()
utils = druida_utils.DruidaUtils()
dd = druida_data.DruidaData()


class YesterdayClose:
    def __init__(self):
        self.name = "yesterday_close"
        self.class_name = "YesterdayClose"
        self.arguments = dict()
        self.BASE_VALUE = None
        self.last_close = None
        self.today_seconds = None
        self.closediff = None
        self.pctclosediff = None
        self.matches = list()

        return

    def update_base_value(self) -> None:
        print(" --> GETTING LAST CLOSE!")
        db.date = self.arguments['date']
        db.get_database()

        params = [
            "ticker='" + self.arguments['ticker'] + "'",
            "fecha<'" + self.arguments['date'] + "'",
            "ORDER BY id DESC",
            "LIMIT 1 OFFSET 0"
        ]
        db_data = db.select(params)
        if len(db_data) > 0:
            self.last_close = db_data[0]['close']

            utils.arguments = self.arguments
            utils.price = self.last_close
            utils.head = 1
            final_number = utils.get_multiplier()

            self.BASE_VALUE = final_number

        return

    def get_matches(self, data: dict) -> None:
        print("DATA: "+str(data))
        matches = list()

        if self.BASE_VALUE is None:
            self.update_base_value()

        utils.fechahora = str(data['fecha']) + " " + str(data['hora'])
        self.today_seconds = utils.get_day_seconds()
        current_close = data['close']

        self.closediff = current_close - self.BASE_VALUE
        self.pctclosediff = int((current_close - self.BASE_VALUE) / self.BASE_VALUE) * 100

        matchs = self.make_closediff_query()
        if len(matchs) > 0:
            print(str(len(matchs)) + " match(es) on closediff")
            matches.extend(matchs)
        matchs = self.make_pctclosediff_query()
        if len(matchs) > 0:
            print(str(len(matchs)) + " match(es) on pctclosediff")
            matches.extend(matchs)
        print("---------------------------------")
        print("MATCHES: "+str(matches))
        print("=================================")

        return

    def make_closediff_query(self):
        db.database = "statsrt"
        db.table = "match_close"

        params = [
            "ticker='" + self.arguments['ticker'] + "'",
            "daysecs=" + str(self.today_seconds),
            "close_diff=" + str(self.closediff)
        ]
        matches = db.select(params)

        return matches

    def make_pctclosediff_query(self):
        db.database = "statsrt"
        db.table = "match_close"

        params = [
            "ticker='" + self.arguments['ticker'] + "'",
            "daysecs=" + str(self.today_seconds),
            "close_pct_diff=" + str(self.pctclosediff)
        ]
        matches = db.select(params)

        return matches

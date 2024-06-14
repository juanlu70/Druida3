import db_mysql
import druida_utils
import druida_data


db = db_mysql.DbMysql()
utils = druida_utils.DruidaUtils()
dd = druida_data.DruidaData()


class YesterdayOpen:
    def __init__(self):
        self.name = "yesterday_open"
        self.class_name = "YesterdayOpen"
        self.arguments = dict()
        self.BASE_VALUE = None
        self.last_open = None
        self.today_seconds = None
        self.opendiff = None
        self.pctopendiff = None
        self.matches = list()

        return

    def update_base_value(self) -> None:
        print(" --> GETTING LAST OPEN!")
        db.date = self.arguments['date']
        db.get_database()

        params = [
            "ticker='" + self.arguments['ticker'] + "'",
            "fecha<'" + self.arguments['date'] + "'",
            "ORDER BY id",
            "LIMIT 1 OFFSET 0"
        ]
        db_data = db.select(params)
        if len(db_data) > 0:
            self.last_open = db_data[0]['open']

            utils.arguments = self.arguments
            utils.price = self.last_open
            utils.head = 1
            final_number = utils.get_multiplier()

            self.BASE_VALUE = final_number
        print("BASE_VALUE: "+str(self.BASE_VALUE))

        return

    def get_matches(self, data: dict) -> None:
        print("DATA: "+str(data))
        matches = list()

        if self.BASE_VALUE is None:
            self.update_base_value()

        utils.fechahora = str(data['fecha']) + " " + str(data['hora'])
        self.today_seconds = utils.get_day_seconds()
        current_open = data['open']

        self.opendiff = current_open - self.BASE_VALUE
        self.pctopendiff = round(100.00 - ((current_open * 100.00) / self.BASE_VALUE), 2)

        open_diff_matchs = self.make_opendiff_query()
        if len(open_diff_matchs) > 0:
            print(str(len(open_diff_matchs)) + " match(es) on opendiff")
            matches.extend(open_diff_matchs)
        pct_open_diff_matchs = self.make_pctopendiff_query()
        if len(pct_open_diff_matchs) > 0:
            print(str(len(pct_open_diff_matchs)) + " match(es) on pctopendiff")
            matches.extend(pct_open_diff_matchs)
        print("---------------------------------")
        print("MATCHES: "+str(matches))
        print("=================================")

        return

    def make_opendiff_query(self) -> list:
        matches = list()
        db.database = "statsrt"
        db.table = "match_open"

        params = [
            "ticker='" + self.arguments['ticker'] + "'",
            "daysecs=" + str(self.today_seconds),
            "open_diff=" + str(self.opendiff)
        ]
        matches = db.select(params)

        return matches

    def make_pctopendiff_query(self):
        matches = list()
        db.database = "statsrt"
        db.table = "match_open"

        params = [
            "ticker='" + self.arguments['ticker'] + "'",
            "daysecs=" + str(self.today_seconds),
            "open_pct_diff=" + str(self.pctopendiff)
        ]
        matches = db.select(params)

        return matches

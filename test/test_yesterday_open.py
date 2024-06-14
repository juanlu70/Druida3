import time

import yesterday_open
import db_mysql
import druida_utils


yo = yesterday_open.YesterdayOpen()
db = db_mysql.DbMysql()
utils = druida_utils.DruidaUtils()


def test_update_base_value():
    ticker = 'KR-XBTUSD'
    today = time.strftime("%Y-%m-%d", time.localtime())

    yo.BASE_VALUE = None
    yo.arguments = {'ticker': ticker, 'date': today}
    yo.update_base_value()

    db.date = today
    db.get_database()
    params = [
        "ticker='" + ticker + "'",
        "fecha<'" + today + "'",
        "ORDER BY fecha, hora LIMIT 1 OFFSET 0"
    ]

    utils.arguments = yo.arguments
    utils.price = db.select(params)[0]['open']
    result = utils.get_multiplier()

    assert yo.BASE_VALUE == result


def test_get_matches():
    pass

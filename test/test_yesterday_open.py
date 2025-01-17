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

    db.get_bolsa_database(today)
    params = [
        "ticker='" + ticker + "'",
        "fecha<'" + today + "'",
        "ORDER BY fecha, hora LIMIT 1 OFFSET 0"
    ]

    utils.arguments = yo.arguments
    utils.price = db.select(params)[0]['open']
    result = utils.get_multiplier(utils.price)

    assert yo.BASE_VALUE == result


def test_get_opendiff_formula():
    ticker = 'KR-XBTUSD'
    fecha = "2024-06-21"
    secs = 1718921606

    yo.arguments = {'ticker': ticker, 'date': fecha}
    utils.arguments = yo.arguments
    yo.update_base_value()

    db.get_bolsa_database("2024-06-21")
    params = [
        "ticker='" + ticker + "'",
        "secs=" + str(secs)
    ]
    db_data = db.select(params)
    try:
        tmp = utils.get_multiplier(db_data[0]['open'])

        assert tmp == 6507840
    except IndexError:
        print("Error, no data gathered!")


def test_get_matches():
    pass

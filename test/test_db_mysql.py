import time

import db_mysql


db = db_mysql.DbMysql()


def test_select_database():
    db.set_database("bolsa")
    db.set_table("quotes1")

    assert db.database == "bolsa"
    assert db.table == "quotes1"

    return


def test_make_select():
    db.get_bolsa_database("2021-03-01")
    params = [
        "id=603787651",
        "ticker='KR-XBTUSD'"
    ]
    result = db.select(params)[0]
    assert result['open'] == 29104.7

    return


def test_make_insert():
    db.set_database("statsrt")
    db.set_table("tests")

    params = {
        'ticker': 'KR-XBTUSD',
        'fecha': "2024-08-21",
        'secs': 1724191200
    }
    db.insert(params)

    params = [
        "ticker='KR-XBTUSD'",
        "fecha='2024-08-21'"
    ]
    result = db.select(params)

    assert result[0]['secs'] == 1724191200

    db.delete(["ticker='KR-XBTUSD'"])

    return


def test_make_row_insert():
    db.set_database("statsrt")
    db.set_table("tests")

    params = [
        {
            'ticker': 'KR-XBTUSD',
            'fecha': '2024-08-21',
            'secs': 1724191200,
            'daysecs': 0
        },
        {
            'ticker': 'KR-XBTUSD',
            'fecha': '2024-08-21',
            'secs': 1724191201,
            'daysecs': 5
        }
    ]
    db.row_insert(params)

    params = [
        "ticker='KR-XBTUSD'",
        "fecha='2024-08-21'",
    ]
    result = db.select(params)

    assert len(result) == 2

    db.delete(["ticker='KR-XBTUSD'"])

    return


def test_make_delete():
    db.set_database("statsrt")
    db.set_table("tests")

    params = {
        'ticker': 'KR-XBTUSD',
        'fecha': '2024-08-21',
        'secs': 1724191200,
        'daysecs': 0
    }
    db.insert(params)

    db.delete(["ticker='KR-XBTUSD'"])

    params = ["ticker='KR-XBTUSD'"]
    result = db.select(params)

    assert len(result) == 0

    return


def test_make_update():
    db.set_database("statsrt")
    db.set_table("tests")

    params = {
        'ticker': 'KR-XBTUSD',
        'fecha': '2024-08-21',
        'secs': 1724191200,
        'daysecs': 0
    }
    db.insert(params)

    params1 = [
        "daysecs=5",
    ]
    params2 = ["ticker='KR-XBTUSD'"]
    db.update(params1, params2)

    params = ["ticker='KR-XBTUSD'"]
    result = db.select(params)

    assert result[0]['daysecs'] == 5

    db.delete(["ticker='KR-XBTUSD'"])

    return


def test_get_database():
    db.get_bolsa_database("2021-03-01")
    assert db.database == "bolsa20212025"

    return


def test_get_database_today():
    db.get_bolsa_database(time.strftime("%Y-%m-%d", time.localtime()))
    assert db.database == "bolsa"

    return

import time

import db_servers
import db_mysql


srv = db_servers.DbServers()
db = db_mysql.DbMysql()


def test_dbbolsa_opens():
    [sql, bbdd, current_db] = srv.db_bolsa()
    assert current_db == "bolsa"


def test_db_bolsa20212025():
    [sql, bbdd, current_db] = srv.db_bolsa20212025()
    assert current_db == "bolsa20212025"


def test_db_statsrt():
    [sql, bbdd, current_db] = srv.db_statsrt()
    assert current_db == "statsrt"


def test_make_select():
    db.date = "2021-03-01"
    db.get_database()
    params = [
        "id=603787651",
        "ticker='KR-XBTUSD'"
    ]
    result = db.select(params)[0]
    assert result['open'] == 29104.7

# def test_make_insert():
# def test_make_row_insert():
# def test_make_delete():
# def test_make_update():


def test_get_database():
    db.date = "2021-03-01"
    db.get_database()
    assert db.database == "bolsa20212025"


def test_get_database_today():
    db.date = time.strftime("%Y-%m-%d", time.localtime())
    db.get_database()
    assert db.database == "bolsa"


def test_get_secs():
    db.datehour = "2020-03-01 00:00:00"
    result = db.get_secs()
    assert result == 1583017200


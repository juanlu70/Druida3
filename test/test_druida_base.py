import pytest
import druida_base
import db_mysql
import druida_process


base = druida_base.DruidaBase()
db = db_mysql.DbMysql()
prcs = druida_process.DruidaProcess()


@pytest.fixture
def setup_arguments():
    arguments = {
        'ticker': "KR-XBTUSD",
        'date': "2024-08-01"
    }

    return arguments


@pytest.fixture
def setup_row() -> dict:
    data = {
        'id': 788301751,
        'ticker': "KR-XBTUSD",
        'fecha': "2024-08-21",
        'hora': "00:00:00",
        'secs': 1724191200,
        'open': 5928600,
        'high': 5928600,
        'low': 5928600,
        'close': 5928600,
        'vol': 60006,
        'voltotal': 60006,
        'daymax': 6139100,
        'daymin': 5857200,
        'ask': 1,
        'bid': 13,
        'precioask': 5928600,
        'preciobid': 5928600
    }

    return data


@pytest.fixture
def setup_data() -> list:
    data = [{
        'id': 788301751,
        'ticker': 'KR-XBTUSD',
        'fecha': "2024-08-21",
        'hora': "00:00:00",
        'secs': 1724191200,
        'open': 5928600,
        'high': 5928600,
        'low': 5928600,
        'close': 5928600,
        'vol': 60006,
        'voltotal': 60006,
        'daymax': 6139100,
        'daymin': 5857200,
        'ask': 1,
        'bid': 13,
        'precioask': 5928600,
        'preciobid': 5928600
    }, {
        'id': 788301752,
        'ticker': 'KR-XBTUSD',
        'fecha': "2024-08-21",
        'hora': "00:00:05",
        'secs': 1724191205,
        'open': 5928600,
        'high': 5928600,
        'low': 5928600,
        'close': 5928600,
        'vol': 60006,
        'voltotal': 60006,
        'daymax': 6139100,
        'daymin': 5857200,
        'ask': 1,
        'bid': 13,
        'precioask': 5928600,
        'preciobid': 5928600
    }, {
        'id': 788301753,
        'ticker': 'KR-XBTUSD',
        'fecha': "2024-08-21",
        'hora': "00:00:10",
        'secs': 1724191210,
        'open': 5928600,
        'high': 5928600,
        'low': 5928600,
        'close': 5928600,
        'vol': 60006,
        'voltotal': 60006,
        'daymax': 6139100,
        'daymin': 5857200,
        'ask': 30,
        'bid': 2,
        'precioask': 5928600,
        'preciobid': 5928600
    }]

    return data


def test_set_arguments(setup_arguments):
    base.set_arguments(setup_arguments)

    assert base.arguments['ticker'] == "KR-XBTUSD"

    return


def test_make_previous_db_clean(setup_arguments):
    base.set_arguments(setup_arguments)
    base.make_previous_db_clean()

    db.set_database("statsrt")
    db.set_table("newlevels")
    params = [
        "ticker='" + base.arguments['ticker'] + "'",
        "fecha='" + base.arguments['date'] + "'"
    ]
    result = db.select(params)

    assert len(result) == 0

    return


def test_add_synthesized_data(setup_arguments, setup_row):
    base.set_arguments(setup_arguments)
    base.set_row(setup_row)
    base.set_old_close(5929000)
    base.set_old_fecha("2024-08-23")

    base.add_synthesized_data()

    assert base.row['downs'] == 1

    return


def test_get_matches(setup_arguments, setup_data):
    base.set_arguments(setup_arguments)
    base.data = setup_data

    #print("MATCHES:")
    print(len(base.matches))
    base.get_matches()
    print("MATCHES:")
    print(len(base.matches))
    assert len(base.matches) == 3

    return

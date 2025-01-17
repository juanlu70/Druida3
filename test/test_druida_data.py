import pytest
import druida_data
import db_mysql


dd = druida_data.DruidaData()
db = db_mysql.DbMysql()


@pytest.fixture
def setup_arguments():
    arguments = {
        'ticker': "KR-XBTUSD",
        'date': "2024-08-01",
        'date_ini': "2024-08-01",
        'date_end': "2024-08-31",
    }

    return arguments


@pytest.fixture
def setup_arguments_alt():
    arguments = {
        'ticker': "KR-XBTUSD",
        'date': "2024-08-18",
        'date_ini': "2024-08-18",
        'date_end': "2024-08-18",
    }

    return arguments


@pytest.fixture
def setup_values(setup_arguments):
    values = {
        'today_seconds': 0,
        'arguments': setup_arguments,
        'diff_field': "close_diff",
        'pct_diff_field': "close_pct_diff",
        'table': "match_close",
        'class_name': "YesterdayClose"
    }

    return values


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


@pytest.fixture
def setup_train_data() -> list:
    train_data = [{
            'id': 470953870,
            'ticker': "KR-XBTUSD",
            'fecha': "2024-08-01",
            'secs': 1722463200,
            'daysecs': 0,
            'close_diff': 0,
            'close_pct_diff': 0,
            'maxtrend': 34700,
            'mintrend': -268600
        }, {
            'id': 470953871,
            'ticker': "KR-XBTUSD",
            'fecha': "2024-08-01",
            'secs': 1722463202,
            'daysecs': 2,
            'close_diff': 0,
            'close_pct_diff': 0,
            'maxtrend': 34700,
            'mintrend': -268600
        },{
            'id': 470953872,
            'ticker': "KR-XBTUSD",
            'fecha': "2024-08-01",
            'secs': 1722463205,
            'daysecs': 5,
            'close_diff': 0,
            'close_pct_diff': 0,
            'maxtrend': 34700,
            'mintrend': -268600
        }]

    return train_data


@pytest.fixture
def setup_levels() -> list:
    levels = [
        {
            'ticker': "KR-XBTUSD",
            'fecha': "2024-08-01",
            'highlow': 1,
            'level': 5929600,
            'num': 3769,
            'secs': 1723932001,
            'burned': 0
        }, {
            'ticker': "KR-XBTUSD",
            'fecha': "2024-08-01",
            'highlow': -1,
            'level': 5929500,
            'num': 36,
            'secs': 1723932001,
            'burned': 0
        }, {
            'ticker': "KR-XBTUSD",
            'fecha': "2024-08-01",
            'highlow': -1,
            'level': 5929200,
            'num': 20,
            'secs': 1723932001,
            'burned': 0
        }
    ]

    return levels


def test_set_arguments(setup_arguments):
    dd.set_arguments(setup_arguments)
    assert dd.arguments['ticker'] == "KR-XBTUSD"
    return


def test_set_close():
    dd.set_close(5928600)
    assert dd.close == 5928600
    return


def test_set_seconds():
    dd.set_seconds(1724191200)
    assert dd.seconds == 1724191200
    return


def test_set_old_close():
    dd.set_old_close(5928600)
    assert dd.old_close == 5928600
    return


def test_clean_newlevels(setup_arguments):
    db.set_database("statsrt")
    db.set_table("newlevels")
    dd.set_arguments(setup_arguments)
    dd.clean_newlevels()

    params = [
        "ticker='" + dd.arguments['ticker'] + "'",
        "fecha='" + dd.arguments['date'] + "'"
    ]
    result = db.select(params)
    assert len(result) == 0

    return


def test_set_matches_values(setup_arguments, setup_values):
    dd.set_arguments(setup_arguments)
    dd.set_matches_values(setup_values)

    assert dd.diff_field == "close_diff"

    return


def test_get_old_day_data(setup_arguments):
    dd.set_arguments(setup_arguments)
    result = dd.get_old_day_data()

    assert str(result[0]['fecha']) == "2024-08-01"
    assert str(result[-1]['fecha']) == "2024-08-01"

    return


def test_get_current_data(setup_arguments):
    dd.set_arguments(setup_arguments)
    dd.last_id = 835903972
    result = dd.get_current_data()

    assert len(result) == 0

    return


def test_get_distinct_dates(setup_arguments):
    dd.set_arguments(setup_arguments)
    result = dd.get_distinct_dates()

    assert len(result) == 31

    return


def test_get_day_data_from_date(setup_arguments):
    dd.set_arguments(setup_arguments)
    result = dd.get_day_data_from_date("2024-08-01")
    assert len(result) == 12675

    return


#def test_get_maxtrend(setup_arguments, setup_data):
#def test_get_mintrend(setup_arguments, setup_data):


def test_make_diff_query(setup_arguments):
    dd.set_arguments(setup_arguments)
    result = dd.make_diff_query(-1400)

    assert len(result) == 5

    return


def test_make_pctdiff_query(setup_arguments):
    dd.set_arguments(setup_arguments)
    result = dd.make_pctdiff_query(-0.02)

    assert len(result) == 59

    return


def test_delete_current_day(setup_arguments, setup_data):
    dd.set_arguments(setup_arguments)
    dd.table = "tests"
    dd.delete_current_day("2024-08-01")

    params = [
        "ticker='" + dd.arguments['ticker'] + "'",
        "fecha='" + dd.arguments['date'] + "'"
    ]
    result = db.select(params)

    assert len(result) == 0

    return


def test_save_match_day(setup_arguments, setup_train_data):
    dd.set_arguments(setup_arguments)
    dd.table = "tests"
    dd.save_match_day(setup_train_data)

    db.set_database("statsrt")
    db.set_table("tests")
    params = [
        "ticker='" + dd.arguments['ticker'] + "'",
        "fecha='" + dd.arguments['date'] + "'"
    ]
    result = db.select(params)

    assert len(result) == 3

    return


def test_check_levels(setup_arguments_alt):
    dd.set_arguments(setup_arguments_alt)
    result = dd.check_levels(5929600)

    assert result is True

    return


def test_insert_new_level(setup_arguments):
    dd.set_arguments(setup_arguments)
    dd.insert_new_level({
        'ticker': "KR-XBTUSD",
        'fecha': "2024-08-01",
        'highlow': 1,
        'level': 5929600,
        'num': 1,
        'secs': 1722463200,
        'burned': 0
    })

    result = dd.check_levels(5929600)
    assert result is True

    db.set_database("statsrt")
    db.set_table("newlevels")
    params = [
        "ticker='" + dd.arguments['ticker'] + "'",
        "fecha='" + dd.arguments['date'] + "'"
    ]
    db.delete(params)

    return


def test_get_level(setup_arguments_alt):
    dd.set_arguments(setup_arguments_alt)
    result = dd.get_level(5929600)

    assert result['secs'] == 1723932001

    return


def test_update_level_num(setup_arguments_alt):
    dd.set_arguments(setup_arguments_alt)

    data = {
        'num': 3770,
        'id': 2806
    }
    dd.update_level_num(data)

    db.set_database("statsrt")
    db.set_table("newlevels")
    params = [
        "id=2806"
    ]
    result = db.select(params)
    assert result[0]['num'] == 3770

    params1 = ["num=3769"]
    params2 = ["id=2806"]
    db.update(params1, params2)

    return


def test_get_newlevel_highs(setup_arguments_alt):
    dd.set_arguments(setup_arguments_alt)
    dd.set_close(5928900)
    result = dd.get_newlevel_highs()

    assert len(result) == 21

    return


def test_get_newlevel_lows(setup_arguments_alt):
    dd.set_arguments(setup_arguments_alt)
    dd.set_close(5928900)
    result = dd.get_newlevel_lows()

    assert len(result) == 28

    return


def test_burn_last_levels(setup_arguments):
    dd.set_arguments(setup_arguments)
    dd.burn_last_levels()

    db.set_database("statsrt")
    db.set_table("newlevels")
    params = [
        "ticker='" + dd.arguments['ticker'] + "'",
        "fecha='" + dd.arguments['date'] + "'",
        "burned=0"
    ]
    result = db.select(params)
    assert len(result) == 0

    return


def test_burn_hour_ago_levels(setup_arguments, setup_levels):
    dd.set_arguments(setup_arguments)

    db.set_database("statsrt")
    db.set_table("newlevels")
    db.row_insert(setup_levels)

    dd.burn_hour_ago_levels()

    params = [
        "ticker='" + dd.arguments['ticker'] + "'",
        "fecha='" + dd.arguments['date'] + "'",
        "burned=1"
    ]
    result = db.select(params)
    assert len(result) == 3

    params = ["burned=1"]
    db.delete(params)

    return

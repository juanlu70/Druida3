import druida_utils


utils = druida_utils.DruidaUtils()


def test_get_multiplier():
    utils.arguments = {'ticker': "KR-XBTUSD"}

    result = utils.get_multiplier(25000.35)
    assert result == 2500035

    return


def test_get_divider():
    utils.arguments = {'ticker': "KR-XBTUSD"}

    result = utils.get_divider(2500035)
    assert result == 25000.35

    return


def test_get_day_seconds():
    result = utils.get_day_seconds("2020-03-01 10:30:00")
    assert result == 37800

    return


def test_get_secs():
    result = utils.get_timestamp("2020-03-01 00:00:00")
    assert result == 1583017200

    return


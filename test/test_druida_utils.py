import druida_utils


utils = druida_utils.DruidaUtils()


def test_get_multiplier():
    utils.arguments = {'ticker': "KR-XBTUSD"}
    utils.price = 25000.35

    result = utils.get_multiplier()
    assert result == 2500035

    return


def test_get_day_seconds():
    utils.fechahora = "2020-03-01 10:30:00"

    result = utils.get_day_seconds()
    assert result == 37800

    return

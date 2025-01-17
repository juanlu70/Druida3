import training_base


tb = training_base.TrainingBase()


def test_get_different_dates():
    tb.arguments = {
        'ticker': "KR-XBTUSD",
        'date_ini': "2021-03-01",
        'date_end': "2021-03-07"
    }

    tb.get_different_dates()
    assert len(tb.dates) == 7

    return


def test_get_day_data_from_date():
    tb.arguments = {
        'ticker': "KR-XBTUSD",
        'date_ini': "2021-03-01",
        'date_end': "2021-03-07"
    }
    tb.date = "2021-03-01"

    data = tb.get_day_data_from_date()

    assert data[5]['open'] == 4525420

    return

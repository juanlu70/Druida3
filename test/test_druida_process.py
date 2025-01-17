import druida_process


pr = druida_process.DruidaProcess()


def test_get_matcher_objects():
    pr.get_matcher_objects()
    class_name = pr.matcher_objects[0].class_name

    assert class_name == "YesterdayOpen"

    return


def test_get_class_name_from_matcher():
    pr.arguments = {'ticker': 'KR_XBTUSD', 'date': '2020-03-01'}
    pr.matchers = ["yesterday_open"]
    pr.get_matcher_objects()

    result = pr.get_class_name_from_matcher("yesterday_open")
    assert result == "YesterdayOpen"

    return


import druida_base


druidab = druida_base.DruidaBase()


def test_get_class_name_from_matcher():
    druidab.matcher = "yesterday_open"

    result = druidab.get_class_name_from_matcher()
    assert result == "YesterdayOpen"

    return


def test_get_matcher_objects():
    druidab.get_matcher_objects()
    class_name = druidab.matcher_objects[1].class_name

    assert class_name == "YesterdayClose"

    return


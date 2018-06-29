import ftw


def test_convert_to_seconds():
    assert ftw.human.convert_to_seconds('5s') == 5
    assert ftw.human.convert_to_seconds('10m') == 600

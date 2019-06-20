from helper import clear_float_zero


def test_clear_float_zero():

    test_cases = [
        '0', '10', '100', '101', '1.0', '1.00', '1.05', '1.50'
    ]

    expected_results = [
        '0', '10', '100', '101', '1', '1', '1.05', '1.5'
    ]

    for t, e in zip(test_cases, expected_results):
        assert clear_float_zero(t) == e

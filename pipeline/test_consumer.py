# pylint: skip-file
# from unittest.mock import patch, MagicMock
from consumer import valid_message


def test_valid_message():
    test_message = {'at': '2024-05-06T10:47:06.086606+01:00',
                    'site': '3', 'val': 1}
    test_message_type = {'at': '2024-05-06T10:47:06.086606+01:00',
                         'site': '3', 'val': -1, 'type': 1}
    assert valid_message(test_message) == test_message
    assert valid_message(test_message_type) == test_message_type


def test_missing_key_message():
    test_message = {'site': '3', 'val': 1}
    assert valid_message(test_message) == "INVALID: missing at values"


def test_incorrect_key_values():
    test_message = {'at': '2024-05-06T10:47:06.086606+01:00',
                    'site': '3', 'val': 'ERR'}
    assert valid_message(test_message) == "INVALID: val values are invalid"


def test_type_key_message():
    test_message = {'at': '2024-05-06T10:47:06.086606+01:00',
                    'site': '3', 'val': -1}
    test_message_type_value = {'at': '2024-05-06T10:47:06.086606+01:00',
                               'site': '3', 'val': -1, 'type': 3}
    assert valid_message(test_message) == "INVALID: missing type values"
    assert valid_message(
        test_message_type_value) == "INVALID: type values are invalid"

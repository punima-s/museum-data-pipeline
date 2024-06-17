# pylint: skip-file
from unittest.mock import patch, MagicMock
from extract import get_objects, clear_folder, download_hist_files, download_exhibit_files, combining_csv_files
import pytest


@pytest.fixture
def test_object():
    return [{'Key': "lmnh_exhibition_Object1.json",
             'Notes': "test1"},
            {'Key': "lmnh_hist_data_Object2.csv",
             'Notes': "test2"},
            {'Key': "lmnh_exhibition_Object3.json",
             'Notes': "test3"},
            {'Key': "lmnh_hist_data_Object4.csv",
             'Notes': "test4"}]


def test_get_objects():
    fake_client = MagicMock()
    fake_client.list_objects.return_value = {
        'Contents': [{'Key': 'ObjectName'}],
        'NotContents': [{'Key': 'NonObjects'}]}

    result = get_objects(fake_client, "Anything")

    assert fake_client.list_objects.call_count == 1
    assert result == [{'Key': 'ObjectName'}]


def test_download_hist_files(test_object):
    fake_client = MagicMock()
    fake_client.download_file.return_value = None
    result = download_hist_files(fake_client, "Anything", test_object)

    assert fake_client.download_file.call_count == 2
    assert len(result) == 2
    assert result == ["lmnh_hist_data_Object2.csv",
                      "lmnh_hist_data_Object4.csv"]


def test_download_exhibit_files(test_object):
    fake_client = MagicMock()
    fake_client.download_file.return_value = None
    result = download_exhibit_files(fake_client, "Anything", test_object)

    assert fake_client.download_file.call_count == 2
    assert len(result) == 2
    assert result == ["lmnh_exhibition_Object1.json",
                      "lmnh_exhibition_Object3.json"]


@patch("extract.os")
def test_clear_folder(fake_os):
    test_folder_path = "fake_folder"
    fake_os.listdir.return_value = ['file.csv', 'file2.json', 'tests.py']
    fake_os.remove.return_value = None
    clear_folder(test_folder_path)
    assert fake_os.remove.call_count == 3


@patch("extract.csv")
@patch("extract.os")
def test_combining_csv_files(fake_os, fake_csv):
    fake_os.listdir.return_value = ['file.csv', 'file2.csv', 'tests.csv']
    test_folder_path = "fake_folder"
    result = combining_csv_files(test_folder_path)
    assert fake_os.path.join.call_count == 3

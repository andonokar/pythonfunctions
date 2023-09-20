import pandas as pd
from globalresources.dataframe_reader import read_dataframe
from tests.variables_test import excel_wrong_conf, excel_conf, csv_conf, csv_wrong_conf
import pytest

CSVFILE = "tests/dummy_files/addresses.csv"
EXCELFILE = "tests/dummy_files/FinancialSample.xlsx"
PDFFILE = "tests/dummy_files/testpdf.pdf"


def test_no_excel_csv_reading():
    with pytest.raises(NotImplementedError):
        read_dataframe(PDFFILE, '/dummy_files/testpdf.pdf', dict())


def test_read_excel_no_conf():
    with pytest.raises(KeyError):
        read_dataframe(EXCELFILE, EXCELFILE, dict())


def test_read_excel_with_conf_no_header():
    with pytest.raises(KeyError):
        read_dataframe(EXCELFILE, EXCELFILE, excel_wrong_conf)


def test_read_excel():
    obj = read_dataframe(EXCELFILE, EXCELFILE, excel_conf)
    assert obj.name == 'FinancialSample.xlsx'
    assert obj.config == excel_conf
    assert isinstance(obj.df, pd.DataFrame)


def test_read_csv_no_conf():
    with pytest.raises(KeyError):
        read_dataframe(CSVFILE, CSVFILE, dict())


def test_read_csv_with_missing_keys():
    with pytest.raises(KeyError):
        read_dataframe(CSVFILE, CSVFILE, csv_wrong_conf)


def test_read_csv():
    obj = read_dataframe(CSVFILE, CSVFILE, csv_conf)
    assert obj.name == 'addresses.csv'
    assert obj.config == csv_conf
    assert isinstance(obj.df, pd.DataFrame)

from tests.variables_test import pdf_index, pdf_regex, pdf_normal_tdc, COLUMNS


def test_regex_rename():
    df = pdf_regex.rename_dataframe()
    assert all(column in COLUMNS for column in df.columns)


def test_normal_rename():
    df = pdf_normal_tdc.rename_dataframe()
    assert all(column in COLUMNS for column in df.columns)


def test_index_rename():
    df = pdf_index.rename_dataframe()
    assert all(column in COLUMNS for column in df.columns)


def test_no_rename():
    df1 = pdf_index.df.copy()
    df = pdf_index.rename_dataframe()
    assert list(df1.columns) == list(df.columns)

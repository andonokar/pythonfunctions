def convert_boolean(val):
    """
    Trata os decimais no caso de não estarem formatados corretamente
    """
    if val:
        if val in [0, '0', 'false', 'False', 'FALSE', False]:
            return False
        if val in [1, '1', 'true', "True", "TRUE", True]:
            return True
    return None


def convert_decimal(val):
    """
    Trata os decimais no caso de não estarem como float
    """
    if ',' in str(val):
        try:
            return float(str(val).replace('.', '').replace(',', '.'))
        except:
            return 0
    else:
        try:
            return float(val)
        except:
            return 0


def rename_columns(df, column_renames, logger):
    df = df.rename(columns=column_renames)
    return df


def regex_rename_columns(df, column_renames, logger):
    try:
        columns = {i: j for i, j in zip([df.filter(regex=name).columns.tolist()[0] for name in column_renames.keys()],
                                        column_renames.values())}
    except IndexError:
        logger.error('um ou mais regex nao resgataram uma coluna para captura')
        raise NameError('um ou mais regex nao resgataram uma coluna para captura')
    df = df.rename(columns=columns)
    return df


def index_rename_columns(df, column_renames, logger):
    try:
        columns = {df.columns.tolist()[int(i)]: j for i, j in column_renames.items()}
    except IndexError:
        logger.error('o indice da coluna nao existe')
        raise IndexError('o indice da coluna nao existe')
    df = df.rename(columns=columns)
    return df

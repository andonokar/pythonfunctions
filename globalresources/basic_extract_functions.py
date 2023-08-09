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
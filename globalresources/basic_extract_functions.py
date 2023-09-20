def convert_boolean(val) -> bool | None:
    """
    Trata os decimais no caso de não estarem formatados corretamente
    """
    if val is not None:
        if val in [0, '0', 'false', 'False', 'FALSE', False]:
            return False
        if val in [1, '1', 'true', "True", "TRUE", True]:
            return True
    return None


def convert_decimal(val) -> float:
    """
    Trata os decimais no caso de não estarem como float
    """
    if not val:
        return 0
    if ',' in str(val):
        try:
            return float(str(val).replace('.', '').replace(',', '.'))
        except ValueError:
            return 0
    try:
        return float(val)
    except ValueError:
        return 0

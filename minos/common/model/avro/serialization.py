import decimal


def prepare_bytes_decimal(data: decimal.Decimal, precision: int, scale: int = 0) -> bytes:
    """Convert decimal.Decimal to bytes"""

    sign, digits, exp = data.as_tuple()

    if len(digits) > precision:
        raise ValueError("The decimal precision is bigger than allowed by schema")

    delta = exp + scale

    if delta < 0:
        raise ValueError("Scale provided in schema does not match the decimal")

    unscaled_datum = 0
    for digit in digits:
        unscaled_datum = (unscaled_datum * 10) + digit

    unscaled_datum = 10 ** delta * unscaled_datum

    bytes_req = (unscaled_datum.bit_length() + 8) // 8

    if sign:
        unscaled_datum = -unscaled_datum

    return unscaled_datum.to_bytes(bytes_req, byteorder="big", signed=True)


def decimal_to_str(value: decimal.Decimal, precision: int, scale: int = 0) -> str:
    value_bytes = prepare_bytes_decimal(value, precision, scale)
    return r"\u" + value_bytes.hex()

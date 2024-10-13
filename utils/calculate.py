import math


def nandiv(numerator, denominator):
    if numerator is None or denominator is None:
        return None
    else:
        return numerator / (denominator + 1e-10)    # 0으로 나누는 것을 방지


def calc_growth_rate(value_base, value_before):
    """
    value_before 값과 비교하여 value_base 값의 증가 비율을 계산합니다.

    Args:
        value_base: 기준 값입니다.
        value_before: 비교 대상 값입니다.

    Returns:
        증가 비율입니다.
    """
    if value_base is None or value_before is None:
        return None
    else:
        return (value_base - value_before) / (value_before + 1e-10)    # 0으로 나누는 것을 방지
    

def calc_error_rate(true_value, estimated_value):
    return abs(true_value - estimated_value) / (true_value + 1e-10)
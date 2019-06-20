"""一些通用函数。"""


def clear_float_zero(n: str) -> str:
    """去除小数字符串结尾的 0"""

    if n.find('.') != -1:
        n = n.rstrip('0')
        if n.endswith('.'):
            n = n[:-1]

    return n

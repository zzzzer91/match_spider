"""本库的配置。

Create:   2018-9-29
Modified: 2018-10-1
"""

import os

__all__ = [
    'LIB_ROOT'
]

# 此库所在目录
LIB_ROOT: str = os.path.dirname(__file__)


def test() -> None:
    print(LIB_ROOT)


if __name__ == '__main__':
    test()

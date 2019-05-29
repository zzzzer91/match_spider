"""

create:   2018-12-12
modified:
"""

from queue import Queue
from typing import Tuple, Dict, FrozenSet, Optional, Union, Pattern, List, Iterator, Type

__all__ = [
    'Queue',
    'Tuple', 'List', 'Dict', 'FrozenSet', 'Type', 'Optional',
    'Iterator', 'Union', 'Pattern',
    'ProductDetailDict',
    'MysqlConfig', 'RedisConfig'
]

MysqlConfig = Dict[str, Union[str, int]]
RedisConfig = Dict[str, Union[str, int]]
ProductDetailDict = Dict[str, Union[str, int, None]]

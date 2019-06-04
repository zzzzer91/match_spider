"""配置文件"""

from crash import log

# 日志级别
LOG_LEVEL = log.DEBUG
del log

# 使用线程数量
THREAD_NUM = 1

# MySQL 配置
MYSQL_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'yousonofabitch',
    'db': 'matches',
}

# 保存数据的表
MYSQL_TABLE_BETFAIR = 'betfair'
MYSQL_TABLE_BETFAIR_DETAIL = 'betfair_detail'
MYSQL_TABLE_FOOTBALL_MATCH_SCHEDULE = 'football_match_schedule'
MYSQL_TABLE_FOOTBALL_MATCH = 'football_match'
MYSQL_TABLE_BASKETBALL_MATCH_SCHEDULE = 'basketball_match_schedule'
MYSQL_TABLE_BASKETBALL_MATCH = 'basketball_match'

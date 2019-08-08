"""配置文件"""

from crash.log import DEBUG, INFO, WARNING, ERROR

# 日志级别
LOG_LEVEL = DEBUG

# 使用线程数量，不改
THREAD_NUM = 1

# MySQL 配置
MYSQL_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': '123456',
    'db': 'topicBet',
}

# 保存数据的表
MYSQL_TABLE_BETFAIR = 'betfair'
MYSQL_TABLE_BETFAIR_DETAIL = 'betfair_detail'
MYSQL_TABLE_FOOTBALL_MATCH_SCHEDULE = 'football_match_schedule'
MYSQL_TABLE_FOOTBALL_MATCH = 'football_match'
MYSQL_TABLE_FOOTBALL_BET = 'football_bet'
MYSQL_TABLE_BASKETBALL_MATCH_SCHEDULE = 'basketball_match_schedule'
MYSQL_TABLE_BASKETBALL_MATCH = 'basketball_match'
MYSQL_TABLE_BASKETBALL_BET = 'basketball_bet'

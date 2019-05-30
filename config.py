"""配置文件"""

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

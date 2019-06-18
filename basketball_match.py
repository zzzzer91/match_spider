"""篮球即时比分，只要 NBA 和 CBA 的，亚盘数据来源选澳门

create:   2019-06-02
modified:
"""

import warnings
import datetime

from crash import spider, log
from crash.types import *
from basketball_bet import BasketballBetSpider

from config import *

# 将警告提升为异常，若字段类型不合法，pymysql 只会发出警告
# 提升为异常，可以过滤掉不符合字段类型的数据
warnings.filterwarnings('error')

log.logger.set_log_level(LOG_LEVEL)


class BasketballMatchSpider(BasketballBetSpider):

    def __init__(self,
                 name: str,
                 mysql_config: MysqlConfig) -> None:

        super().__init__(name, mysql_config)

    def run(self) -> None:

        today_format = datetime.date.today().strftime('%Y%m%d')
        url = self.url_temp
        r = self.session.get(url)

        for item in self.parse(r.text, today_format):
            log.logger.debug(item)
            self.insert_or_update(
                MYSQL_TABLE_BASKETBALL_MATCH,
                item,
                self.UPDATE_FIELD
            )


def main() -> None:

    spider.run_spider(
        1,
        BasketballMatchSpider,
        MYSQL_CONFIG
    )


if __name__ == '__main__':
    main()

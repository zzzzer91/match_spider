"""与篮球即时比分相比，比赛开始了，这张表的赔率不更新，与 basketball_match 适用不同场景的。

create:   2019-06-16
modified:
"""

import warnings
import datetime

from crash import spider, log
from crash.types import *
from basketball_match import BasketballMatchSpider

from config import *

# 将警告提升为异常，若字段类型不合法，pymysql 只会发出警告
# 提升为异常，可以过滤掉不符合字段类型的数据
warnings.filterwarnings('error')

log.logger.set_log_level(LOG_LEVEL)


class BasketballBetSpider(BasketballMatchSpider):

    url_temp = 'https://basket.13322.com/jsbf.html'

    # sql 插入已存在主键纪录时，更新如下字段
    AFTER_MATCH_START_UPDATE_FIELD = {
        'compete_time',
        'home_quarter_one',
        'visitor_quarter_one',
        'home_quarter_two',
        'visitor_quarter_two',
        'home_quarter_three',
        'visitor_quarter_three',
        'home_quarter_four',
        'visitor_quarter_four',
        'home_total_score',
        'visitor_total_score',
    }

    def __init__(self,
                 name: str,
                 mysql_config: MysqlConfig,
                 table_save:  str) -> None:

        super().__init__(name, mysql_config, table_save)

    def run(self) -> None:

        today_format = datetime.date.today().strftime('%Y%m%d')
        url = self.url_temp
        r = self.session.get(url)

        for item in self.parse(r.text, today_format):
            item.pop('home_rank')
            item.pop('visitor_rank')
            log.logger.debug(item)
            if item['compete_time'] == '未赛':
                update_field = self.UPDATE_FIELD
            else:
                update_field = self.AFTER_MATCH_START_UPDATE_FIELD
            self.insert_or_update(item, update_field)


def main() -> None:

    spider.run_spider(
        1,
        BasketballBetSpider,
        MYSQL_CONFIG,
        MYSQL_TABLE_BASKETBALL_BET
    )


if __name__ == '__main__':
    main()

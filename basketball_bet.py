"""与篮球即时比分相比，比赛开始了，这张表的赔率不更新，与 basketball_match 适用不同场景的。

create:   2019-06-16
modified:
"""

import warnings
import datetime

from lxml import etree

from crash import spider, log
from crash.types import *
from basketball_match_schedule import BasketballMatchScheduleSpider

from config import *

# 将警告提升为异常，若字段类型不合法，pymysql 只会发出警告
# 提升为异常，可以过滤掉不符合字段类型的数据
warnings.filterwarnings('error')

log.logger.set_log_level(LOG_LEVEL)


class BasketballBetSpider(BasketballMatchScheduleSpider):

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

    # sql 插入已存在主键纪录时，更新如下字段
    UPDATE_FIELD = {
        *BasketballMatchScheduleSpider.UPDATE_FIELD,
        *AFTER_MATCH_START_UPDATE_FIELD
    }

    def __init__(self,
                 name: str,
                 mysql_config: MysqlConfig) -> None:

        super().__init__(name, mysql_config)

    def run(self) -> None:

        today_format = datetime.date.today().strftime('%Y%m%d')
        url = self.url_temp
        r = self.session.get(url)

        for item in self.parse(r.text, today_format):
            item.pop('home_rank')
            item.pop('visitor_rank')

            if item['compete_time'] == '未赛':
                update_field = self.UPDATE_FIELD
            else:
                update_field = self.AFTER_MATCH_START_UPDATE_FIELD

            log.logger.debug(item)

            self.insert_or_update(
                MYSQL_TABLE_BASKETBALL_BET,
                item,
                update_field
            )

    @classmethod
    def parse(cls, html: str, date_format: str) -> Iterator[Dict]:

        selector = etree.HTML(html)
        table_all_element = selector.xpath('.//div[@class="table-all"]')[0]

        for item in cls._parse(table_all_element, date_format):
            yield item


def main() -> None:

    spider.run_spider(
        1,
        BasketballBetSpider,
        MYSQL_CONFIG
    )


if __name__ == '__main__':
    main()

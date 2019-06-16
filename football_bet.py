"""与足球即时比分相比，比赛开始了，这张表的赔率不更新，与 football_match 适用不同场景的。

create:   2019-06-16
modified:
"""

import warnings
import datetime

from crash import spider, log
from crash.types import *
from football_match import FootballMatchSpider

from config import *

# 将警告提升为异常，若字段类型不合法，pymysql 只会发出警告
# 提升为异常，可以过滤掉不符合字段类型的数据
warnings.filterwarnings('error')

log.logger.set_log_level(LOG_LEVEL)


class FootballBetSpider(FootballMatchSpider):

    # sql 插入已存在主键纪录时，更新如下字段
    # 比赛开始后，赔率相关不更新
    AFTER_MATCH_START_UPDATE_FIELD = {
        'compete_time',
        'home_half_score',
        'visitor_half_score',
    }

    def __init__(self,
                 name: str,
                 mysql_config: MysqlConfig,
                 table_save:  str) -> None:

        super().__init__(name, mysql_config, table_save)

    def run(self) -> None:
        today = datetime.datetime.today()
        current_hour = today.hour
        if current_hour < 12:
            today -= datetime.timedelta(1)

        self.fetch(today)

        if 12 <= current_hour < 13:
            self.fetch(today - datetime.timedelta(1))

    def fetch(self, date: datetime.datetime) -> None:

        url = self.url_temp.format(date.strftime('%Y-%m-%d'))
        r = self.session.get(url)
        jd = r.json()

        for item in self.parse(jd, date.strftime('%Y%m%d')):
            item.pop('home_rank')
            item.pop('visitor_rank')
            item.pop('home_corner_kick')
            item.pop('visitor_corner_kick')
            item.pop('home_half_score')
            item.pop('visitor_half_score')
            item.pop('home_yellow_card')
            item.pop('visitor_yellow_card')
            item.pop('home_red_card')
            item.pop('visitor_red_card')
            log.logger.debug(item)
            if item['compete_time'] in self.MATCH_NOT_START_FLAG:
                update_field = self.UPDATE_FIELD
            else:
                update_field = self.AFTER_MATCH_START_UPDATE_FIELD

            self.insert_or_update(item, update_field)


def main() -> None:

    spider.run_spider(
        1,
        FootballBetSpider,
        MYSQL_CONFIG,
        MYSQL_TABLE_FOOTBALL_BET
    )


if __name__ == '__main__':
    main()

"""足球即时比分

create:   2019-05-31
modified:
"""

import warnings
import datetime

from crash import spider, log
from crash.types import *
from football_bet import FootballBetSpider

from config import *

# 将警告提升为异常，若字段类型不合法，pymysql 只会发出警告
# 提升为异常，可以过滤掉不符合字段类型的数据
warnings.filterwarnings('error')

log.logger.set_log_level(LOG_LEVEL)


class FootballMatchSpider(FootballBetSpider):

    # sql 插入已存在主键纪录时，更新如下字段
    UPDATE_FIELD = {
        *FootballBetSpider.UPDATE_FIELD,

        'home_corner_kick',
        'visitor_corner_kick',

        'home_half_score',
        'visitor_half_score',

        'home_yellow_card',
        'visitor_yellow_card',
        'home_red_card',
        'visitor_red_card'
    }

    def __init__(self,
                 name: str,
                 mysql_config: MysqlConfig) -> None:

        super().__init__(name, mysql_config)

    def fetch(self, date: datetime.datetime) -> None:

        url = self.url_temp.format(date.strftime('%Y-%m-%d'))
        r = self.session.get(url)
        jd = r.json()

        for item in self.parse(jd, date.strftime('%Y%m%d')):

            if item['compete_time'] in self.MATCH_NOT_START_FLAG:
                temp = self.get_current_odds(item['remote_id'])
                item.update(temp)

            log.logger.debug(item)
            self.insert_or_update(
                MYSQL_TABLE_FOOTBALL_MATCH,
                item,
                self.UPDATE_FIELD
            )

    @classmethod
    def parse(cls, jd: Dict, date_format: str) -> Iterator[Dict]:

        matches = jd['matches']

        for item, match in zip(super().parse(jd, date_format), matches):

            compete_time = item['compete_time']

            # 比分，角球
            home_corner_kick = cls._compute_kick_or_score(compete_time, match['hoCo'])
            visitor_corner_kick = cls._compute_kick_or_score(compete_time, match['guCo'])
            home_half_score = cls._compute_kick_or_score(compete_time, match['hoHalfScore'])
            visitor_half_score = cls._compute_kick_or_score(compete_time, match['guHalfScore'])

            # 红黄牌
            home_yellow_card = match['hoYellow']
            visitor_yellow_card = match['guYellow']
            home_red_card = match['hoRed']
            visitor_red_card = match['guRed']

            # 原地
            item.update({
                'home_corner_kick': home_corner_kick,
                'visitor_corner_kick': visitor_corner_kick,

                'home_half_score': home_half_score,
                'visitor_half_score': visitor_half_score,

                'home_yellow_card': home_yellow_card,
                'visitor_yellow_card': visitor_yellow_card,
                'home_red_card': home_red_card,
                'visitor_red_card': visitor_red_card,
            })

            yield item


def main() -> None:

    spider.run_spider(
        1,
        FootballMatchSpider,
        MYSQL_CONFIG
    )


if __name__ == '__main__':
    main()

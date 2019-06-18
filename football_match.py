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

            odds = item.pop('odds')

            # 如果比赛还未开始或已经结束，就用即时赔率
            if item['compete_time'] in self.MATCH_NOT_START_FLAG\
                    or item['compete_time'] == '完':
                item.update(self.get_current_odds(item['remote_id']))
            else:  # 一旦比赛开始，则用滚球赔率
                item.update(self.get_roll_odds(odds))

            log.logger.debug(item)

            self.insert_or_update(
                MYSQL_TABLE_FOOTBALL_MATCH,
                item,
                self.UPDATE_FIELD
            )

    def parse(self, jd: Dict, date_format: str) -> Iterator[Dict]:

        matches = jd['matches']

        for item, match in zip(super().parse(jd, date_format), matches):

            compete_time = item['compete_time']

            # 比分，角球
            home_corner_kick = self._compute_kick_or_score(compete_time, match['hoCo'])
            visitor_corner_kick = self._compute_kick_or_score(compete_time, match['guCo'])
            home_half_score = self._compute_kick_or_score(compete_time, match['hoHalfScore'])
            visitor_half_score = self._compute_kick_or_score(compete_time, match['guHalfScore'])

            # 红黄牌
            home_yellow_card = match['hoYellow']
            visitor_yellow_card = match['guYellow']
            home_red_card = match['hoRed']
            visitor_red_card = match['guRed']

            # 滚球指数，交给调用者判断
            odds = match['odds']

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

                'odds': odds,
            })

            yield item

    def get_roll_odds(self, odds: Dict[str, str]) -> Dict[str, str]:
        """获取滚球赔率"""

        handicap = self._compute_handicap(odds['let'])
        home_handicap_odds = odds['letHm']
        visitor_handicap_odds = odds['letAw']
        handicap_total = odds['size']
        home_handicap_total_odds = odds['sizeBig']
        visitor_handicap_total_odds = odds['sizeSma']
        win_odds = odds['avgHm']
        draw_odds = odds['avgEq']
        lose_odds = odds['avgAw']

        return {
            'handicap': handicap,
            'home_handicap_odds': home_handicap_odds,
            'visitor_handicap_odds': visitor_handicap_odds,
            'handicap_total': handicap_total,
            'home_handicap_total_odds': home_handicap_total_odds,
            'visitor_handicap_total_odds': visitor_handicap_total_odds,
            'win_odds': win_odds,
            'draw_odds': draw_odds,
            'lose_odds': lose_odds
        }


def main() -> None:

    spider.run_spider(
        1,
        FootballMatchSpider,
        MYSQL_CONFIG
    )


if __name__ == '__main__':
    main()

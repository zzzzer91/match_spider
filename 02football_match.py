"""足球即时比分

create:   2019-05-31
modified:
"""

import re
import warnings
import datetime

from crash import spider, log
from crash.types import *

from config import *

# 将警告提升为异常，若字段类型不合法，pymysql 只会发出警告
# 提升为异常，可以过滤掉不符合字段类型的数据
warnings.filterwarnings('error')

log.logger.set_log_level(log.DEBUG)


class FootballMatchSpider(spider.MultiThreadSpider):

    url_temp = 'https://live.13322.com/lotteryScore/list?getExtra=1&lang=zh&date={}'

    # sql 插入已存在主键纪录时，更新如下字段
    UPDATE_FIELD = {
        'compete_time',

        'home_corner_kick',
        'visitor_corner_kick',

        'home_score',
        'visitor_score',

        'home_half_score',
        'visitor_half_score',

        'home_yellow_card',
        'visitor_yellow_card',
        'home_red_card',
        'visitor_red_card',

        'handicap',
        'home_handicap_odds',
        'visitor_handicap_odds',
        'handicap_total',
        'home_handicap_total_odds',
        'visitor_handicap_total_odds',
        'win_odds',
        'draw_odds',
        'lose_odds',
    }

    # 球队排名字符串中可能还带联赛名，只提取排名
    RE_FIND_NUM = re.compile(r'\d+')

    def __init__(self,
                 name: str,
                 mysql_config: MysqlConfig,
                 table_save:  str) -> None:
        super().__init__(name, mysql_config, table_save)

        # 改成抓取 json 数据的头部
        self.session.headers.update(self.headers_json)

    def run(self) -> None:

        today = datetime.date.today()
        t_format = today.strftime('%Y-%m-%d')
        t_format2 = today.strftime('%Y%m%d')
        r = self.session.get(self.url_temp.format(t_format))
        jd = r.json()

        for i, item in enumerate(self.parse(jd, t_format2), 1):
            log.logger.debug(item)
            # self.insert_or_update(item, self.UPDATE_FIELD)

    @classmethod
    def parse(cls, jd: Dict, date_format: str) -> Iterator[Dict]:

        matches = jd['matches']

        for match in matches:
            ser_num = cls.RE_FIND_NUM.findall(match['serNum'])[0]
            host_rank = match['hoRank']
            guest_rank = match['guRank']
            odds = match['odds']
            compete_time = match['min']

            yield {
                'id': f'{date_format}{ser_num}',
                'type': 0,

                'remote_id': match['id'],

                'start_time': match['time'],
                'league': match['leagueName'],
                'home_name': match['hoTeamName'],
                'visitor_name': match['guTeamName'],
                # 字符串中可能还带联赛名，只提取排名
                'home_rank': cls.RE_FIND_NUM.findall(host_rank)[0] if host_rank else None,
                'visitor_rank': cls.RE_FIND_NUM.findall(guest_rank)[0] if guest_rank else None,

                'compete_time': compete_time,

                'home_corner_kick': match['hoCo'],
                'visitor_corner_kick': match['guCo'],

                'home_score': match['hoScore'],
                'visitor_score': match['guScore'],

                'home_half_score': match['hoHalfScore'],
                'visitor_half_score': match['guHalfScore'],

                'home_yellow_card': match['hoYellow'],
                'visitor_yellow_card': match['guYellow'],
                'home_red_card': match['hoRed'],
                'visitor_red_card': match['guRed'],

                'handicap': odds['let'].replace('-', '*'),
                'home_handicap_odds': odds['letHm'],
                'visitor_handicap_odds': odds['letAw'],
                'handicap_total': odds['size'],
                'home_handicap_total_odds': odds['sizeBig'],
                'visitor_handicap_total_odds': odds['sizeSma'],
                'win_odds': odds['avgEq'],
                'draw_odds': odds['avgHm'],
                'lose_odds': odds['avgAw'],
            }


def main() -> None:

    spider.run_spider(
        1,
        FootballMatchSpider,
        MYSQL_CONFIG,
        MYSQL_TABLE_FOOTBALL_MATCH
    )


if __name__ == '__main__':
    main()

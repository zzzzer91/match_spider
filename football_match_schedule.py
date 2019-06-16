"""足球比赛日程安排

create:   2019-05-30
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

log.logger.set_log_level(LOG_LEVEL)


class FootballMatchScheduleSpider(spider.MultiThreadSpider):

    url_temp = 'https://live.13322.com/lotteryScore/list?getExtra=1&lang=zh&date={}'

    # sql 插入已存在主键纪录时，更新如下字段
    UPDATE_FIELD = {
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

        # 比赛日程安排是提取明天的
        tomorrow = datetime.datetime.today()
        current_hour = tomorrow.hour
        if current_hour >= 12:
            tomorrow += datetime.timedelta(1)

        self.fetch(tomorrow)

    def fetch(self, date: datetime.datetime) -> None:

        url = self.url_temp.format(date.strftime('%Y-%m-%d'))
        r = self.session.get(url)
        jd = r.json()

        for item in self.parse(jd, date.strftime('%Y%m%d')):
            log.logger.debug(item)
            self.insert_or_update(item, self.UPDATE_FIELD)

    @classmethod
    def parse(cls, jd: Dict, date_format: str) -> Iterator[Dict]:

        matches = jd['matches']

        for match in matches:
            # 提取比赛序号
            ser_num = cls.RE_FIND_NUM.findall(match['serNum'])[0]
            host_rank = match['hoRank']
            guest_rank = match['guRank']

            odds = match['odds']

            handicap = cls._compute_handicap(odds['let'].replace('-', '*'))

            yield {
                'id': f'{date_format}{ser_num}',  # 与 betfair 中的 id 完全对应
                'remote_id': match['id'],
                'start_time': match['time'],
                'league': match['leagueSimpName'],
                'home_name': match['hoTeamSimpName'],
                'visitor_name': match['guTeamSimpName'],
                # 字符串中可能还带联赛名，只提取排名
                'home_rank': cls.RE_FIND_NUM.findall(host_rank)[0] if host_rank else None,
                'visitor_rank': cls.RE_FIND_NUM.findall(guest_rank)[0] if guest_rank else None,

                'handicap': handicap,
                'home_handicap_odds': odds['letHm'],
                'visitor_handicap_odds': odds['letAw'],
                'handicap_total': odds['size'],
                'home_handicap_total_odds': odds['sizeBig'],
                'visitor_handicap_total_odds': odds['sizeSma'],
                'win_odds': odds['avgHm'],
                'draw_odds': odds['avgEq'],
                'lose_odds': odds['avgAw'],
            }

    @staticmethod
    def _compute_handicap(handicap: str) -> Optional[str]:
        """去除小数字符串结尾的 0"""

        if handicap is None:
            return None

        if handicap.endswith('.00'):
            handicap = handicap[:-3]
        elif handicap.endswith('0'):
            handicap = handicap[:-1]
        return handicap


def main() -> None:

    spider.run_spider(
        1,
        FootballMatchScheduleSpider,
        MYSQL_CONFIG,
        MYSQL_TABLE_FOOTBALL_MATCH_SCHEDULE
    )


if __name__ == '__main__':
    main()

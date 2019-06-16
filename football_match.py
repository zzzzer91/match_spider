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

log.logger.set_log_level(LOG_LEVEL)


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
        # 即时比分抓取今天的
        # 足球竞彩时间计算规则：
        # 在 url 中请求今天日期，返回的是
        # 今天中午 12 点（包括 12 点）后，到明天中午 12 点前的比赛
        today = datetime.datetime.today()
        current_hour = today.hour
        if current_hour < 12:
            today -= datetime.timedelta(1)

        self.fetch(today)

        # 有的比赛会比过 12 点
        # 因为过 12 点，就算昨天的了，所以会导致不正确更新
        # 这里在 1 小时内，再把昨天的数据抓下
        if 12 <= current_hour < 13:
            self.fetch(today - datetime.timedelta(1))

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
            ser_num = cls.RE_FIND_NUM.findall(match['serNum'])[0]
            host_rank = match['hoRank']
            guest_rank = match['guRank']
            odds = match['odds']
            match_min = match['min']

            status = match['status']

            compete_time = cls._compute_compete_time(status, match_min)

            home_corner_kick = cls._compute_kick_or_score(status, match['hoCo'])
            visitor_corner_kick = cls._compute_kick_or_score(status, match['guCo'])
            home_score = cls._compute_kick_or_score(status, match['hoScore'])
            visitor_score = cls._compute_kick_or_score(status, match['guScore'])
            home_half_score = cls._compute_kick_or_score(status, match['hoHalfScore'])
            visitor_half_score = cls._compute_kick_or_score(status, match['guHalfScore'])

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

                'compete_time': compete_time,

                'home_corner_kick': home_corner_kick,
                'visitor_corner_kick': visitor_corner_kick,

                'home_score': home_score,
                'visitor_score': visitor_score,

                'home_half_score': home_half_score,
                'visitor_half_score': visitor_half_score,

                'home_yellow_card': match['hoYellow'],
                'visitor_yellow_card': match['guYellow'],
                'home_red_card': match['hoRed'],
                'visitor_red_card': match['guRed'],

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
    def _compute_compete_time(status: int, match_min: int) -> str:
        """目标网站生成比赛状态的前端代码 Python 版"""

        if status == 1:  # 上半场
            compete_time = '45+' if match_min > 45 else f'{match_min}'
        elif status == 3 or status == 4 or status == 5:  # 下半场、加时、点球
            compete_time = '90+' if match_min > 90 else f'{match_min}'
        elif status == 2:  # 中场
            compete_time = '中'
        elif status == -1:  # 完场
            compete_time = '完'
        elif status == -10:
            compete_time = '取消'
        elif status == -11:
            compete_time = '待定'
        elif status == -12:
            compete_time = '腰斩'
        elif status == -13:
            compete_time = '中断'
        elif status == -14:
            compete_time = '推迟'
        else:
            compete_time = '未'

        return compete_time

    @staticmethod
    def _compute_kick_or_score(status: int, data: int) -> Optional[int]:
        """如果比赛还没开始那么返回 None，而不是 0"""

        if data == 0 and status not in (1, 2, 3, 4, 5, -1):
            return None

        return data

    @staticmethod
    def _compute_handicap(handicap: str) -> str:
        """去除小数字符串结尾的 0"""

        if handicap.endswith('.00'):
            handicap = handicap[:-3]
        elif handicap.endswith('0'):
            handicap = handicap[:-1]
        return handicap


def main() -> None:

    spider.run_spider(
        1,
        FootballMatchSpider,
        MYSQL_CONFIG,
        MYSQL_TABLE_FOOTBALL_MATCH
    )


if __name__ == '__main__':
    main()

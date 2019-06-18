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

    url_current_odds_url = 'https://live.13322.com/common/ajaxOddsInfoByMatchId'

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
                 mysql_config: MysqlConfig) -> None:
        super().__init__(name, mysql_config)

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

            temp = self.get_current_odds(item['remote_id'])
            item.update(temp)

            log.logger.debug(item)
            self.insert_or_update(
                MYSQL_TABLE_FOOTBALL_MATCH_SCHEDULE,
                item,
                self.UPDATE_FIELD
            )

    @classmethod
    def parse(cls, jd: Dict, date_format: str) -> Iterator[Dict]:

        matches = jd['matches']

        for match in matches:
            # 提取比赛序号
            ser_num = cls.RE_FIND_NUM.findall(match['serNum'])[0]
            host_rank = match['hoRank']
            guest_rank = match['guRank']

            first_odds = match['firstodds']

            # 为空的数据不如库
            if first_odds is None:
                continue

            handicap = cls._compute_handicap(first_odds['let'].replace('-', '*'))
            home_handicap_odds = first_odds['letHm']
            visitor_handicap_odds = first_odds['letAw']
            handicap_total = first_odds['size']
            home_handicap_total_odds = first_odds['sizeBig']
            visitor_handicap_total_odds = first_odds['sizeSma']
            win_odds = first_odds['avgHm']
            draw_odds = first_odds['avgEq']
            lose_odds = first_odds['avgAw']

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
                'home_handicap_odds': home_handicap_odds,
                'visitor_handicap_odds': visitor_handicap_odds,
                'handicap_total': handicap_total,
                'home_handicap_total_odds': home_handicap_total_odds,
                'visitor_handicap_total_odds': visitor_handicap_total_odds,
                'win_odds': win_odds,
                'draw_odds': draw_odds,
                'lose_odds': lose_odds,
            }

    def get_current_odds(self, remote_id: int) -> Dict:
        """获取实时赔率"""

        # 这里 post 时要换个请求头
        temp = self.session.headers
        self.session.headers = self.headers_html
        r = self.session.post(self.url_current_odds_url, data={'matchId': remote_id})
        self.session.headers = temp

        jd = r.json()

        # log.logger.debug(jd)

        current_odds = self._compute_current_odds(jd)

        handicap = self._compute_handicap(current_odds['let'].replace('-', '*'))
        home_handicap_odds = current_odds['letHm']
        visitor_handicap_odds = current_odds['letAw']
        handicap_total = current_odds['size']
        home_handicap_total_odds = current_odds['sizeBig']
        visitor_handicap_total_odds = current_odds['sizeSma']
        win_odds = current_odds['avgHm']
        draw_odds = current_odds['avgEq']
        lose_odds = current_odds['avgAw']

        return {
            'handicap': handicap,
            'home_handicap_odds': home_handicap_odds,
            'visitor_handicap_odds': visitor_handicap_odds,
            'handicap_total': handicap_total,
            'home_handicap_total_odds': home_handicap_total_odds,
            'visitor_handicap_total_odds': visitor_handicap_total_odds,
            'win_odds': win_odds,
            'draw_odds': draw_odds,
            'lose_odds': lose_odds,
        }

    @staticmethod
    def _compute_handicap(handicap: str) -> Optional[str]:
        """去除小数字符串结尾的 0"""

        if handicap.endswith('.00'):
            handicap = handicap[:-3]
        elif len(handicap) > 1 and handicap.endswith('0'):
            handicap = handicap[:-1]
        return handicap

    @staticmethod
    def _compute_current_odds(jd: List) -> Dict[str, str]:
        """把返回的数据解析成合适的数据"""

        temp = [s.split(',') for s in jd]

        d = {}
        for l in temp:
            if l[-1] == '21':
                d['let'] = l[3]
                d['letHm'] = l[4]
                d['letAw'] = l[5]
            elif l[-1] == '11':
                d['avgEq'] = l[3]
                d['avgHm'] = l[4]
                d['avgAw'] = l[5]
            elif l[-1] == '31':
                d['size'] = l[3]
                d['sizeBig'] = l[4]
                d['sizeSma'] = l[5]
        return d


def main() -> None:

    spider.run_spider(
        1,
        FootballMatchScheduleSpider,
        MYSQL_CONFIG
    )


if __name__ == '__main__':
    main()

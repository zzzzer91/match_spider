"""
create:   2019-05-30
modified:
"""

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

    url_temp = 'https://live.dszuqiu.com/ajax/score/data?mt=0&nr=1'

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

    def __init__(self,
                 name: str,
                 mysql_config: MysqlConfig,
                 table_save:  str) -> None:
        super().__init__(name, mysql_config, table_save)

        # 改成抓取 json 数据的头部
        self.session.headers.update(self.headers_json)

    def run(self) -> None:

        today = datetime.date.today()
        today_format = today.strftime('%Y%m%d')
        r = self.session.get(self.url_temp)
        jd = r.json()

        for i, item in enumerate(self.parse(jd), 1):
            item['id'] = f'{today_format}{i:0>3d}'
            item['type'] = 0
            log.logger.debug(item)
            self.insert_or_update(item, self.UPDATE_FIELD)

    @classmethod
    def parse(cls, jd: Dict) -> Iterator[Dict]:

        matches = jd['rs']

        for match in matches:
            # 全场信息，可提取红黄牌，进球，角球
            rd = match['rd']
            # 半场信息
            rh = match['rh']

            yield {
                'remote_id': match['id'],

                'start_time': match['rtime'],
                'compete_time': match['status'],

                'league': match['leagueName']['n'],
                'home_name': match['host']['n'],
                'guest': match['guTeamName']['n'],

                'home_corner_kick': rd['hc'],
                'visitor_corner_kick': rd['gc'],
                'home_score': rh['hg'],
                'visitor_score': rh['gg'],

                'home_half_score': rd['hg'],
                'visitor_half_score': rd['gg'],

                'home_yellow_card': rd['hy'],
                'visitor_yellow_card': rd['gy'],
                'home_red_card': rd['hr'],
                'visitor_red_card': rd['gr'],
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

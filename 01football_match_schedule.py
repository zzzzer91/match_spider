"""
CREATE TABLE `football_match_schedule` (
  `id` bigint(20) unsigned NOT NULL COMMENT '赛事编号，如20190404001。',
  `type` tinyint(3) unsigned DEFAULT NULL COMMENT '赛事类型，如0表示足球，1表示篮球,这个字段废弃了，不需要入库。',
  `start_time` datetime NOT NULL COMMENT '赛事开始时间，如2019-05-04 12:00。',
  `league` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '联赛名称，如中超。',
  `home_name` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '主队名称，如北京人和。',
  `visitor_name` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '客队名称，如深圳佳兆业。',
  `home_rank` tinyint(3) unsigned DEFAULT NULL COMMENT '主队排名，如15。',
  `visitor_rank` tinyint(3) unsigned DEFAULT NULL COMMENT '客队排名如8。',
  `handicap` decimal(6,2) DEFAULT NULL COMMENT '亚盘让球盘口，即时。',
  `home_handicap_odds` decimal(6,2) DEFAULT NULL COMMENT '主队亚盘让球赔率，即时，如0.71。',
  `visitor_handicap_odds` decimal(6,2) DEFAULT NULL COMMENT '客队亚盘让球赔率，即时，如1.17。',
  `handicap_total` decimal(6,2) DEFAULT NULL COMMENT '亚盘大小球盘口，即时。',
  `home_handicap_total_odds` decimal(6,2) DEFAULT NULL COMMENT '主队亚盘大小球赔率，即时，如1.12。',
  `visitor_handicap_total_odds` decimal(6,2) DEFAULT NULL COMMENT '客队亚盘大小球赔率，即时，如0.72。',
  `win_odds` decimal(6,2) DEFAULT NULL COMMENT '主胜欧赔赔率，即时，如1.10。',
  `draw_odds` decimal(6,2) DEFAULT NULL COMMENT '主平欧赔赔率，即时，如7.00',
  `lose_odds` decimal(6,2) DEFAULT NULL COMMENT '主负欧赔赔率，即时，如31.00。',
  `cst_create` datetime DEFAULT current_timestamp() COMMENT 'CST 时区创建时间',
  `cst_modified` datetime DEFAULT NULL ON UPDATE current_timestamp() COMMENT 'CST 时区修改时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci

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

log.logger.set_log_level(log.DEBUG)


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
        today_format = today.strftime('%Y-%m-%d')
        today_format2 = today.strftime('%Y%m%d')
        r = self.session.get(self.url_temp.format(today_format))
        jd = r.json()

        for i, item in enumerate(self.parse(jd), 1):
            item['id'] = f'{today_format2}{i:0>3d}'
            item['type'] = 0
            log.logger.debug(item)
            self.insert_or_update(item, self.UPDATE_FIELD)

    @classmethod
    def parse(cls, jd: Dict) -> Iterator[Dict]:

        matches = jd['matches']

        for match in matches:
            odds = match['odds']
            yield {
                'start_time': match['time'],
                'league': match['leagueName'],
                'home_name': match['hoTeamName'],
                'visitor_name': match['guTeamName'],
                'home_rank': cls.RE_FIND_NUM.findall(match['hoRank'])[0] if match['hoRank'] else None,
                'visitor_rank': cls.RE_FIND_NUM.findall(match['guRank'])[0] if match['guRank'] else None,
                'handicap': odds['let'],
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
        FootballMatchScheduleSpider,
        MYSQL_CONFIG,
        MYSQL_TABLE_FOOTBALL_MATCH_SCHEDULE
    )


if __name__ == '__main__':
    main()

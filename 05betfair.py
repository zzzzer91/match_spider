"""
CREATE TABLE `betfair` (
  `id` bigint(20) unsigned NOT NULL COMMENT '赛事编号，如20190405001。',
  `start_time` datetime NOT NULL COMMENT '赛事开始时间，如2019-05-0412:00。',
  `betfair_win_odds` decimal(8,2) DEFAULT NULL COMMENT '主胜必发赔率，如1.10。',
  `betfair_draw_odds` decimal(8,2) DEFAULT NULL COMMENT '主平必发赔率，如7.00。',
  `betfair_lose_odds` decimal(8,2) DEFAULT NULL COMMENT '主负必发赔率，如31.00。',
  `betfair_win_index` decimal(8,2) DEFAULT NULL COMMENT '主胜必发指数，如52.93。',
  `betfair_draw_index` decimal(8,2) DEFAULT NULL COMMENT '主平必发指数，如21.34。',
  `betfair_lose_index` decimal(8,2) DEFAULT NULL COMMENT '主负必发指数，如25.71。',
  `avg_win_odds` decimal(8,2) DEFAULT NULL COMMENT '主胜百家欧赔赔率，如1.10。',
  `avg_draw_odds` decimal(8,2) DEFAULT NULL COMMENT '主平百家欧赔赔率，如7.00。',
  `avg_lose_odds` decimal(8,2) DEFAULT NULL COMMENT '主负百家欧赔赔率，如31.00。',
  `total` int(10) unsigned DEFAULT NULL COMMENT '成交总量，如3000000',
  `betfair_win_proportion` decimal(5,2) DEFAULT NULL COMMENT '主胜交易占比，如74.99。',
  `betfair_draw_proportion` decimal(5,2) DEFAULT NULL COMMENT '主平交易占比，如15.13。',
  `betfair_lose_proportion` decimal(5,2) DEFAULT NULL COMMENT '主负交易占比，如10.18。',
  `betfair_win_large_proportion` decimal(5,2) DEFAULT NULL COMMENT '主胜大额交易占比，如76.89。',
  `betfair_draw_large_proportion` decimal(5,2) DEFAULT NULL COMMENT '主平大额交易占比，如21.27。',
  `betfair_lose_large_proportion` decimal(5,2) DEFAULT NULL COMMENT '主负大额交易占比，如1.84。',
  `cst_create` datetime DEFAULT current_timestamp() COMMENT 'CST 时区创建时间',
  `cst_modified` datetime DEFAULT NULL ON UPDATE current_timestamp() COMMENT 'CST 时区修改时间',
  `league` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '联赛名称',
  `home_name` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '主队名称',
  `visitor_name` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '客队名称',
  `match_bf_id` varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '网站的比赛 id，用于获取 detail',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='必发数据爬取'

create:   2019-05-28
modified:
"""

import warnings
import re
import datetime

from lxml import etree

from crash import spider, log
from crash.types import *

from config import *

# 将警告提升为异常，若字段类型不合法，pymysql 只会发出警告
# 提升为异常，可以过滤掉不符合字段类型的数据
warnings.filterwarnings('error')

log.logger.set_log_level(log.DEBUG)


class BetfairSpider(spider.MultiThreadSpider):

    url_temp = 'https://live.aicai.com/jsbf/timelyscore!dynamicBfDataFromPage.htm?lotteryType=zc&issue={}'

    # sql 插入已存在主键纪录时，更新如下字段
    UPDATE_FIELD = {
        'betfair_win_odds',
        'betfair_win_index',
        'avg_win_odds',
        'betfair_draw_odds',
        'betfair_draw_index',
        'avg_draw_odds',
        'betfair_lose_odds',
        'betfair_lose_index',
        'avg_lose_odds',
        'total',
        'betfair_win_proportion',
        'betfair_draw_proportion',
        'betfair_lose_proportion',
        'betfair_win_large_proportion',
        'betfair_draw_large_proportion',
        'betfair_lose_large_proportion'
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

        today_format = datetime.date.today().strftime('%Y%m%d')
        r = self.session.get(self.url_temp.format(today_format))
        jd = r.json()

        for item in self.parse(jd['result']['bf_page'], today_format):
            log.logger.debug(item)
            self.insert_or_update(item, self.UPDATE_FIELD)

    @classmethod
    def parse(cls, html: str, date_format: str) -> Iterator[Dict]:

        selector = etree.HTML(html)

        md_data_box_element_list = selector.xpath('.//div[@class="md_data_box css_league"]')
        for md_data_box_element in md_data_box_element_list:

            title_box_element = md_data_box_element.xpath('.//div[@class="md_tit_box"]')[0]

            match_index = title_box_element.xpath('./span[1]/span//text()')[0].strip()
            match_index = cls.RE_FIND_NUM.findall(match_index)[0]
            _id = f'{date_format}{match_index}'

            # league
            league = title_box_element.xpath('./span[@class="c_dgreen"]/text()')[0].strip()
            # home_name
            home_name = title_box_element.xpath('./span[@class="c_yellow"]/span[1]/text()')[0].strip()
            # visitor_name
            visitor_name = title_box_element.xpath('./span[@class="c_yellow"]/span[last()]/text()')[0].strip()
            # start_time
            start_time = title_box_element.xpath('./span[@class="md_ks_time"]/span[1]/text()')[0].strip()

            content_box_element = md_data_box_element.xpath('.//div[@class="md_con_box"]')[0]

            # match_bf_id，请求 detail 时使用
            match_bf_id = content_box_element.xpath('./div[2]/@value')[0]

            data_table_element = content_box_element.xpath('./div[@class="data_table"]/table/tbody')[0]
            # betfair_win_odds
            betfair_win_odds = data_table_element.xpath('./tr[1]/td[2]/strong/text()')[0].strip()
            # betfair_win_index
            betfair_win_index = data_table_element.xpath('./tr[1]/td[3]/strong/text()')[0].strip()
            # avg_win_odds
            avg_win_odds = data_table_element.xpath('./tr[1]/td[4]/strong/text()')[0].strip()
            # betfair_draw_odds
            betfair_draw_odds = data_table_element.xpath('./tr[2]/td[2]/strong/text()')[0].strip()
            # betfair_draw_index
            betfair_draw_index = data_table_element.xpath('./tr[2]/td[3]/strong/text()')[0].strip()
            # avg_draw_odds
            avg_draw_odds = data_table_element.xpath('./tr[2]/td[4]/strong/text()')[0].strip()
            # betfair_lose_odds
            betfair_lose_odds = data_table_element.xpath('./tr[3]/td[2]/strong/text()')[0].strip()
            # betfair_lose_index
            betfair_lose_index = data_table_element.xpath('./tr[3]/td[3]/strong/text()')[0].strip()
            # avg_lose_odds
            avg_lose_odds = data_table_element.xpath('./tr[3]/td[4]/strong/text()')[0].strip()

            proportion_element = content_box_element.xpath('./div[2]')[0]
            # total
            total = proportion_element.xpath('./div[1]/p[2]/strong/text()')[0].strip()
            # betfair_win_proportion
            betfair_win_proportion = proportion_element.xpath('./div[2]/p[1]/span[2]/text()')[0].strip().rstrip('%')
            # betfair_draw_proportion
            betfair_draw_proportion = proportion_element.xpath('./div[2]/p[2]/span[2]/text()')[0].strip().rstrip('%')
            # betfair_lose_proportion
            betfair_lose_proportion = proportion_element.xpath('./div[2]/p[3]/span[2]/text()')[0].strip().rstrip('%')

            large_proportion_element = content_box_element.xpath('./div[3]')[0]
            # betfair_win_large_proportion
            betfair_win_large_proportion = large_proportion_element.xpath('./div[2]/p[1]/span[2]/text()')[0].strip().rstrip('%')
            # betfair_draw_large_proportion
            betfair_draw_large_proportion = large_proportion_element.xpath('./div[2]/p[2]/span[2]/text()')[0].strip().rstrip('%')
            # betfair_lose_large_proportion
            betfair_lose_large_proportion = large_proportion_element.xpath('./div[2]/p[3]/span[2]/text()')[0].strip().rstrip('%')

            yield {
                'id': _id,
                'league': league,
                'home_name': home_name,
                'visitor_name': visitor_name,
                'start_time': start_time,
                'match_bf_id': match_bf_id,
                'betfair_win_odds': betfair_win_odds,
                'betfair_win_index': betfair_win_index,
                'avg_win_odds': avg_win_odds,
                'betfair_draw_odds': betfair_draw_odds,
                'betfair_draw_index': betfair_draw_index,
                'avg_draw_odds': avg_draw_odds,
                'betfair_lose_odds': betfair_lose_odds,
                'betfair_lose_index': betfair_lose_index,
                'avg_lose_odds': avg_lose_odds,
                'total': total,
                'betfair_win_proportion': betfair_win_proportion,
                'betfair_draw_proportion': betfair_draw_proportion,
                'betfair_lose_proportion': betfair_lose_proportion,
                'betfair_win_large_proportion': betfair_win_large_proportion,
                'betfair_draw_large_proportion': betfair_draw_large_proportion,
                'betfair_lose_large_proportion': betfair_lose_large_proportion,
            }


def main() -> None:
    # 从产品列表页抓取部分数据
    spider.run_spider(
        1,
        BetfairSpider,
        MYSQL_CONFIG,
        MYSQL_TABLE_BETFAIR
    )


if __name__ == '__main__':
    main()

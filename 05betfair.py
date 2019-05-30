"""
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

        if jd['status'] == 'success':
            for item in self.parse(jd['result']['bf_page'], today_format):
                log.logger.debug(item)
                self.insert_or_update(item, self.UPDATE_FIELD)
        else:  # 访问失败，如请求未来日期，日期不合法
            log.logger.error(jd['msg'])

    @classmethod
    def parse(cls, html: str, date_format: str) -> Iterator[Dict]:
        """

        :param html:
        :param date_format: 用于创建入库 id，须在函数外创建，否则可能导致不一致性
        """

        selector = etree.HTML(html)

        md_data_box_element_list = selector.xpath('.//div[@class="md_data_box css_league"]')
        for md_data_box_element in md_data_box_element_list:

            title_box_element = md_data_box_element.xpath('.//div[@class="md_tit_box"]')[0]

            match_index = title_box_element.xpath('./span[1]/span//text()')[0].strip()
            match_index = cls.RE_FIND_NUM.findall(match_index)[0]
            # 入库 id
            _id = f'{date_format}{match_index}'

            # 联赛名，可能为空
            league_element = title_box_element.xpath('./span[@class="c_dgreen"]/text()')
            league = league_element[0].strip() if league_element else None
            # 主队名
            home_name = title_box_element.xpath('./span[@class="c_yellow"]/span[1]/text()')[0].strip()
            # 客队名
            visitor_name = title_box_element.xpath('./span[@class="c_yellow"]/span[last()]/text()')[0].strip()
            # 比赛开始时间
            start_time = title_box_element.xpath('./span[@class="md_ks_time"]/span[1]/text()')[0].strip()

            content_box_element = md_data_box_element.xpath('.//div[@class="md_con_box"]')[0]

            # match_bf_id，目标网站数据库中的比赛 id，请求 detail 时使用
            match_bf_id = content_box_element.xpath('./div[2]/@value')[0]

            data_table_element = content_box_element.xpath('./div[@class="data_table"]/table/tbody')[0]
            # 主胜必发赔率，如 1.10。
            betfair_win_odds = data_table_element.xpath('./tr[1]/td[2]/strong/text()')[0].strip()
            # 主胜必发指数，如52.93。
            betfair_win_index = data_table_element.xpath('./tr[1]/td[3]/strong/text()')[0].strip()
            # 主胜百家欧赔赔率，如1.10。
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
            # 成交总量，如3000000
            total = proportion_element.xpath('./div[1]/p[2]/strong/text()')[0].strip()
            # 主胜交易占比，如74.99。
            betfair_win_proportion = proportion_element.xpath('./div[2]/p[1]/span[2]/text()')[0].strip().rstrip('%')
            # betfair_draw_proportion
            betfair_draw_proportion = proportion_element.xpath('./div[2]/p[2]/span[2]/text()')[0].strip().rstrip('%')
            # betfair_lose_proportion
            betfair_lose_proportion = proportion_element.xpath('./div[2]/p[3]/span[2]/text()')[0].strip().rstrip('%')

            large_proportion_element = content_box_element.xpath('./div[3]')[0]
            # 主胜大额交易占比，如76.89。
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
    spider.run_spider(
        1,
        BetfairSpider,
        MYSQL_CONFIG,
        MYSQL_TABLE_BETFAIR
    )


if __name__ == '__main__':
    main()

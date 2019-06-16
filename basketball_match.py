"""篮球即时比分，只要 NBA 和 CBA 的，亚盘数据来源选澳门

create:   2019-06-02
modified:
"""

import re
import warnings
import datetime

from lxml import etree

from crash import spider, log
from crash.types import *

from config import *

# 将警告提升为异常，若字段类型不合法，pymysql 只会发出警告
# 提升为异常，可以过滤掉不符合字段类型的数据
warnings.filterwarnings('error')

log.logger.set_log_level(LOG_LEVEL)


class BasketballMatchSpider(spider.MultiThreadSpider):

    url_temp = 'https://basket.13322.com/jsbf.html'

    # sql 插入已存在主键纪录时，更新如下字段
    UPDATE_FIELD = {
        'handicap',
        'home_handicap_odds',
        'visitor_handicap_odds',
        'handicap_total',
        'home_handicap_total_odds',
        'visitor_handicap_total_odds',
        'win_odds',
        'lose_odds',
        'compete_time',
        'home_quarter_one',
        'visitor_quarter_one',
        'home_quarter_two',
        'visitor_quarter_two',
        'home_quarter_three',
        'visitor_quarter_three',
        'home_quarter_four',
        'visitor_quarter_four',
        'home_total_score',
        'visitor_total_score',
    }

    LEAGUE_FILTER = {
        'NBA',
        'CBA'
    }

    # 球队排名字符串中可能还带联赛名，只提取排名
    RE_FIND_NUM = re.compile(r'\d+')

    def __init__(self,
                 name: str,
                 mysql_config: MysqlConfig,
                 table_save:  str) -> None:
        super().__init__(name, mysql_config, table_save)

    def run(self) -> None:

        today_format = datetime.date.today().strftime('%Y%m%d')
        url = self.url_temp
        r = self.session.get(url)

        for item in self.parse(r.text, today_format):
            log.logger.debug(item)
            self.insert_or_update(item, self.UPDATE_FIELD)

    @classmethod
    def parse(cls, html: str, date_format: str) -> Iterator[Dict]:

        selector = etree.HTML(html)
        table_all_element = selector.xpath('.//div[@class="table-all"]')[0]

        i = 0
        for table in table_all_element:
            # 联赛名
            league = table.xpath('./thead/tr/th[1]/a/text()')
            league = league[0].strip() if league else None

            if league in cls.LEAGUE_FILTER:
                i += 1

                # 对方数据库中的比赛 id，可用于去重
                remote_id = table.xpath('./@id')[0].lstrip('t_')

                # 比赛开始时间
                time_str_list = table.xpath('./tbody/tr[1]/td[1]/text()')
                start_time = ' '.join(s.strip() for s in time_str_list if s.strip())

                # 即时比赛时间
                compete_time = table.xpath('./thead/tr/th[2]/text()')[0].strip()

                # 主队名，客队名
                xpath_str = './tbody/tr[{}]/td[{}]/a/text()'
                home_name = table.xpath(xpath_str.format(1, 2))[0].strip()
                visitor_name = table.xpath(xpath_str.format(2, 1))[0].strip()

                # 主客队排名
                xpath_str = './tbody/tr[{}]/td/span[contains(@class," show_rank")]/text()'
                home_rank = table.xpath(xpath_str.format(1))[0].strip().strip('[]')
                visitor_rank = table.xpath(xpath_str.format(2))[0].strip().strip('[]')

                # 第一节主客队比分
                xpath_str = './tbody/tr[{}]/td[@tag="{}Score1"]/text()'
                home_quarter_one = table.xpath(xpath_str.format(1, 'h'))[0].strip()
                visitor_quarter_one = table.xpath(xpath_str.format(2, 'g'))[0].strip()
                home_quarter_one = None if home_quarter_one == '-' else home_quarter_one
                visitor_quarter_one = None if visitor_quarter_one == '-' else visitor_quarter_one

                # 第二节主客队比分
                xpath_str = './tbody/tr[{}]/td[@tag="{}Score2"]/text()'
                home_quarter_two = table.xpath(xpath_str.format(1, 'h'))[0].strip()
                visitor_quarter_two = table.xpath(xpath_str.format(2, 'g'))[0].strip()
                home_quarter_two = None if home_quarter_two == '-' else home_quarter_two
                visitor_quarter_two = None if visitor_quarter_two == '-' else visitor_quarter_two

                # 第三节主客队比分
                xpath_str = './tbody/tr[{}]/td[@tag="{}Score3"]/text()'
                home_quarter_three = table.xpath(xpath_str.format(1, 'h'))[0].strip()
                visitor_quarter_three = table.xpath(xpath_str.format(2, 'g'))[0].strip()
                home_quarter_three = None if home_quarter_three == '-' else home_quarter_three
                visitor_quarter_three = None if visitor_quarter_three == '-' else visitor_quarter_three

                # 第四节主客队比分
                xpath_str = './tbody/tr[{}]/td[@tag="{}Score4"]/text()'
                home_quarter_four = table.xpath(xpath_str.format(1, 'h'))[0].strip()
                visitor_quarter_four = table.xpath(xpath_str.format(2, 'g'))[0].strip()
                home_quarter_four = None if home_quarter_four == '-' else home_quarter_four
                visitor_quarter_four = None if visitor_quarter_four == '-' else visitor_quarter_four

                # 主客队总比分
                xpath_str = './tbody/tr[{}]/td[@tag="{}TotalScore"]/text()'
                home_total_score = table.xpath(xpath_str.format(1, 'h'))[0].strip()
                visitor_total_score = table.xpath(xpath_str.format(2, 'g'))[0].strip()
                home_total_score = None if home_total_score == '-' else home_total_score
                visitor_total_score = None if visitor_total_score == '-' else visitor_total_score

                # 欧指
                xpath_str = './tbody/tr[{}]/td[@tag="{}EurOdds"]/a/text()'
                win_odds = table.xpath(xpath_str.format(1, 'h'))
                win_odds = win_odds[0].strip() if win_odds else None
                lose_odds = table.xpath(xpath_str.format(2, 'g'))
                lose_odds = lose_odds[0].strip() if lose_odds else None

                # 亚盘盘口
                xpath_str = './tbody/tr[{}]/td[starts-with(@tag, "rfOdds")]/span[1]/a/text()'
                handicap = table.xpath(xpath_str.format(1))[0].strip()
                handicap2 = table.xpath(xpath_str.format(2))[0].strip()
                if not handicap:
                    if handicap2:
                        handicap = '-' + handicap2
                    else:
                        handicap = None
                handicap = cls._compute_handicap(handicap)

                # 亚盘赔率
                xpath_str = './tbody/tr[{}]/td[@tag="{}RfOdds"]/span[1]/a/text()'
                home_handicap_odds = table.xpath(xpath_str.format(1, 'h'))[0].strip()
                home_handicap_odds = home_handicap_odds if home_handicap_odds else None
                visitor_handicap_odds = table.xpath(xpath_str.format(2, 'g'))[0].strip()
                visitor_handicap_odds = visitor_handicap_odds if visitor_handicap_odds else None

                # 大小球盘口
                xpath_str = './tbody/tr[{}]/td[@tag="dxfOdds"]/span[1]/a/text()'
                handicap_total = table.xpath(xpath_str.format(1))[0].strip()
                handicap_total = handicap_total if handicap_total else None

                # 大小球赔率
                xpath_str = './tbody/tr[{}]/td[@tag="{}DxfOdds"]/span[1]/a/text()'
                home_handicap_total_odds = table.xpath(xpath_str.format(1, 'h'))[0].strip()
                home_handicap_total_odds = home_handicap_total_odds if home_handicap_total_odds else None
                visitor_handicap_total_odds = table.xpath(xpath_str.format(2, 'g'))[0].strip()
                visitor_handicap_total_odds = visitor_handicap_total_odds if visitor_handicap_total_odds else None

                yield {
                    'remote_id': remote_id,
                    'id': f'{date_format}{i:0>3d}',  # 左填充 0
                    'league': league,
                    'start_time': f'{date_format[:4]}-{start_time}',
                    'home_name': home_name,
                    'visitor_name': visitor_name,
                    'home_rank': home_rank,
                    'visitor_rank': visitor_rank,
                    'win_odds': win_odds,
                    'lose_odds': lose_odds,
                    'handicap': handicap,
                    'home_handicap_odds': home_handicap_odds,
                    'visitor_handicap_odds': visitor_handicap_odds,
                    'handicap_total': handicap_total,
                    'home_handicap_total_odds': home_handicap_total_odds,
                    'visitor_handicap_total_odds': visitor_handicap_total_odds,

                    'compete_time': compete_time,
                    'home_quarter_one': home_quarter_one,
                    'visitor_quarter_one': visitor_quarter_one,
                    'home_quarter_two': home_quarter_two,
                    'visitor_quarter_two': visitor_quarter_two,
                    'home_quarter_three': home_quarter_three,
                    'visitor_quarter_three': visitor_quarter_three,
                    'home_quarter_four': home_quarter_four,
                    'visitor_quarter_four': visitor_quarter_four,
                    'home_total_score': home_total_score,
                    'visitor_total_score': visitor_total_score,
                }

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
        BasketballMatchSpider,
        MYSQL_CONFIG,
        MYSQL_TABLE_BASKETBALL_MATCH
    )


if __name__ == '__main__':
    main()

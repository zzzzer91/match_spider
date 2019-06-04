"""
create:   2019-05-29
modified:
"""

import queue
import warnings
import datetime

from crash import spider, log
from crash.types import *

from config import *

# 将警告提升为异常，若字段类型不合法，pymysql 只会发出警告
# 提升为异常，可以过滤掉不符合字段类型的数据
warnings.filterwarnings('error')

log.logger.set_log_level(LOG_LEVEL)


class BetfairDetailSpider(spider.MultiThreadSpider):

    url_temp = 'https://live.aicai.com/bf/bfindex!ajaxsBigTrade.htm?matchBFId={}'

    trade_type = {
        '0': '无', '1': '买入', '2': '卖出'
    }

    index_type = {
        '0': '客胜', '1': '平局', '3': '主胜'
    }

    def __init__(self,
                 name: str,
                 mysql_config: MysqlConfig,
                 table_save:  str) -> None:
        super().__init__(name, mysql_config, table_save)

        # 改成抓取 json 数据的头部
        self.session.headers.update(self.headers_json)

    def run(self) -> None:

        while self._running:
            try:
                _id, match_bf_id = self.q.get_nowait()
            except queue.Empty:
                break

            year = datetime.date.today().year
            url = self.url_temp.format(match_bf_id)
            r = self.session.get(url)
            jd = r.json()

            if jd['status'] == 'success':
                for item in self.parse(jd, year):
                    item['betfair_id'] = _id  # 外键
                    log.logger.debug(item)
                    self.insert(item)
            else:
                log.logger.error(jd['msg'])

    @classmethod
    def parse(cls, jd: Dict, year: int) -> Iterator[Dict]:
        """

        :param jd:
        :param year: 用于在交易时间前插入年份，函数外传入，保持一致性
        """

        result = jd['result']

        big_list = result['bigTradeList']['bigList']
        if not big_list['all']:
            return

        for record in big_list['win'] + big_list['draw'] + big_list['lose']:
            # record 格式类似 '110342|2|1|500|05-29 04:35'
            temp = record.split('|')

            # 成交额
            turnover = int(temp[0]) // 100
            # 属性，从数字映射成中文
            status = cls.index_type[temp[2]] + cls.trade_type[temp[1]]
            # 价位
            price = round(float(temp[3]) / 100, 2)
            # 交易时间
            transaction_time = f'{year}-{temp[4]}'
            # 总交易额
            total_trade = int(result['bfMatch']['homeAmount'])\
                          + int(result['bfMatch']['drawAmount'])\
                          + int(result['bfMatch']['awayAmount'])
            # 交易占比
            proportion = round(turnover * 100 / total_trade * 100, 2)

            yield {
                'turnover': turnover,
                'status': status,
                'price': price,
                'transaction_time': transaction_time,
                'proportion': proportion
            }


def main() -> None:
    today_format = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
    # 只更新比赛开始时间大于当前时间的数据
    mysql_sql = f'SELECT id, match_bf_id FROM {MYSQL_TABLE_BETFAIR} WHERE start_time > "{today_format}"'
    log.logger.debug(mysql_sql)
    BetfairDetailSpider.create_task_list(MYSQL_CONFIG, mysql_sql)

    spider.run_spider(
        1,
        BetfairDetailSpider,
        MYSQL_CONFIG,
        MYSQL_TABLE_BETFAIR_DETAIL
    )


if __name__ == '__main__':
    main()

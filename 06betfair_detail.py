"""
CREATE TABLE `betfair_detail` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `betfair_id` bigint(20) unsigned NOT NULL COMMENT 'betfair表中的赛事编号，如20190405001。',
  `turnover` int(10) unsigned NOT NULL COMMENT '成交额，如3000。',
  `status` char(8) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '交易状态，如主胜买入。',
  `price` decimal(10,2) NOT NULL COMMENT '交易价格，如1.89。',
  `proportion` decimal(5,2) NOT NULL COMMENT '交易占比，如12.34。',
  `time` datetime NOT NULL COMMENT '交易时间，如2019-05-04 09:00。',
  `cst_create` datetime DEFAULT current_timestamp() COMMENT 'CST 时区创建时间',
  `cst_modified` datetime DEFAULT NULL ON UPDATE current_timestamp() COMMENT 'CST 时区修改时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci

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

log.logger.set_log_level(log.DEBUG)


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

            r = self.session.get(self.url_temp.format(match_bf_id))
            jd = r.json()
            for item in self.parse(jd):
                item['betfair_id'] = _id  # 外键
                log.logger.debug(item)
                self.insert(item)

    @classmethod
    def parse(cls, jd: Dict) -> Iterator[Dict]:

        result = jd['result']

        big_list = result['bigTradeList']['bigList']
        if not big_list['all']:
            return

        for record in big_list['win'] + big_list['draw'] + big_list['lose']:
            # recode 格式类似 '110342|2|1|500|05-29 04:35'
            temp = record.split('|')

            # 成交额
            turnover = int(temp[0]) // 100
            # 属性，从数字映射成中文
            status = cls.index_type[temp[2]] + cls.trade_type[temp[1]]
            # 价位
            price = round(float(temp[3]) / 100, 2)
            # 交易时间
            time = datetime.datetime.today().strftime('%Y-') + temp[4]
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
                'time': time,
                'proportion': proportion
            }


def main() -> None:
    mysql_sql = 'SELECT id, match_bf_id FROM {}'.format(MYSQL_TABLE_BETFAIR)
    BetfairDetailSpider.create_task_list(MYSQL_CONFIG, mysql_sql)

    # 从产品列表页抓取部分数据
    spider.run_spider(
        1,
        BetfairDetailSpider,
        MYSQL_CONFIG,
        MYSQL_TABLE_BETFAIR_DETAIL
    )


if __name__ == '__main__':
    main()

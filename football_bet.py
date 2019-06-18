"""与足球即时比分相比，比赛开始了，这张表的赔率不更新，与 football_match 适用不同场景的。

create:   2019-06-16
modified:
"""

import warnings
import datetime

from crash import spider, log
from crash.types import *
from football_match_schedule import FootballMatchScheduleSpider

from config import *

# 将警告提升为异常，若字段类型不合法，pymysql 只会发出警告
# 提升为异常，可以过滤掉不符合字段类型的数据
warnings.filterwarnings('error')

log.logger.set_log_level(LOG_LEVEL)


class FootballBetSpider(FootballMatchScheduleSpider):

    # sql 插入已存在主键纪录时，更新如下字段
    # 比赛开始后，赔率相关不更新
    AFTER_MATCH_START_UPDATE_FIELD = {
        'compete_time',
        'home_score',
        'visitor_score',
    }

    UPDATE_FIELD = {
        *FootballMatchScheduleSpider.UPDATE_FIELD,
        *AFTER_MATCH_START_UPDATE_FIELD
    }

    MATCH_NOT_START_FLAG = {'取消', '待定', '腰斩', '中断', '推迟', '未'}

    def __init__(self,
                 name: str,
                 mysql_config: MysqlConfig) -> None:

        super().__init__(name, mysql_config)

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
        # 这里在一段时间内，再把昨天的数据抓下
        if 12 <= current_hour < 14:
            self.fetch(today - datetime.timedelta(1))

    def fetch(self, date: datetime.datetime) -> None:

        url = self.url_temp.format(date.strftime('%Y-%m-%d'))
        r = self.session.get(url)
        jd = r.json()

        for item in self.parse(jd, date.strftime('%Y%m%d')):
            # 不用这些字段
            item.pop('home_rank')
            item.pop('visitor_rank')

            item.update(self.get_current_odds(item['remote_id']))

            if item['compete_time'] in self.MATCH_NOT_START_FLAG:
                update_field = self.UPDATE_FIELD
            else:
                update_field = self.AFTER_MATCH_START_UPDATE_FIELD

            log.logger.debug(item)

            self.insert_or_update(MYSQL_TABLE_FOOTBALL_BET, item, update_field)

    def parse(self, jd: Dict, date_format: str) -> Iterator[Dict]:

        matches = jd['matches']

        for item, match in zip(super().parse(jd, date_format), matches):

            # 比赛状态，有多种，详见 _compute_compete_time 函数
            status = match['status']

            # 比赛进行时间，原始数据需要进行一些转换
            match_min = match['min']
            compete_time = self._compute_compete_time(status, match_min)

            # 总比分
            home_score = self._compute_kick_or_score(compete_time, match['hoScore'])
            visitor_score = self._compute_kick_or_score(compete_time, match['guScore'])

            # 原地
            item.update({
                'compete_time': compete_time,
                'home_score': home_score,
                'visitor_score': visitor_score
            })

            yield item

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

    @classmethod
    def _compute_kick_or_score(cls, compete_time: str, data: int) -> Optional[int]:
        """如果比赛还没开始那么返回 None，而不是 0"""

        if data == 0 and compete_time in cls.MATCH_NOT_START_FLAG:
            return None

        return data


def main() -> None:

    spider.run_spider(
        1,
        FootballBetSpider,
        MYSQL_CONFIG
    )


if __name__ == '__main__':
    main()

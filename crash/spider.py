"""通用爬虫类，数据存储在 MySQL 中。

create:   2018-12-12
modified: 2019-05-28
"""

import atexit
import threading
import queue

import pymysql

from . import sessions, db, log
from .types import *


class MultiThreadSpider(threading.Thread):

    # 如果请求 html，用这个头部
    headers_html: Dict[str, str] = {
        'User-Agent': 'Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko)',
        'Accept': 'text/html,application/xhtml+xml,application/xml;'
                  'q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive'
    }

    # 如果请求 json，用这个头部
    headers_json: Dict[str, str] = {
        'User-Agent': 'Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko)',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7',
        'Content-Type': 'application/json; charset=UTF-8',
        'x-requested-with': 'XMLHttpRequest',  # 代表是 ajax 请求
        'Connection': 'keep-alive'
    }

    # 任务队列，分发任务
    q: Optional[Queue] = None

    def __init__(self,
                 name: str,
                 mysql_config: MysqlConfig,
                 daemon: bool = True) -> None:
        super().__init__(name=name, daemon=daemon)

        self._running = True

        atexit.register(self.close)  # 注册清理函数，线程结束时自动调用

        self.mysql_conn = pymysql.connect(
            **mysql_config, autocommit=True
        )
        self.cursor = self.mysql_conn.cursor()

        self.session = sessions.Session()
        self.session.headers.update(self.headers_html)  # 默认 html 头部

    def run(self) -> None:
        """抽象方法，由子类继承创建。"""

        raise NotImplementedError

    def insert(self, table: str, item: Dict) -> None:
        sql = 'INSERT INTO {} ({}) VALUES ({})'.format(
            table,
            ', '.join(item),
            ', '.join(f'%({k})s' for k in item)
        )

        try:
            self.cursor.execute(sql, item)
        except pymysql.IntegrityError:
            log.logger.debug(f'存在重复字段！ {str(item)}')
        except pymysql.err.Warning:  # 过滤不合法 mysql 类型
            log.logger.error(f'字段类型不合法！ {str(item)}')

    def update(self, table: str, where: str, item: Dict) -> None:
        sql = 'UPDATE {} SET {} WHERE {}'.format(
            table,
            ', '.join(f'{k} = %({k})s' for k in item),
            where
        )

        try:
            self.cursor.execute(sql, item)
        except pymysql.err.Warning:
            log.logger.error(f'字段类型不合法！ {str(item)}')

    def insert_or_update(self, table: str, item: Dict, update_field: set) -> None:
        """sql 插入已存在主键纪录时，更新指定字段

        :param table: 表名
        :param item: 数据
        :param update_field: 需要更新的字段
        """

        sql = 'INSERT INTO {} ({}) VALUES ({}) ON DUPLICATE KEY UPDATE {}'.format(
            table,
            ', '.join(item),
            ', '.join(f'%({k})s' for k in item),
            ', '.join(f'{k} = %({k})s' for k in item if k in update_field)
        )

        try:
            self.cursor.execute(sql, item)
        except pymysql.err.Warning:  # 过滤不合法 mysql 类型
            log.logger.error(f'字段类型不合法！ {str(item)}')

    def terminate(self) -> None:
        self._running = False

    def close(self) -> None:
        self.session.close()
        self.cursor.close()
        self.mysql_conn.close()

    @classmethod
    def create_task_list(cls, mysql_config: MysqlConfig, sql: str) -> None:
        """
        从 MySQL 中读取任务，
        放入一个全局变量 `q` 队列中，
        供多个线程使用。
        """

        cls.q = queue.Queue()

        for row in db.read_data(mysql_config, sql):
            cls.q.put(row)


def run_spider(
        thread_num: int,
        spider_class: Type[MultiThreadSpider],
        mysql_config: MysqlConfig) -> None:

    thread_list: List[MultiThreadSpider] = []
    for i in range(thread_num):
        t = spider_class(
            f'thread{i+1}', mysql_config
        )
        thread_list.append(t)

    for t in thread_list:
        log.logger.info(f'{t.__class__.__name__} {t.name} 启动')
        t.start()

    try:
        for t in thread_list:
            t.join()
    except KeyboardInterrupt:  # 只有主线程能收到键盘中断
        for t in thread_list:  # 防止下面在保存完 `row` 后，线程又请求一个新 `row`
            t.terminate()
        exit(1)

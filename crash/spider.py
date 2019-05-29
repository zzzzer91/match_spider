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
        'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7'
    }

    # 如果请求 json，用这个头部
    headers_json: Dict[str, str] = {
        'User-Agent': 'Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko)',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7',
        'Content-Type': 'application/json; charset=UTF-8',
        'x-requested-with': 'XMLHttpRequest'  # 代表是 ajax 请求
    }

    # 任务队列，分发任务
    q: Optional[Queue] = None

    def __init__(self,
                 name: str,
                 mysql_config: MysqlConfig,
                 table_save: str,
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

        self.table_save = table_save

        self.sql_insert: Optional[str] = None
        self.sql_update: Optional[str] = None
        self.sql_insert_or_update: Optional[str] = None

    def run(self) -> None:
        """抽象方法，由子类继承创建。"""

        raise NotImplementedError

    def insert(self, item: Dict) -> None:
        if self.sql_insert is None:  # 只构建一次，提高性能
            self.sql_insert = 'INSERT INTO {} ({}) VALUES ({})'.format(
                self.table_save,
                ', '.join(item),
                ', '.join(f'%({k})s' for k in item)
            )

        try:
            self.cursor.execute(self.sql_insert, item)
        except pymysql.IntegrityError:
            log.logger.debug(f'存在重复字段！ {str(item)}')
        except pymysql.err.Warning:  # 过滤不合法 mysql 类型
            log.logger.error(f'字段类型不合法！ {str(item)}')

    def update(self, where: str, item: Dict) -> None:
        if self.sql_update is None:
            self.sql_update = 'UPDATE {} SET {} WHERE {{}}'.format(
                self.table_save,
                ', '.join(f'{k} = %({k})s' for k in item)
            )

        try:
            self.cursor.execute(self.sql_update.format(where), item)
        except pymysql.err.Warning:
            log.logger.error(f'字段类型不合法！ {str(item)}')

    def insert_or_update(self, item: Dict, update_field: set) -> None:
        """sql 插入已存在主键纪录时，更新指定字段

        :param item: 数据
        :param update_field: 需要更新的字段
        """

        if self.sql_insert_or_update is None:  # 只构建一次，提高性能
            self.sql_insert_or_update = 'INSERT INTO {} ({}) VALUES ({}) ON DUPLICATE KEY UPDATE {}'.format(
                self.table_save,
                ', '.join(item),
                ', '.join(f'%({k})s' for k in item),
                ', '.join(f'{k} = %({k})s' for k in item if k in update_field)
            )

        try:
            self.cursor.execute(self.sql_insert_or_update, item)
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
        mysql_config: MysqlConfig,
        table_save:  str) -> None:

    thread_list: List[MultiThreadSpider] = []
    for i in range(thread_num):
        t = spider_class(
            f'thread{i+1}', mysql_config, table_save
        )
        thread_list.append(t)

    for t in thread_list:
        t.start()

    try:
        for t in thread_list:
            t.join()
    except KeyboardInterrupt:  # 只有主线程能收到键盘中断
        for t in thread_list:  # 防止下面在保存完 `row` 后，线程又请求一个新 `row`
            t.terminate()
        exit(1)

import json
import sqlite3
import logging


logging.basicConfig(format='%(asctime)s %(message)s')
log = logging.getLogger(name="log")
log.setLevel(level=logging.INFO)

class DatabaseOperation():
    def __init__(self):
        self.database_path = "/home/jianrui/Gold/goldPriceHistory"
        self.connect,self.cursor = self.buildConnect()
        log.info("数据库初始化成功！")

    def buildConnect(self):
        # 建立数据连接和cursor
        connect = sqlite3.connect(self.database_path,timeout=10)
        cursor = connect.cursor()
        log.info("数据连接成功！")
        return connect,cursor
    
    def closeConnect(self):
        self.connect.close()

    def searchSql_price(self,current_timestamp,time_long):
        # 黄金价格查询sql
        time_long = time_long
        sql = "select * from goldPrice where {current_timestamp}-timestamp<{time_long} and {current_timestamp}-timestamp>-1".format(current_timestamp=str(current_timestamp), time_long=str(time_long))
        return sql
    
    def searchSql_ETFprice(self,current_timestamp,time_long,fund_code):
        # 黄金ETF价格查询sql
        time_long = time_long
        sql = "select * from GoldETFPrice where fund_code={fund_code} and {current_timestamp}-timestamp<{time_long} and {current_timestamp}-timestamp>-1".format(fund_code=fund_code,current_timestamp=str(current_timestamp), time_long=str(time_long))
        return sql

    def searchSql_fluctuation(self,current_timestamp,time_long):
        time_long = time_long
        sql = "select * from goldPriceFluctuation where {current_timestamp}-timestamp<{time_long} and {current_timestamp}-timestamp>-1".format(current_timestamp=str(current_timestamp), time_long=str(time_long))
        return sql

    def searchSql_byTimestamp(self,tablename,timestamp,condition=None):
        sql = "select * from {tablename} where timestamp={timestamp}".format(tablename=tablename,timestamp=str(timestamp))
        if condition:
            sql  = sql + " and " + condition
        return sql

    def searchSql_maxTimestamp(self,tablename,condition=None):
        # 获取价格数据库的最大时间
        sql = "select max(timestamp) as timestamp from {}".format(tablename)
        if condition:
            sql = sql + " where " + condition
        return sql

    def searchSql_maxOneTimestamp(self,tablename,timestamp,condition=None):
        sql = "select timestamp from {tablename} where timestamp>{timestamp}".format(tablename=tablename,timestamp=str(timestamp))
        if condition:
            sql = sql + " and " + condition
        return sql

    def executeSearchSql(self,sql):
        # sql执行
        self.cursor.execute(sql)
        headers = [header[0] for header in self.cursor.description]
        rows = []
        for oneline in self.cursor.fetchall():
            tmp = {}
            for k,v in zip(headers,oneline):
                tmp[k] = v
            rows.append(tmp)
        return rows

    def insertSql_fluctuation(self,oneline):
        # 插入波动sql
        key = ",".join(list(oneline.keys()))
        value = ",".join([oneline[k] for k in oneline.keys()])
        sql = """insert into goldPriceFluctuation ({key}) values ({value}) ON CONFLICT(timestamp) Do nothing""".format(
                    key = key, value = value )
        return sql
    
    def insertSql_norm(self,table_name,onelines):
        # 一般形式的写入sql
        key = ",".join(list(onelines[0].keys()))
        values = ["("+",".join(["'"+str(oneline[k])+"'" for k in oneline.keys()])+")" for oneline in onelines]
        values = ",".join(values)
        sql = """insert into {table_name} ({key}) values {values}""".format(
                    table_name = table_name, key = key, values = values )
        return sql

    def executeInsertSql(self,sql):
        self.cursor.execute(sql)
        self.connect.commit()

import pandas as pd
import json
import time
from datetime import datetime
import requests
import logging
import sqlite3
from DatabaseOperation import DatabaseOperation
from Robot import Robot
import adata

import tushare as ts
ts.set_token("08aaec36d1fad0154592971c8fb64a472b8f3fe954dc33c8d0e87447")
from rtq import  (realtime_quote, realtime_list)
from histroy_divide import (realtime_tick)
ts.realtime_quote = realtime_quote
ts.realtime_list = realtime_list
ts.realtime_tick = realtime_tick

logging.basicConfig(format='%(asctime)s %(message)s')
log = logging.getLogger(name="log")
log.setLevel(level=logging.INFO)

# 抓取数据脚本

class DataCrawBase():
    def __init__(self):
        self.databaseOperation = DatabaseOperation()
        self.tradeCalender,self.tradeYear = self.getTradeCalender()
        self.readFrequency = 15 # 一分钟采样10个点

    def getCurrentDate(slef):
        # 读取当前时间，返回年月日时分秒
        return datetime.now()

    def getTradeCalender(self):
        # 读取交易日历
        date = self.getCurrentDate()
        year = date.strftime("%Y")
        calender = adata.stock.info.trade_calendar(year=year)
        calender = calender.to_dict(orient="records")
        calender ={i["trade_date"]:i for i in calender}
        return calender,year # 格式：{"trade_date":{"trade_date":"2023-01-01","trade_status":"0","day_week":"1"}}
    
    def getTradeTimestampInterval(self,date):
        # 计算开市的时间戳
        date_str = date.strftime("%Y-%m-%d %h:%M:%S").split(" ")[0]
        morningDate = [" ".join([date_str,"09:30:00"])," ".join([date_str,"11:30:00"])]
        afternoonDate = [" ".join([date_str,"13:00:00"])," ".join([date_str,"15:00:00"])]
        morningTimestamp = [datetime.strptime(i, '%Y-%m-%d %H:%M:%S').timestamp() for i in morningDate]
        afternoonTimestamp = [datetime.strptime(i, '%Y-%m-%d %H:%M:%S').timestamp() for i in afternoonDate]
        return morningTimestamp,afternoonTimestamp
    
    def getNextDay(self,data):
        tmp_timestamp = data.timestamp() + 24*60*60
        nextDay = datetime.fromtimestamp(tmp_timestamp)
        nextDay = nextDay.strftime("%Y-%m-%d")
        nextDay = nextDay + " 00:00:01"
        nextDay = datetime.strptime(nextDay,'%Y-%m-%d %H:%M:%S')
        return nextDay             

    def getDataAndSave(self):
        pass

    def run(self):
        while True:
            currentDate = self.getCurrentDate()
            # 判断日历年号和当前年号是否一致,如果不一致，那么就将更新日历
            if currentDate.strftime("%Y") != self.tradeYear:
                self.tradeCalender,self.tradeYear = self.getTradeCalender()

            # 判断是否是开市日，# 没有开市,休眠到第二天的
            if self.tradeCalender[currentDate.strftime("%Y-%m-%d")]["trade_status"] == 0:
                nextDay = self.getNextDay(currentDate)
                nextDayTimestamp = nextDay.timestamp()
                currentTimestamp = currentDate.timestamp()
                log.info(":{}不是开市日".format(currentDate.strftime("%Y-%m-%d")))
                time.sleep(nextDayTimestamp - currentTimestamp)
                continue

            # 当前时间开市时间段
            morningTimestamp, afternoonTimestamp = self.getTradeTimestampInterval(currentDate)


            while True:
                # 休眠到早市开市时间
                currentDate = self.getCurrentDate()
                currentTimestamp = currentDate.timestamp()
                # currentTimestamp = 1751683286

                # 处在开市前时间
                if currentTimestamp < morningTimestamp[0]:
                    deltaTimestamp = morningTimestamp[0] - currentTimestamp
                    log.info("休眠到开早市的时间")
                    time.sleep(deltaTimestamp)   
                
                elif morningTimestamp[0] < currentTimestamp < morningTimestamp[1] or afternoonTimestamp[0] < currentTimestamp < afternoonTimestamp[1]:
                    # 处在交易时间
                    start = datetime.now()
                    # 抓取数据
                    self.getDataAndSave()
                    end = datetime.now()
                    delta = end - start
                    sleep_time = self.readFrequency-delta.seconds if self.readFrequency-delta.seconds >0 else 0
                    time.sleep(sleep_time)

                elif morningTimestamp[1] < currentTimestamp < afternoonTimestamp[0]:
                    # 中午休市区
                    log.info("中午休市的时间")
                    sleep_time = afternoonTimestamp[0] - currentTimestamp
                    time.sleep(sleep_time)

                elif currentTimestamp > afternoonTimestamp[1]:
                    # 晚上休市
                    nextDay = self.getNextDay(currentDate)
                    nextDayTimestamp = nextDay.timestamp()
                    currentTimestamp = currentDate.timestamp()
                    log.info("休眠到下一天")
                    time.sleep(nextDayTimestamp - currentTimestamp)
                    break
                else:
                    time.sleep(1)

class DataCraw_TuShare(DataCrawBase):
    def __init__(self):
        super().__init__()        
        # 备选
        # ["518880","159934","159937","518800","518660"]:
        self.fund_code_list = ["518880.SH","588000.SH"]
        # self.fund_code_list = ["588000.SH"]
        self.wroteTimestamp = self.getWroteTimestamp()
        self.robot = Robot()



    def getWroteTimestamp(self):
        self.wroteTimestamp = {}
        for id in self.fund_code_list:
            sql = self.databaseOperation.searchSql_maxTimestamp("GoldETFPrice","fund_code={}".format(id.split(".")[0]))
            row = self.databaseOperation.executeSearchSql(sql)
            row = row[0]
            if row["timestamp"]:
                self.wroteTimestamp[id] = row["timestamp"]
            else:
                self.wroteTimestamp[id] = 0
        return self.wroteTimestamp

    def getDataAndSave(self):
        result = []
        for id in self.fund_code_list:
            try:
                df = ts.realtime_quote(ts_code=id, src='dc')
            except Exception as e:
                data = {"date":self.getCurrentDate().strftime('%Y-%m-%d %H:%M:%S'),"错误类型":str(e)}
                self.robot.sendMessage(data, self.robot.transMessage_dataCraw )
                continue

            df  = df.to_dict(orient="records")[0]

            df["timestamp"] = int(datetime.strptime(df["DATE"]+df["TIME"], '%Y%m%d %H:%M:%S').timestamp())
            df["trade_time"] = datetime.fromtimestamp(df["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')

            # 改为小写+改名
            for key in list(df.keys()):
                df[key.lower()] = df[key]
                if key.lower() != key:
                    df.pop(key)
            df["fund_code"] = df["ts_code"].split(".")[0]
            df.pop("ts_code")


            if df["timestamp"] > self.wroteTimestamp[id]:
                self.wroteTimestamp[id] = df["timestamp"]
                result.append(df)
            else:
                pass

            time.sleep(3)

        if result:
            sql = self.databaseOperation.insertSql_norm("GoldETFPrice",result)
            log.info("数据写入数据库")
            log.info(sql)
            self.databaseOperation.executeInsertSql(sql)
        else:
            log.info("无更新数据插入!!")



if __name__ == "__main__":
    data = DataCraw_TuShare()
    data.getDataAndSave()

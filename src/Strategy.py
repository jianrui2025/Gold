import json
import logging
from Robot import Robot
from DatabaseOperation import DatabaseOperation
from datetime import datetime
from abc import abstractclassmethod
import time
import math
from scipy.stats import norm
import numpy as np
import adata
import random
import requests
import pandas as pd
import io
import statistics
import multiprocessing as mp
import copy

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

class StrategyBase():
    def __init__(self,runStrategyInterval):
        self.robot = Robot()
        # self.databaseOperation = DatabaseOperation()
        self.tradeCalender,self.tradeYear = self.getTradeCalender()
        self.runStrategyInterval = runStrategyInterval # 检测策略的间隔
        self.RANDOM_STR = "ZXCVBNMASDFGHJKLQWERTYUIOP1234567890qwertyuiopasdfghjklzxcvbnm"
        self.before_strategy_mark = False

    def getCurrentDate(self):
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
    
    def getNextDay(self,date):
        tmp_timestamp = date.timestamp() + 24*60*60
        nextDay = datetime.fromtimestamp(tmp_timestamp)
        nextDay = nextDay.strftime("%Y-%m-%d")
        nextDay = nextDay + " 00:00:01"
        nextDay = datetime.strptime(nextDay,'%Y-%m-%d %H:%M:%S')
        return nextDay
    
    def getTradeTimestampInterval(self,date):
        # 计算开市的时间戳
        date_str = date.strftime("%Y-%m-%d %h:%M:%S").split(" ")[0]
        morningDate = [" ".join([date_str,"09:30:00"])," ".join([date_str,"11:30:00"])]
        afternoonDate = [" ".join([date_str,"13:00:00"])," ".join([date_str,"15:00:00"])]
        morningTimestamp = [datetime.strptime(i, '%Y-%m-%d %H:%M:%S').timestamp() for i in morningDate]
        afternoonTimestamp = [datetime.strptime(i, '%Y-%m-%d %H:%M:%S').timestamp() for i in afternoonDate]
        return morningTimestamp,afternoonTimestamp
    
    def set_worning(self,worning,worning_file):
        with open(worning_file,"a") as f:
            f.write(json.dumps(worning,ensure_ascii=False)+"\n")
    
    def before_strategy(self):
        pass

    @abstractclassmethod
    def strategy(self):
        pass

    def after_strategy(self):
        pass

    def run(self):
        while True:
            currentDate = self.getCurrentDate()
            # 判断是否是开市日
            if self.tradeCalender[currentDate.strftime("%Y-%m-%d")]["trade_status"] == 0:
                # 没有开市,休眠到第二天的
                nextDay = self.getNextDay(currentDate)
                nextDayTimestamp = nextDay.timestamp()
                currentTimestamp = currentDate.timestamp()
                log.info(":{}不是开市日，休眠到下一个开市日".format(currentDate.strftime("%Y-%m-%d")))
                time.sleep(nextDayTimestamp - currentTimestamp)
                continue

            # 计算当天开市的时间戳
            morningTimestamp,afternoonTimestamp = self.getTradeTimestampInterval(currentDate)
            morningTimestamp[0] = morningTimestamp[0] + 2*60
            afternoonTimestamp[0] = afternoonTimestamp[0] + 2*60

            while True:
                currentDate = self.getCurrentDate()
                currentTimestamp = currentDate.timestamp()

                # 上午时间段 + 下午时间段
                if morningTimestamp[0] <= currentTimestamp <= morningTimestamp[1] or afternoonTimestamp[0] <= currentTimestamp <= afternoonTimestamp[1]:

                    start = datetime.now()
                    if not self.before_strategy_mark:
                        self.before_strategy_mark = self.before_strategy()
                    self.strategy()
                    end = datetime.now()
                    delte = end - start
                    if self.runStrategyInterval - delte.seconds > 0:
                        time.sleep(self.runStrategyInterval - delte.seconds)

                # 上午开市时间段 和 中午休市时间段
                elif currentTimestamp < morningTimestamp[0] or morningTimestamp[1] < currentTimestamp < afternoonTimestamp[0]:
                    if currentTimestamp < morningTimestamp[0]:
                        self.before_strategy_mark = self.before_strategy()
                        log.info(":休眠到开市！")
                        currentDate = self.getCurrentDate()
                        currentTimestamp = currentDate.timestamp()
                        time.sleep(morningTimestamp[0] - currentTimestamp)

                    elif morningTimestamp[1] < currentTimestamp < afternoonTimestamp[0]:
                        if not self.before_strategy_mark:
                            self.before_strategy_mark = self.before_strategy()
                        log.info(":中午休市中,休眠到下午开市")
                        currentDate = self.getCurrentDate()
                        currentTimestamp = currentDate.timestamp()
                        time.sleep(afternoonTimestamp[0]-currentTimestamp)

                # 下午闭市时间段
                elif afternoonTimestamp[1] < currentTimestamp:
                    self.after_strategy()
                    nextDay = self.getNextDay(currentDate)
                    nextDayTimestamp = nextDay.timestamp()
                    currentTimestamp = currentDate.timestamp()
                    log.info(":今日已经闭市，休眠到下一日")
                    currentDate = self.getCurrentDate()
                    currentTimestamp = currentDate.timestamp()
                    time.sleep(nextDayTimestamp - currentTimestamp)
                    break

class Strategy_FluctuationAndNorm(StrategyBase):
    def __init__(self):
        super(Strategy_FluctuationAndNorm,self).__init__()
        self.fund_code_list = ["518880"]
        self.fluctuation_5min_delte = 0.004
        self.sample = 0.05

    def calculate_normal_cdf(self, x, mean=0, std_dev=1):
        """
        计算正态分布的累积分布函数值
        参数:
            x: 要计算的值
            mean: 均值(默认0)
            std_dev: 标准差(默认1)
        返回:
            累积分布概率值
        """
        # 使用scipy的norm.cdf函数计算
        return norm.cdf(x, loc=mean, scale=std_dev)

    def strategy(self):
        try:
            isinstance(self.NormMaxTimestamp_dict,dict)
        except:
            self.NormMaxTimestamp_dict = {}
            self.NormInfo_dict = {}
        try:
            isinstance(self.FluctuationMaxTimestamp_dict,dict)
        except:
            self.FluctuationMaxTimestamp_dict = {}


        for fund_code in self.fund_code_list:
            # 查询波动
            self.FluctuationMaxTimestamp_dict.setdefault(fund_code,0)
            sql = self.databaseOperation.searchSql_maxTimestamp("GoldETFPriceFluctuation","fund_code={}".format(fund_code))
            maxTimestamp = self.databaseOperation.executeSearchSql(sql)[0]["timestamp"]
            if self.FluctuationMaxTimestamp_dict[fund_code] != maxTimestamp:
                sql = self.databaseOperation.searchSql_byTimestamp("GoldETFPriceFluctuation",maxTimestamp,"fund_code={}".format(fund_code))
                Fluctuation = self.databaseOperation.executeSearchSql(sql)[0]
                self.fluctuation_5min = Fluctuation["fluctuation_5min"]
                self.FluctuationMaxTimestamp_dict[fund_code] = maxTimestamp
            else:
                log.info("时间戳重复:{}".format(str(maxTimestamp)))
                continue
            # 查询前一天的均值
            self.NormMaxTimestamp_dict.setdefault(fund_code,0)
            sql = self.databaseOperation.searchSql_maxTimestamp("GoldETFPriceNorm","fund_code={}".format(fund_code))
            maxTimestamp = self.databaseOperation.executeSearchSql(sql)[0]["timestamp"]
            if self.NormMaxTimestamp_dict[fund_code] != maxTimestamp:
                # 
                sql = self.databaseOperation.searchSql_byTimestamp("GoldETFPriceNorm",maxTimestamp,"fund_code={}".format(fund_code))
                norm = self.databaseOperation.executeSearchSql(sql)[0]
                self.NormInfo_dict[fund_code] = {}
                self.NormInfo_dict[fund_code]["average_1day"] = norm["average_1day"]
                self.NormInfo_dict[fund_code]["std_1day"] = norm["std_1day"]
                log.info("{}:均值：{}".format(fund_code,str(norm["average_1day"])))
                log.info("{}:标准差：{}".format(fund_code,str(norm["std_1day"])))

                # 
                maxDatatime = datetime.fromtimestamp(maxTimestamp)
                morningTimestamp,afternoonTimestamp = self.getTradeTimestampInterval(maxDatatime)
                sql = self.databaseOperation.searchSql_ETFprice(afternoonTimestamp[1],afternoonTimestamp[1]-morningTimestamp[0],fund_code)
                row = self.databaseOperation.executeSearchSql(sql)
                row.sort(key=lambda x: x["price"],reverse=False)

                sample_length = int(len(row) * self.sample)
                low_price_prob = self.calculate_normal_cdf(row[sample_length]["price"],self.NormInfo_dict[fund_code]["average_1day"],self.NormInfo_dict[fund_code]["std_1day"])
                high_price_prob = 1 - self.calculate_normal_cdf(row[-sample_length]["price"],self.NormInfo_dict[fund_code]["average_1day"],self.NormInfo_dict[fund_code]["std_1day"])
                self.NormInfo_dict[fund_code]["low_price_prob"] = low_price_prob
                self.NormInfo_dict[fund_code]["high_price_prob"] = high_price_prob

                #
                self.NormMaxTimestamp_dict[fund_code] = maxTimestamp

                log.info("{}:采样数据量：{}".format(fund_code,str(sample_length)))
                log.info("{}:低点概率：{}".format(fund_code,str(low_price_prob)))
                log.info("{}:高点概率：{}".format(fund_code,str(high_price_prob)))
                log.info("---------")
            # 查询当前的价格
            sql = self.databaseOperation.searchSql_byTimestamp("GoldETFPrice",self.FluctuationMaxTimestamp_dict[fund_code],"fund_code={}".format(fund_code))
            row = self.databaseOperation.executeSearchSql(sql)[0]
            price = row["price"]
            date = datetime.fromtimestamp(float(row["timestamp"])).strftime('%Y-%m-%d %H:%M:%S')

            

            # 计算正太分布的概率
            prob = self.calculate_normal_cdf(price, self.NormInfo_dict[fund_code]["average_1day"], self.NormInfo_dict[fund_code]["std_1day"])
            price_high_prob = 1.0 - prob
            price_low_prob = prob
            log.info(json.dumps({"fund_code":fund_code, "price":price,"fluctuation_5min":self.fluctuation_5min,"price_high_prob":price_high_prob,"price_low_prob":price_low_prob}, ensure_ascii=False))

            # 报警条件：价格高点
            if -self.fluctuation_5min_delte < self.fluctuation_5min < self.fluctuation_5min_delte and price_high_prob < self.NormInfo_dict[fund_code]["high_price_prob"]:
                tmp = {"fund_code":fund_code, "priceType":"高点", "price":price,"fluctuation_5min":self.fluctuation_5min,"price_high_prob":price_high_prob,"datetime":date}
                self.robot.sendMessage(tmp, self.robot.transMessage_MarkDown)

            # 报警条件： 价格低
            if -self.fluctuation_5min_delte < self.fluctuation_5min < self.fluctuation_5min_delte and price_low_prob < self.NormInfo_dict[fund_code]["low_price_prob"]:
                tmp = {"fund_code":fund_code, "priceType":"低点", "price":price,"fluctuation_5min":self.fluctuation_5min,"price_low_prob":price_low_prob,"datetime":date}
                self.robot.sendMessage(tmp, self.robot.transMessage_MarkDown)

class Strategy_PriceLowHigh(StrategyBase):
    def __init__(self):
        self.runStrategyInterval = 15 # 价格检索
        super().__init__(self.runStrategyInterval)
        self.fund_code_list = ["518880"]
        
        self.worningTimeInterval = 8*60 # 报警间隔时间
        # 价格划分
        self.sample = 0.20
        self.priceStatus = ""
        self.warnTimestamp = 0

    def computeMiddlePrice(self,price,low,high):
        intervartal = high - low
        normPrice = price - low
        low_prob = normPrice/intervartal
        high_prob = 1.0 - low_prob
        return low_prob,high_prob
    
    def currentPriceStatus(self,low_prob,high_prob):
        if low_prob < self.sample:
            priceStatus = "低点区间"
        elif high_prob < self.sample:
            priceStatus = "高点区间"
        else:
            priceStatus = "中间点区间"
        return priceStatus

    def before_strategy(self):
        self.priceStatus = ""

    def strategy(self):
        
        try:
            isinstance(self.FluctuationMaxTimestamp_dict,dict)
        except:
            self.FluctuationMaxTimestamp_dict = {}

        for fund_code in self.fund_code_list:
            # 查询最大时间戳
            self.FluctuationMaxTimestamp_dict.setdefault(fund_code,0)
            sql = self.databaseOperation.searchSql_maxTimestamp("GoldETFPrice","fund_code={}".format(fund_code))
            maxTimestamp = self.databaseOperation.executeSearchSql(sql)[0]["timestamp"]

            # 
            if self.FluctuationMaxTimestamp_dict[fund_code] == maxTimestamp:
                log.info("时间戳重复:{}".format(str(maxTimestamp)))
                continue
            else:
                self.FluctuationMaxTimestamp_dict[fund_code] = maxTimestamp

            # 查询当前的价格和位置
            sql = self.databaseOperation.searchSql_byTimestamp("GoldETFPrice",self.FluctuationMaxTimestamp_dict[fund_code],"fund_code={}".format(fund_code))
            print(sql)
            row = self.databaseOperation.executeSearchSql(sql)[0]
            price = row["price"]
            date = datetime.fromtimestamp(float(row["timestamp"])).strftime('%Y-%m-%d %H:%M:%S')
            low = row["low"]
            high = row["high"]
            low_prob,high_prob = self.computeMiddlePrice(price,low,high)
            priceStatus = self.currentPriceStatus(low_prob,high_prob)

            log.info(json.dumps({"fund_code":fund_code, "price":price,"低价占比":low_prob,"高价占比":high_prob}, ensure_ascii=False))

            # 状态改变就报警
            if priceStatus != self.priceStatus:
                tmp = {"fund_code":fund_code, "priceType":priceStatus, "price":price, "data":date}
                self.priceStatus = priceStatus
                self.robot.sendMessage(tmp, self.robot.transMessage_MarkDown)
                self.warnTimestamp = row["timestamp"]
            # 超过报警时间间隔就报警
            if row["timestamp"] - self.warnTimestamp >= self.worningTimeInterval:
                tmp = {"fund_code":fund_code, "priceType":priceStatus, "price":price, "data":date}
                self.robot.sendMessage(tmp, self.robot.transMessage_MarkDown)
                self.warnTimestamp = row["timestamp"]

class Strategy_FluctuationAndTradeNum(StrategyBase):
    def __init__(self):
        self.runStrategyInterval = 15
        super().__init__(self.runStrategyInterval)
        self.fund_code_list = ["518880"]
        
        self.sample = 0.15
        self.fluctuation_key = "fluctuation_4min"
        self.fluctuation_delte = 0.02 # 波动每克两毛

    def strategy(self):
        
        try:
            isinstance(self.FluctuationMaxTimestamp_dict,dict)
        except:
            self.FluctuationMaxTimestamp_dict = {}

        for fund_code in self.fund_code_list:
            # 查询波动
            self.FluctuationMaxTimestamp_dict.setdefault(fund_code,0)
            sql = self.databaseOperation.searchSql_maxTimestamp("GoldETFPriceFluctuation","fund_code={}".format(fund_code))
            maxTimestamp = self.databaseOperation.executeSearchSql(sql)[0]["timestamp"]

            # 数据未更新，跳过。
            if self.FluctuationMaxTimestamp_dict[fund_code] == maxTimestamp:
                log.info("时间戳重复:{}".format(str(maxTimestamp)))
                continue

            # 查询波动
            sql = self.databaseOperation.searchSql_byTimestamp("GoldETFPriceFluctuation",maxTimestamp,"fund_code={}".format(fund_code))
            Fluctuation = self.databaseOperation.executeSearchSql(sql)[0]
            self.fluctuation = Fluctuation[self.fluctuation_key]

            # 更新查询时间
            self.FluctuationMaxTimestamp_dict[fund_code] = maxTimestamp


            # 查询当前的价格
            sql = self.databaseOperation.searchSql_byTimestamp("GoldETFPrice",self.FluctuationMaxTimestamp_dict[fund_code],"fund_code={}".format(fund_code))
            row = self.databaseOperation.executeSearchSql(sql)[0]
            price = row["price"]
            date = datetime.fromtimestamp(float(row["timestamp"])).strftime('%Y-%m-%d %H:%M:%S')

            # 
            log.info(json.dumps({"fund_code":fund_code, "price":price, self.fluctuation_key:self.fluctuation}, ensure_ascii=False))

            # 报警条件：价格高点+四分钟波动2毛
            if -self.fluctuation_delte < self.fluctuation < self.fluctuation_delte:
                tmp = {"fund_code":fund_code, "priceType":"稳定波动", "price":price, self.fluctuation_key:self.fluctuation, "data":date}
                self.robot.sendMessage(tmp, self.robot.transMessage_MarkDown)

class Strategy_PointPrice(StrategyBase):
    # 指定价格报警。。
    def __init__(self):
        self.runStrategyInterval = 15
        super().__init__(self.runStrategyInterval)
        self.fund_code_list = ["518880","588000"]
        self.PointPrice_path = "/home/jianrui/Gold/conf/wornning.json"

    def before_strategy(self):
        self.readPointPrice()

    def readPointPrice(self):
        # 读取报警信息
        self.PointPrice = {}
        with open(self.PointPrice_path,"r") as f:
            for i in f:
                i = json.loads(i.strip())
                if i["status"] == "开启":
                    self.PointPrice[i["id"]] = i
        log.info("读取报警条件完成!")

    def updataPointPrice(self):

        try:
            isinstance(self.PointPrice,dict)
        except:
            self.readPointPrice()
            log.info("读取报警信息")

        # 更新报警信息
        with open(self.PointPrice_path,"r") as f:
            for i in f:
                i = json.loads(i.strip())
                if i["id"] not in self.PointPrice and i["status"]=="开启":
                    self.PointPrice[i["id"]] = i
    
    def writePointPrice(self):
        # 写出报警价格信息
        with open(self.PointPrice_path,"w") as f:
            for k,v in self.PointPrice.items():
                if v["status"] == "开启":
                    f.write(json.dumps(v,ensure_ascii=False)+"\n")

    def strategy(self):
        
        try:
            isinstance(self.FluctuationMaxTimestamp_dict,dict)
        except:
            self.FluctuationMaxTimestamp_dict = {}


        self.readPointPrice()
        log.info("当前条件："+json.dumps(self.PointPrice, ensure_ascii=False))

        for fund_code in self.fund_code_list:
            # 查询波动
            self.FluctuationMaxTimestamp_dict.setdefault(fund_code,0)

            # 查询波动
            sql = self.databaseOperation.searchSql_maxTimestamp("GoldETFPrice","fund_code={}".format(fund_code))
            maxTimestamp = self.databaseOperation.executeSearchSql(sql)[0]["timestamp"]

            # 数据未更新，跳过。
            if self.FluctuationMaxTimestamp_dict[fund_code] == maxTimestamp:
                log.info("时间戳重复:{}".format(str(maxTimestamp)))
                continue

            # 查询当前的价格
            sql = self.databaseOperation.searchSql_byTimestamp("GoldETFPrice",maxTimestamp,"fund_code={}".format(fund_code))

            row = self.databaseOperation.executeSearchSql(sql)[0]
            price = row["price"]
            date = datetime.fromtimestamp(float(row["timestamp"])).strftime('%Y-%m-%d %H:%M:%S')

            # 更新查询时间
            self.FluctuationMaxTimestamp_dict[fund_code] = maxTimestamp

            # 判断是否命中条件
            hitPoint = self.isWorning(price,self.PointPrice,fund_code)

            # 报警
            if hitPoint:
                tmp = {"fund_code":fund_code, "condition":hitPoint[0]["condition"],"price":price, "data":date,"info":hitPoint[0]["info"]}
                self.robot.sendMessage(tmp, self.robot.transMessage_Point)
        
        self.updataPointPrice()
        self.writePointPrice()

    def after_strategy(self):
        # 写出报警价格信息
        with open(self.PointPrice_path,"w") as f:
            for k,v in self.PointPrice.items():
                v["onlineday"] = v["onlineday"] - 1
                if v["onlineday"] < 1:
                    v["status"] = "关闭"
                if v["status"] == "开启":
                    f.write(json.dumps(v,ensure_ascii=False)+"\n")

    def isWorning(self,price,PointPrice,fund_code):
        PointPrice = {k:v for k,v in PointPrice.items() if v["fund_code"]==fund_code}
        hitPoint = []
        for k,v in PointPrice.items():
            condition = v["condition"].replace(" ","")
            condition_list = condition.split(",")
            mark = False
            if v["status"] == "开启":
                for tmp_condition in condition_list:
                    condition,thre = tmp_condition[:2],float(tmp_condition[2:])
                    for con in condition:
                        if con == ">":
                            if price > thre:
                                mark = True
                        elif con == "<":
                            if price < thre:
                                mark = True
                        elif con == "=":
                            if price == thre:
                                mark = True
            if mark:
                hitPoint.append(v)
        for v in hitPoint:
            v["time"] = v["time"] - 1
            if v["time"] < 1:
                v["status"] = "关闭"
        return hitPoint
        
class Strategy_PriceReBound(StrategyBase):
    # 价格回调报警
    def __init__(self):
        self.runStrategyInterval = 15
        super().__init__(self.runStrategyInterval)
        self.fund_code_list = ["518880"]
        self.PointPrice_path = "C:\\Users\\Administrator\\Desktop\\Gold\\conf\\wornning_ReBound.json"

    def before_strategy(self):
        self.readPointPrice()
        # 设置状态
        self.isWatchingStatus = False

    def readPointPrice(self):
        # 读取报警信息
        self.PointPrice = {}
        with open(self.PointPrice_path,"r") as f:
            for i in f:
                i = json.loads(i.strip())
                if i["status"] == "开启":
                    self.PointPrice[i["id"]] = i
        log.info("读取报警条件完成!")

    def strategy(self):
        # 初始化 时间戳，用来标记已经记录的时间
        try:
            isinstance(self.FluctuationMaxTimestamp_dict,dict)
        except:
            self.FluctuationMaxTimestamp_dict = {}

        # 读取条件
        self.readPointPrice()
        log.info("当前条件："+json.dumps(self.PointPrice, ensure_ascii=False))

        for fund_code in self.fund_code_list:

            # 初始化波动表的最大时间戳
            self.FluctuationMaxTimestamp_dict.setdefault(fund_code,0)

            # 查询表中最大时间戳
            sql = self.databaseOperation.searchSql_maxTimestamp("GoldETFPrice","fund_code={}".format(fund_code))
            maxTimestamp = self.databaseOperation.executeSearchSql(sql)[0]["timestamp"]

            # 数据未更新，跳过。
            if self.FluctuationMaxTimestamp_dict[fund_code] == maxTimestamp:
                log.info("时间戳重复:{}".format(str(maxTimestamp)))
                continue

            # 查询当前的价格
            sql = self.databaseOperation.searchSql_byTimestamp("GoldETFPrice",maxTimestamp,"fund_code={}".format(fund_code))

            row = self.databaseOperation.executeSearchSql(sql)[0]
            price = row["price"]
            date = datetime.fromtimestamp(float(row["timestamp"])).strftime('%Y-%m-%d %H:%M:%S')

            # 更新查询时间
            self.FluctuationMaxTimestamp_dict[fund_code] = maxTimestamp

            # 判断是否满足启动反弹检测的条件
            if not self.isWatchingStatus:
                self.watchingPoint = self.isWorning(price,self.PointPrice)
            
            # 启动
            if not self.isWatchingStatus and self.watchingPoint:
                self.isWatchingStatus = True
                self.FanTan_or_HuiTiao = self.isFanTanOrHuiTiao(self.watchingPoint[0])
                self.WatchingPrice = price
                self.prewarning_num = 0
            # 已经启动，进入到检测阶段
            elif self.isWatchingStatus:
                if self.FanTan_or_HuiTiao == "<": # 等待触底反弹
                    # 当前价格小于watchingprice时，更新watchingprice。
                    # 当前价格高于watchingprice价格2毛三次时报警。
                    # 当前价格高于watchingprice价格3毛时，立刻报警。 
                    if self.WatchingPrice > price:
                        self.WatchingPrice = price
                        self.prewarning_num = 0
                    elif price - self.WatchingPrice == 0.002:
                        self.prewarning_num += 1
                    elif price - self.WatchingPrice >= 0.003:
                        self.prewarning_num = 3

                elif self.FanTan_or_HuiTiao == ">": # 等待价高回落
                    if self.WatchingPrice < price:
                        self.WatchingPrice = price
                        self.prewarning_num = 0
                    elif self.WatchingPrice - price == 0.002:
                        self.prewarning_num += 1
                    elif self.WatchingPrice - price >= 0.003:
                        self.prewarning_num = 3
                
                # 通过prewarning_num次数判断是否需要报警
                if self.prewarning_num >= 3:
                    tmp = {"fund_code":fund_code, "condition":self.watchingPoint[0]["condition"],"price":price, "data":date,"info":self.watchingPoint[0]["info"]}
                    self.robot.sendMessage(tmp, self.robot.transMessage_PointReBond)
        self.updataPointPrice()
        self.writePointPrice()
          
    def updataPointPrice(self):

        try:
            isinstance(self.PointPrice,dict)
        except:
            self.readPointPrice()
            log.info("读取报警信息")

        # 更新报警信息
        with open(self.PointPrice_path,"r") as f:
            for i in f:
                i = json.loads(i.strip())
                if i["id"] not in self.PointPrice and i["status"]=="开启":
                    self.PointPrice[i["id"]] = i

    def writePointPrice(self):
        # 写出报警价格信息
        with open(self.PointPrice_path,"w") as f:
            for k,v in self.PointPrice.items():
                if v["status"] == "开启":
                    f.write(json.dumps(v,ensure_ascii=False)+"\n")

    def isWatching(self,price,PointPrice):
        hitPoint = []
        for k,v in PointPrice.items():
            condition = v["condition"].replace(" ","")
            condition_list = condition.split(",")
            mark = False
            if v["status"] == "开启":
                for tmp_condition in condition_list:
                    condition,thre = tmp_condition[:2],float(tmp_condition[2:])
                    for con in condition:
                        if con == ">":
                            if price > thre:
                                mark = True
                        elif con == "<":
                            if price < thre:
                                mark = True
                        elif con == "=":
                            if price == thre:
                                mark = True
            if mark:
                hitPoint.append(v)
        for v in hitPoint:
            v["time"] = v["time"] - 1
            if v["time"] < 1:
                v["status"] = "关闭"
        return hitPoint
    
    def isFanTanOrHuiTiao(self,one):
        if ">" in one["condition"]:
            return "<"
        elif "<" in one["condition"]:
            return ">"

    def after_strategy(self):
        # 写出报警价格信息
        with open(self.PointPrice_path,"w") as f:
            for k,v in self.PointPrice.items():
                v["onlineday"] = v["onlineday"] - 1
                if v["onlineday"] < 1:
                    v["status"] = "关闭"
                if v["status"] == "开启":
                    f.write(json.dumps(v,ensure_ascii=False)+"\n")

class Strategy_LowPriceAndHighPrice_Line_Prediction(StrategyBase):
    def __init__(self):
        self.runStrategyInterval = 15 # 价格检索
        super().__init__(self.runStrategyInterval)
        self.fund_code_list = ["518880","588000"]
        self.last_k = 5
        self.wornning_file = "/home/jianrui/Gold/conf/wornning.json"
    


    def linear_fit(self, x, y):
        """
        对给定的 x 和 y 数据进行线性拟合（y = a*x + b）

        参数:
            x (array-like): 自变量数据
            y (array-like): 因变量数据
            plot (bool): 是否绘制拟合曲线

        返回:
            predict (function): 输入新的 x 值返回预测 y 值的函数
        """
        # 转换为numpy数组
        x = np.array(x)
        y = np.array(y)

        # 使用最小二乘法拟合： y = a*x + b
        A = np.vstack([x, np.ones(len(x))]).T
        a, b = np.linalg.lstsq(A, y, rcond=None)[0]

        # 生成预测函数
        predict = lambda new_x: a * np.array(new_x) + b

        return predict


    def before_strategy(self):
        pass

    def strategy(self):
        pass

    def after_strategy(self):
        # 休眠一个小时
        time.sleep(3600*5)

        # 读取当天的K线数据
        for fund_code  in self.fund_code_list:
            k_df = adata.fund.market.get_market_etf(fund_code=fund_code)
            last_k_dict = k_df.to_dict(orient="records")[-self.last_k:]
            print(last_k_dict)
            high_list = [float(i["high"]) for i in last_k_dict]
            print(high_list)
            low_list = [float(i["low"]) for i in last_k_dict]
            print(low_list)
            predict_high_func = self.linear_fit(x=[i for i in range(0,self.last_k)], y=high_list)
            predict_low_func = self.linear_fit(x=[i for i in range(0,self.last_k)], y=low_list)
            predict_high = predict_high_func(self.last_k)
            predict_low = predict_low_func(self.last_k)
            wornning = {"condition": "<={low}".format(low=str(predict_low*100)), "time": 5, "onlineday": 1, "fund_code": fund_code, "id": "auto_"+"".join(random.sample(self.RANDOM_STR,10)), "status": "开启", "datatime": datetime.now().strftime("%Y-%m-%d"), "info": datetime.now().strftime("%Y-%m-%d")+"价格报警"}
            self.set_worning(wornning,self.wornning_file)

class Strategy_MeanLineAndVolume(StrategyBase):
    def __init__(self):
        self.runStrategyInterval = 30 # 价格检索间隔
        super().__init__(self.runStrategyInterval)
        self.HpParam_path = "./CallBack/conf/MeanLineAndVolume.jsonl"

    def read_HpParam(self,HpParam_path):
        # 读取超参数配置信息
        with open(HpParam_path,"r") as f:
            HpParam_list = [json.loads(i.strip()) for i in f]
            # HpParam_list = [i for i in HpParam_list if i["fund_code"]== "159766.SZ"]
        HpParam_dict = {i["fund_code"]:i for i in HpParam_list}
        return HpParam_dict
    
    def _request_post(self,**kwargs):
        response = requests.post("http://106.13.59.142:6010/download_history_data",json=kwargs)
        while response.status_code != 200:
            time.sleep(range.sample([i for i in range(30,69)],1)[0])
            response = requests.post("http://106.13.59.142:6010/download_history_data",json=kwargs)
            log.info("该参数在获取数据时，暴露问题:"+kwargs["stock_code"])
        buffer = io.BytesIO(response.content)
        df = pd.read_pickle(buffer)
        index = set([i[:8] for i in df.index.to_list()])
        index = [int(i) for i in index]
        index.sort(key=lambda x:x)
        log.info("数据获取成功，参数如下:"+kwargs["stock_code"]+","+str(index))
        return df

    def build_current_day_param(self,kwargs):
        # 构造检索的参数值
        mean_long_day = kwargs["mean_long_day"]
        mean_short_day = kwargs["mean_short_day"]
        volume_day = kwargs["Volume_day"]
        max_day = max(mean_long_day,mean_short_day,volume_day)

        # 从QMT读取行情信息
        QMT_kwargs = {}
        QMT_kwargs["field_list"] = ["time","open","close","low","high","volume","preClose"]
        QMT_kwargs["stock_code"] = kwargs["fund_code"]
        QMT_kwargs["incrementally"] = False

        # 获取1d数据
        QMT_kwargs["period"] = "1d"
        QMT_kwargs["count"] = max_day
        df_k_1d = self._request_post(**QMT_kwargs)
        df_k_1d = df_k_1d.to_dict(orient="index")
        df_k_1d_key = [i for i in df_k_1d.keys()]

        # 计算金叉的最小值
        mode = 2
        if mode == 1:
            com_day_long = df_k_1d_key[-mean_long_day:]
            df_k_1d_tmp_long = [df_k_1d[i] for i in com_day_long]
            com_day_short = df_k_1d_key[-mean_short_day:]
            df_k_1d_tmp_short = [df_k_1d[i] for i in com_day_short]
            meanLong = sum([i["close"] for i in df_k_1d_tmp_long])/(mean_long_day+1)
            meanShort = sum([i["close"] for i in df_k_1d_tmp_short])/(mean_short_day+1)
            min_price = (mean_long_day+1)*(mean_short_day+1)*(meanLong-meanShort)/(mean_long_day-mean_short_day)
            kwargs["min_price"] = min_price
        elif mode == 2:
            com_day_long = df_k_1d_key[-mean_long_day:]
            df_k_1d_tmp_long = [df_k_1d[i] for i in com_day_long]
            com_day_short = df_k_1d_key[-mean_short_day:]
            df_k_1d_tmp_short = [df_k_1d[i] for i in com_day_short]
            meanLong = sum([i["close"] for i in df_k_1d_tmp_long])/(mean_long_day)
            sumShort = sum([i["close"] for i in df_k_1d_tmp_short])
            min_price = meanLong*((mean_short_day+1)) - sumShort
            kwargs["min_price"] = min_price

        # 计算前一天的收盘价
        yesterday_key = df_k_1d_key[-1]
        yesterday_1d = df_k_1d[yesterday_key]
        yesterday_price = yesterday_1d["preClose"]
        kwargs["preClose"] = yesterday_price

        # 计算柏林带
        


        # 计算交易量的平均演变过程
        com_day_volume = df_k_1d_key[-volume_day:]
        QMT_kwargs["period"] = "1m"
        QMT_kwargs.pop("count")
        QMT_kwargs["start_time"] = com_day_volume[0]
        QMT_kwargs["end_time"] = com_day_volume[-1]
        df_k_5m = self._request_post(**QMT_kwargs)
        df_k_5m = df_k_5m.to_dict(orient="index")
        df_k_5m_divideByDay = {}
        for k,v in df_k_5m.items():
            day = k[:8]
            df_k_5m_divideByDay.setdefault(day,[])
            df_k_5m_divideByDay[day].append(v)
        df_k_5m_key = [i for i in df_k_5m_divideByDay.keys()]
        df_k_5m_divideByDay_tmp = [df_k_5m_divideByDay[i] for i in com_day_volume]
        df_k_5m_divideByDay_tmp_dict = {}
        for tmp in df_k_5m_divideByDay_tmp:
            for i in tmp:
                time_int = int(datetime.fromtimestamp(int(i["time"])/1000).strftime("%H%M%S"))
                df_k_5m_divideByDay_tmp_dict.setdefault(time_int,[])
                df_k_5m_divideByDay_tmp_dict[time_int].append(i)
        tmp_sum = 0
        df_k_5m_volume_mean = {}
        for k,v in df_k_5m_divideByDay_tmp_dict.items():
            tmp_sum = tmp_sum + statistics.mean([i["volume"] for i in v])
            df_k_5m_volume_mean[k] = tmp_sum
        kwargs["df_k_5m_volume_mean"] = df_k_5m_volume_mean

        return kwargs
        
    def before_strategy(self):
        # 休眠一个小时
        # time.sleep(60)

        # 读取超参数
        self.HpParam_dict = self.read_HpParam(self.HpParam_path)
        self.fund_code_list = list(self.HpParam_dict.keys())
        self.fund_code_dict = {i:False for i in self.fund_code_list}
        with mp.Pool(processes=1) as pool:
            tmp = pool.map(self.build_current_day_param,list(self.HpParam_dict.values()))
            self.HpParam_dict = {k:v for k,v in zip(self.fund_code_list,tmp)}
        self.fund_code_group = []
        for i in range(0,len(self.fund_code_list),50):
            self.fund_code_group.append(self.fund_code_list[i:i+50])
        return True

    def strategy(self):
        start = time.time()
        for fund_codes in self.fund_code_group:
            try:
                df = ts.realtime_quote(ts_code=",".join(fund_codes), src='sina')
                df.to_json("tmp.json",orient="records",lines=True)
            except Exception as e:
                data = {"date":self.getCurrentDate().strftime('%Y-%m-%d %H:%M:%S'),"错误类型":str(e)}
                self.robot.sendMessage(data, self.robot.transMessage_dataCraw )
                time.sleep(60)
            df = df.to_dict(orient="records")
            
            for one_code in df:
                # 参数抄写出来
                name = one_code["NAME"]
                fund_code = one_code["TS_CODE"]
                date = one_code["DATE"]+" "+one_code["TIME"]
                price = float(one_code["PRICE"])
                volume = int(one_code["VOLUME"])/100
                date_time = int(datetime.strptime(date, '%Y%m%d %H:%M:%S').strftime("%H%M%S"))
                HpParam = self.HpParam_dict[fund_code]
                df_k_5m_volume_mean = HpParam["df_k_5m_volume_mean"][(date_time//100)*100]
                min_price = HpParam["min_price"]
                ShouYi = HpParam["ShouYi"]
                ZhiShun = HpParam["ZhiShun"]
                mean_keep_day = HpParam["mean_keep_day"]
                precesion = HpParam["precion"]
                preClose = HpParam["preClose"]

                # 输出
                info = copy.deepcopy(HpParam)
                info["df_k_5m_volume_mean"] = df_k_5m_volume_mean
                info["price"] = price
                info["price_diff"] = self.fund_code_dict[fund_code]
                info["volume"] = volume
                log.info(json.dumps(info,ensure_ascii=False))

                # 计算均值 且 成交量大于过去的均值
                if min_price > 0 and \
                        price > min_price and \
                        volume > df_k_5m_volume_mean and \
                        self.fund_code_dict[fund_code] != True \
                        and min_price > preClose :
                        # and self.fund_code_dict[fund_code] < 0:
                    
                    ShouYi_price = price*(1+ShouYi)
                    ZhiShun_price = price*(1+ZhiShun)
                    post_data = {
                        "fund_code" : fund_code,
                        "condition" : ">"+str(min_price),
                        "price" : str(price),
                        "name": name,
                        "收益线" : str(ShouYi_price),
                        "止损线" : str(ZhiShun_price),
                        "平均持有时间": str(mean_keep_day),
                        "回测准确率": str(precesion),
                        "报警时间": self.getCurrentDate().strftime('%Y-%m-%d %H:%M:%S')
                    }
                    self.robot.sendMessage(post_data,self.robot.transMessage_MeanLineAndVolume)
                    self.fund_code_dict[fund_code] = True
                elif self.fund_code_dict[fund_code] != True:
                    self.fund_code_dict[fund_code] = price - min_price
        end = time.time()

        

    def after_strategy(self):
        pass

if __name__ == "__main__":
    strategy = Strategy_MeanLineAndVolume()
    strategy.before_strategy()
    # print(strategy.HpParam_dict)
    # strategy.strategy()
    # strategy.after_strategy()

            


        
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
from tqdm import tqdm

import tushare as ts
ts.set_token("08aaec36d1fad0154592971c8fb64a472b8f3fe954dc33c8d0e87447")
from rtq import  (realtime_quote, realtime_list)
from histroy_divide import (realtime_tick)
ts.realtime_quote = realtime_quote
ts.realtime_list = realtime_list
ts.realtime_tick = realtime_tick

from WriteToTensorboard import fund_amount_and_price

logging.basicConfig(format='%(asctime)s %(message)s')
log = logging.getLogger(name="log")
log.setLevel(level=logging.INFO)

class StrategyBase():
    def __init__(self,runStrategyInterval):
        self.robot = Robot()
        # self.databaseOperation = DatabaseOperation()
        self.tradeCalender, self.tradeYear = self.getTradeCalender()
        self.runStrategyInterval = runStrategyInterval # 检测策略的间隔
        self.RANDOM_STR = "ZXCVBNMASDFGHJKLQWERTYUIOP1234567890qwertyuiopasdfghjklzxcvbnm"
        self.before_strategy_mark = False

        self.pro = ts.pro_api('3085222731857622989')
        self.pro._DataApi__http_url = "http://47.109.97.125:8080/tushare"

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
        last_calender = adata.stock.info.trade_calendar(year=str(int(year)-1))
        last_calender = last_calender.to_dict(orient="records")
        last_calender = {i["trade_date"]:i for i in last_calender}
        calender = {**calender,**last_calender}
        return calender, year
    
    def getNextDay(self,date):
        tmp_timestamp = date.timestamp() + 24*60*60
        nextDay = datetime.fromtimestamp(tmp_timestamp)
        nextDay = nextDay.strftime("%Y-%m-%d")
        nextDay = nextDay + " 00:00:01"
        nextDay = datetime.strptime(nextDay,'%Y-%m-%d %H:%M:%S')
        return nextDay
    
    def getyesterday(self,date):
        tmp_timestamp = date.timestamp() - 24*60*60
        yesterday = datetime.fromtimestamp(tmp_timestamp)
        yesterday = yesterday.strftime("%Y-%m-%d")
        yesterday = yesterday + " 01:00:00"
        yesterday = datetime.strptime(yesterday,'%Y-%m-%d %H:%M:%S')
        return yesterday
    
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

    def strategy(self):
        pass

    def after_strategy(self):
        pass

    def Timestamp_fix(self,morningTimestamp,afternoonTimestamp):
        return morningTimestamp,afternoonTimestamp

    def _request_post(self,**kwargs):
        response = requests.post("http://106.13.59.142:6010/download_history_data",json=kwargs)
        while response.status_code != 200:
            time.sleep(random.sample([i for i in range(30,69)],1)[0])
            response = requests.post("http://106.13.59.142:6010/download_history_data",json=kwargs)
            log.info("该参数在获取数据时，暴露问题:"+kwargs["stock_code"])
        buffer = io.BytesIO(response.content)
        df = pd.read_pickle(buffer)
        index = set([i[:8] for i in df.index.to_list()])
        index = [int(i) for i in index]
        index.sort(key=lambda x:x)
        log.info("数据获取成功，参数如下:"+kwargs["stock_code"]+","+str(index))
        return df
    
    def _request_post_index_weight(self,**kwargs):
        response = requests.post("http://106.13.59.142:6010/get_fund_info_with_index_weight",json=kwargs)
        while response.status_code != 200:
            time.sleep(random.sample([i for i in range(30,69)],1)[0])
            response = requests.post("http://106.13.59.142:6010/get_fund_info_with_index_weight",json=kwargs)
            log.info("该参数在获取数据时，暴露问题:"+kwargs["fund_code"])
        fund_info = response.json()
        log.info("数据获取成功，参数如下:"+kwargs["fund_code"])
        return fund_info

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
            # 对监控时间进行微调
            morningTimestamp,afternoonTimestamp = self.Timestamp_fix(morningTimestamp,afternoonTimestamp)

            while True:
                currentDate = self.getCurrentDate()
                currentTimestamp = currentDate.timestamp()

                # 上午开市时间段 + 下午开市时间段
                if morningTimestamp[0] <= currentTimestamp <= morningTimestamp[1] or afternoonTimestamp[0] <= currentTimestamp <= afternoonTimestamp[1]:
                    start = datetime.now()
                    if not self.before_strategy_mark:
                        self.before_strategy_mark = self.before_strategy()
                    self.strategy()
                    end = datetime.now()
                    delte = end - start
                    if self.runStrategyInterval - delte.seconds > 0:
                        time.sleep(self.runStrategyInterval - delte.seconds)

                # 上午开市前时间
                if currentTimestamp < morningTimestamp[0]:
                    self.before_strategy_mark = self.before_strategy()
                    log.info(":休眠到开市！")
                    currentDate = self.getCurrentDate()
                    currentTimestamp = currentDate.timestamp()
                    if morningTimestamp[0] - currentTimestamp > 0:
                        time.sleep(morningTimestamp[0] - currentTimestamp)
                        log.info(":苏醒")
                    
                # 中午休市时间段
                if morningTimestamp[1] < currentTimestamp < afternoonTimestamp[0]:
                    if not self.before_strategy_mark:
                        self.before_strategy_mark = self.before_strategy()
                    log.info(":中午休市中,休眠到下午开市")
                    currentDate = self.getCurrentDate()
                    currentTimestamp = currentDate.timestamp()
                    if afternoonTimestamp[0]-currentTimestamp:
                        time.sleep(afternoonTimestamp[0]-currentTimestamp)
                        log.info(":苏醒")

                # 下午闭市后时间段
                if afternoonTimestamp[1] < currentTimestamp:
                    currentDate = self.getCurrentDate()
                    nextDay = self.getNextDay(currentDate)
                    nextDayTimestamp = nextDay.timestamp()
                    self.after_strategy()
                    currentDate = self.getCurrentDate()
                    currentTimestamp = currentDate.timestamp()
                    currentTimestamp = currentDate.timestamp()
                    if nextDayTimestamp - currentTimestamp > 0:
                        time.sleep(nextDayTimestamp - currentTimestamp)
                        log.info(":今日已经闭市，休眠到下一日")
                        log.info(":苏醒")
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
        self.PointPrice_path = "./conf/wornning.json"

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
        self.fund_code_list_path = "./conf/wornning.json"

    def before_strategy(self):
        self.readPointPrice()
        # 设置状态
        self.isWatchingStatus = False

    def readPointPrice(self):
        # 读取报警信息
        self.PointPrice = {}
        with open(self.fund_code_list_path,"r") as f:
            for i in f:
                i = json.loads(i.strip())
                if i["status"] == "开启" and i["type"] == "价格反转检测":
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
                if i["id"] not in self.PointPrice and i["status"]=="开启" and i["type"] == "价格反转检测":
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
        self.static_day = 5 # 统计资金流向的天数

    def Timestamp_fix(self,morningTimestamp,afternoonTimestamp):
        morningTimestamp[0] = morningTimestamp[0] + 2*60 # 延迟2分钟开始计算
        afternoonTimestamp[0] = afternoonTimestamp[0] + 2*60 # 延迟2分钟开始计算
        return morningTimestamp,afternoonTimestamp

    def read_HpParam(self,HpParam_path):
        # 读取超参数配置信息
        with open(HpParam_path,"r") as f:
            HpParam_list = [json.loads(i.strip()) for i in f]
            # HpParam_list = [i for i in HpParam_list if i["fund_code"]== "563020.SH"]
        HpParam_dict = {i["fund_code"]:i for i in HpParam_list}
        return HpParam_dict

    def build_current_day_param(self,kwargs):
        # 构造检索的参数值
        fund_code = kwargs["fund_code"]
        mean_long_day = kwargs["mean_long_day"]
        mean_short_day = kwargs["mean_short_day"]
        volume_day = kwargs["Volume_day"]
        max_day = max(mean_long_day,mean_short_day,volume_day)

        # 从QMT读取行情信息
        QMT_kwargs = {}
        QMT_kwargs["field_list"] = ["time","open","close","low","high","volume","preClose"]
        QMT_kwargs["stock_code"] = fund_code
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
        yesterday_price = yesterday_1d["close"]
        kwargs["preClose"] = yesterday_price
        
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
            day = k[:8]  # 前8个字符是年月日
            df_k_5m_divideByDay.setdefault(day,[])
            df_k_5m_divideByDay[day].append(v)
        df_k_5m_key = [i for i in df_k_5m_divideByDay.keys()]
        df_k_5m_divideByDay_tmp = [df_k_5m_divideByDay[i] for i in df_k_5m_key]
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

        # 资金流向情况
        self.static_day = 5
        fund_info = self._request_post_index_weight(**{"reload":False,"fund_code":fund_code})
        Timestamp = self.getCurrentDate()
        day_list = []
        while self.static_day > len(day_list):
            Timestamp = self.getyesterday(Timestamp)
            if self.tradeCalender[Timestamp.strftime("%Y-%m-%d")]["trade_status"] == 1:
                day_list.append(Timestamp.strftime("%Y%m%d"))
        start_day = day_list[-1]
        end_day = day_list[0]
        if "指数权重" in fund_info and fund_info["指数权重"] != "无":
            weight = fund_info["指数权重"]
            net_mf_amount = 0
            for we in weight:
                net_mf_amount += self.get_moneyflow(we["con_code"],start_day,end_day)
        elif "指数权重" in fund_info and fund_info["指数权重"] == "无":
            net_mf_amount = -9999
            print(fund_info)
            print("该指数不再权重计算范围内")
        else:
            net_mf_amount = 0
            print(fund_info)
        kwargs["net_mf_amount"] = net_mf_amount

        return kwargs

    def get_moneyflow(self,fund_code,start_day,end_day):
        if fund_code not in self.net_mf_amount_dict:
            df = self.pro.moneyflow(ts_code=fund_code, start_date=start_day, end_date=end_day)
            df = df.to_dict(orient="records")
            net_mf_amount = 0
            for i in df:
                net_mf_amount += i["net_mf_amount"]
            self.net_mf_amount_dict[fund_code] = net_mf_amount
            time.sleep(0.15)
        return self.net_mf_amount_dict[fund_code]
        
    def static_HpParam(self,HpParam_dict):
        tmp = {}
        tmp_param_list = [v for v in HpParam_dict.values()]
        tmp["总数"] = len(tmp_param_list)
        tmp["(有效)"] = ""
        tmp["(无效)"] = ""
        tmp_param_list = [i for i in tmp_param_list if i["min_price"] >= i["preClose"]]
        tmp["金叉线小于收盘线"] = tmp["总数"] - len(tmp_param_list)
        tmp_param_list = [i for i in tmp_param_list if i["net_mf_amount"] < 0 ]
        tmp["资金流向为负数"] = tmp["总数"] - len(tmp_param_list)
        tmp["(有效)"] = len(tmp_param_list)
        tmp["(无效)"] = tmp["总数"] - tmp["(有效)"]
        tmp["统计时间"]= self.getCurrentDate().strftime('%Y-%m-%d %H:%M:%S')

        self.robot.sendMessage(tmp,self.robot.transMessage_StaticInfo)

    def before_strategy(self):

        # 读取超参数
        self.HpParam_dict = self.read_HpParam(self.HpParam_path)
        self.fund_code_list = list(self.HpParam_dict.keys())
        self.fund_code_dict = {i:False for i in self.fund_code_list}
        self.net_mf_amount_dict = {}  # 记录股票资金流入情况的缓存

        # with mp.Pool(processes=1) as pool:
        #     tmp = pool.map(self.build_current_day_param,list(self.HpParam_dict.values()))
        #     self.HpParam_dict = {k:v for k,v in zip(self.fund_code_list,tmp)}

        tmp = []
        for i in list(self.HpParam_dict.values()):
            j = self.build_current_day_param(i)
            tmp.append(j)
        self.HpParam_dict = {k:v for k,v in zip(self.fund_code_list,tmp)}

        self.static_HpParam(self.HpParam_dict)

        self.fund_code_group = []
        for i in range(0,len(self.fund_code_list),50):
            self.fund_code_group.append(self.fund_code_list[i:i+50])
        return True

    def strategy(self):
        for fund_codes in self.fund_code_group:
            try:
                df = ts.realtime_quote(ts_code=",".join(fund_codes), src='sina')
            except Exception as e:
                data = {"date":self.getCurrentDate().strftime('%Y-%m-%d %H:%M:%S'),"错误类型":str(e)}
                self.robot.sendMessage(data, self.robot.transMessage_dataCraw )
                time.sleep(60)
                continue

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
                LiangBi = HpParam["LiangBi"]
                net_mf_amount = HpParam["net_mf_amount"]

                # 输出
                info = copy.deepcopy(HpParam)
                info["df_k_5m_volume_mean"] = df_k_5m_volume_mean
                info["price"] = price
                info["price_diff"] = self.fund_code_dict[fund_code]
                info["volume"] = volume
                log.info(json.dumps(info,ensure_ascii=False))

                # 计算均值 且 成交量大于过去的均值
                if          min_price > 0 \
                        and price > min_price \
                        and self.fund_code_dict[fund_code] != True \
                        and volume/df_k_5m_volume_mean > LiangBi \
                        and min_price > preClose \
                        and net_mf_amount >= 0:
                    
                    ShouYi_price = min_price*(1+ShouYi)
                    ZhiShun_price = min_price*(1+ZhiShun)
                    post_data = {
                        "fund_code" : fund_code,
                        "condition" : ">"+str(min_price),
                        "price" : str(price),
                        "name": name,
                        "收益线" : str(ShouYi_price),
                        "止损线" : str(ZhiShun_price),
                        "量比" : str(volume/df_k_5m_volume_mean),
                        "平均持有时间": str(mean_keep_day),
                        "回测准确率": str(precesion),
                        "报警时间": self.getCurrentDate().strftime('%Y-%m-%d %H:%M:%S')
                    }
                    self.robot.sendMessage(post_data,self.robot.transMessage_MeanLineAndVolume)
                    self.fund_code_dict[fund_code] = True
                elif self.fund_code_dict[fund_code] != True:
                    self.fund_code_dict[fund_code] = price - min_price

    def after_strategy(self):
        pass

class Strategy_TaoLi(StrategyBase):
    def __init__(self):
        self.runStrategyInterval = 300
        super().__init__(self.runStrategyInterval)
        self.conf_path = "./conf/wornning.json"

    def read_conf(self,conf_path):
        with open(conf_path,"r") as f:
            conf = [json.loads(i.strip()) for i in f]
            conf = [i for i in conf if "type" in i]
            conf = [i for i in conf if i["type"]=="套利检测"]
        self.conf = {i["id"]:i for i in conf}

    def strategy(self):
        self.read_conf(self.conf_path)
        currentday = self.getCurrentDate()
        yesterday = self.getyesterday(currentday).strftime("%Y%m%d")
        QMT_kwargs = {}
        QMT_kwargs["field_list"] = ["time","open","close","low","high","volume","preClose"]
        QMT_kwargs["incrementally"] = False
        for k,v in self.conf.items():
            fund_code = v["fund_code"]

            # 收盘价
            QMT_kwargs["stock_code"] = fund_code
            QMT_kwargs["start_time"] = yesterday
            QMT_kwargs["end_time"] = yesterday
            QMT_kwargs["period"] = "1d"
            fund_preClose = self._request_post(**QMT_kwargs).to_dict(orient="records")[0]["close"]

            # 指数成分和权重
            index2weight = self._request_post_index_weight(**{"fund_code":fund_code,"reload":True})["指数权重"]
            index2weight = {i["con_code"]:i["weight"]/100 for i in index2weight}
            indexs = [i for i in index2weight.keys()]

            # 获取开盘价
            try:
                df = ts.realtime_quote(ts_code=",".join(indexs), src='sina')
                df = df.to_dict(orient="records")
            except Exception as e:
                data = {"date":self.getCurrentDate().strftime('%Y-%m-%d %H:%M:%S'),"错误类型":str(e)}
                self.robot.sendMessage(data, self.robot.transMessage_dataCraw )
                time.sleep(60)
                continue

            # 测试效果
            for i in df:
                print(i)




    def Timestamp_fix(self,morningTimestamp,afternoonTimestamp):
        morningTimestamp[1] = morningTimestamp[0]
        morningTimestamp[0] = morningTimestamp[0] - 290 # 9点25分10秒开始。
        afternoonTimestamp[0] = morningTimestamp[1] + 1
        afternoonTimestamp[1] = morningTimestamp[1] + 2

        return morningTimestamp,afternoonTimestamp

class Strategy_price_linear_fit(StrategyBase):
    def __init__(self):
        self.runStrategyInterval = 60 # 价格检索间隔 6个小时
        super().__init__(self.runStrategyInterval)
        self.pro = ts.pro_api('3085222731857622989')
        self.pro._DataApi__http_url = "http://47.109.97.125:8080/tushare"
        self.fund_list_path = "./conf/fund_info_with_index_weight_only.json"
        self.last_daies = 20
        self.writer = fund_amount_and_price()
        self.hit_fund_code = []


    def read_fund_list(self):
        with open(self.fund_list_path,"r") as f:
            fund_list = [json.loads(i.strip()) for i in f]
        self.fund_list = [i for i in fund_list if "index_weight" in i ]
        # 排除掉包含非a股的指数
        tmp = []
        for i in self.fund_list:
            mark = True
            for j in i["index_weight"]:
                if not (j["con_code"].endswith("SH") or j["con_code"].endswith("SZ") or j["con_code"].endswith("BJ")):
                    mark = False
            if mark:
                tmp.append(i)
        self.fund_list = tmp

    def get_last_daies(self):
        current_date = self.getCurrentDate()
        trade_calendar_dict, year = self.getTradeCalender()
        last_daies = []
        while len(last_daies) < self.last_daies:
            current_date_str = current_date.strftime('%Y-%m-%d')
            if trade_calendar_dict[current_date_str]["trade_status"] == 1:
                last_daies.append(current_date.strftime('%Y%m%d'))
            current_date = self.getyesterday(current_date)
        return last_daies

    def divide_high_and_low(self,price):
        price.sort(key=lambda x: int(x["trade_date"]))
        high = []
        high_index = []
        low = []
        low_index = []
        for i in range(1,len(price)-1):
            if price[i]["close"] > price[i-1]["close"] and price[i]["close"] > price[i+1]["close"]:
                high.append(price[i]["close"])
                high_index.append(i)
            elif price[i]["close"] < price[i-1]["close"] and price[i]["close"] < price[i+1]["close"]:
                low.append(price[i]["close"])
                low_index.append(i)
        if price[-1]["close"] > price[-2]["close"]:
            high.append(price[-1]["close"])
            high_index.append(20-1)
        elif price[-1]["close"] < price[-2]["close"]:
            low.append(price[-1]["close"])
            low_index.append(20-1)

        return high,high_index,low,low_index

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

        return  a, b


    def after_strategy(self):
        time.sleep(3*60*60)
        self.read_fund_list()
        last_daies = self.get_last_daies()

        # 
        hit_fund_code = []
        hit_fund_code_line = {}
        for fund in tqdm(self.fund_list):
            price = self.pro.fund_daily(ts_code=fund["ts_code"],start_date=last_daies[-1],end_date=last_daies[0]).to_dict(orient="records")
            time.sleep(0.12)
            high,high_index,low,low_index = self.divide_high_and_low(price=price)
            high_a, high_b = self.linear_fit(high_index,high)
            low_a, low_b = self.linear_fit(low_index,low)
            if high_a > 0 and low_a > 0:
                hit_fund_code.append(fund["ts_code"])
                hit_fund_code_line[fund["ts_code"]] = high_a + low_a
        hit_fund_code = sorted([(k,v) for k,v in hit_fund_code_line.items()],key=lambda x: x[1],reverse=True)
        hit_fund_code = [i[0] for i in hit_fund_code]

        # 
        # tmp = "159106.SZ,563580.SH,159368.SZ,515790.SH,159870.SZ,159595.SZ,159141.SZ,159350.SZ,159203.SZ,159915.SZ,159209.SZ,159901.SZ,562500.SH,563360.SH,159218.SZ,563760.SH,510300.SH,159819.SZ,512800.SH,512880.SH,159572.SZ,159275.SZ,563320.SH,159287.SZ,159949.SZ,159565.SZ,159309.SZ,159599.SZ,560800.SH,159325.SZ,159326.SZ,159516.SZ,159328.SZ,159335.SZ,159336.SZ,510500.SH,159385.SZ,159391.SZ,159628.SZ,159695.SZ,159510.SZ,159511.SZ,159512.SZ,515800.SH,159520.SZ,159523.SZ,512890.SH,516510.SH,159528.SZ,563300.SH,159546.SZ,561980.SH,159583.SZ,159698.SZ,159588.SZ,159597.SZ,159781.SZ,159606.SZ,562800.SH,159616.SZ,159617.SZ,516950.SH,159620.SZ,562000.SH,159845.SZ,515030.SH,159638.SZ,562990.SH,516650.SH,159717.SZ,562310.SH,159662.SZ,159667.SZ,159666.SZ,515650.SH,159928.SZ,561330.SH,159692.SZ,516260.SH,159761.SZ,159905.SZ,159709.SZ,516150.SH,516590.SH,159725.SZ,159728.SZ,516570.SH,159736.SZ,159743.SZ,516270.SH,159758.SZ,159766.SZ,159773.SZ,159778.SZ,561600.SH,159786.SZ,159790.SZ,560180.SH,515250.SH,159796.SZ,159804.SZ,512980.SH,159807.SZ,159811.SZ,159814.SZ,159825.SZ,512170.SH,159836.SZ,159843.SZ,159861.SZ,515170.SH,159865.SZ,159869.SZ,159871.SZ,159872.SZ,516160.SH,159880.SZ,159886.SZ,159887.SZ,159902.SZ,159903.SZ,159906.SZ,159909.SZ,159910.SZ,159912.SZ,159913.SZ,159918.SZ,159930.SZ,512640.SH,159933.SZ,562580.SH,159939.SZ,159940.SZ,159944.SZ,159945.SZ,159965.SZ,159966.SZ,159967.SZ,159973.SZ,515900.SH,159976.SZ,159993.SZ,515050.SH,159996.SZ,159997.SZ,510010.SH,510020.SH,510030.SH,510180.SH,510090.SH,510130.SH,510150.SH,510160.SH,510170.SH,510200.SH,510210.SH,510230.SH,510270.SH,530380.SH,510410.SH,510630.SH,510650.SH,510720.SH,510770.SH,510810.SH,510880.SH,512040.SH,512070.SH,512190.SH,512220.SH,512260.SH,512330.SH,512400.SH,512480.SH,512530.SH,512660.SH,512580.SH,512650.SH,512670.SH,512710.SH,512750.SH,512770.SH,512870.SH,512970.SH,515000.SH,515090.SH,515110.SH,515200.SH,515260.SH,515580.SH,515590.SH,515630.SH,515700.SH,515730.SH,515750.SH,515760.SH,515860.SH,515880.SH,515920.SH,515980.SH,516110.SH,516190.SH,562910.SH,516910.SH,516550.SH,516720.SH,516800.SH,516970.SH,516980.SH,560030.SH,560120.SH,560170.SH,563010.SH,562320.SH,561500.SH,560660.SH,560810.SH,560860.SH,560980.SH,561130.SH,562850.SH,561320.SH,561360.SH,562010.SH,562080.SH,562330.SH,562900.SH,563060.SH,563330.SH,563380.SH,563700.SH,588000.SH,588010.SH,588020.SH,588100.SH,588230.SH,588170.SH,588200.SH,589060.SH,589680.SH,588790.SH,588780.SH,588830.SH,588850.SH,588910.SH".split(",")
        # hit_fund_code = [i["ts_code"] for i in self.fund_list if i["ts_code"] not in tmp]
        # print(hit_fund_code)


        # 写入
        if hit_fund_code:
            self.writer.write(hit_fund_code, end_date=last_daies[0])
            # self.writer.write(hit_fund_code,end_date="20251210")  

        info = {}
        tmp = [i for i in hit_fund_code if i not in self.hit_fund_code]
        self.hit_fund_code = self.hit_fund_code + tmp
        info["今日命中数量"] = len(hit_fund_code)
        info["今日命中详情"] = ",".join(hit_fund_code)
        info["新增数量"] = len(tmp)
        info["新增详情"] = ",".join(tmp)
        info["date"] = last_daies[0]
        self.robot.sendMessage(info, self.robot.transMessage_price_line_fit)




    

if __name__ == "__main__":
    pass
    # strategy = Strategy_MeanLineAndVolume()
    # strategy.run()
    # strategy.before_strategy()

    # strategy.strategy()
    # strategy.after_strategy()

    # strategy = Strategy_TaoLi()
    # strategy.run()

    strategy = Strategy_price_linear_fit()
    strategy.after_strategy()

            


        

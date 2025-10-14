import json
from DatabaseOperation import DatabaseOperation
import logging
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler
import numpy as np
import time
from datetime import datetime
import adata

logging.basicConfig(format='%(asctime)s %(message)s')
log = logging.getLogger(name="log")
log.setLevel(level=logging.INFO)


class StaticNorm():
    def __init__(self) -> None:
        self.databaseOperation = DatabaseOperation()
        self.computedDay = self.setMaxTimestamp()

    def setMaxTimestamp(self):
        maxTimestamp_sql = self.databaseOperation.searchSql_maxTimestamp("goldPriceNorm")
        maxTimestamp = self.databaseOperation.executeSearchSql(maxTimestamp_sql)[0]
        if maxTimestamp["timestamp"]:
            computedDay = maxTimestamp["timestamp"]
            computedDay = datetime.fromtimestamp(computedDay)
            computedDay = computedDay.strftime('%Y-%m-%d %H:%M:%S')
        else:
            computedDay = None
        return computedDay

    def getCurrentTimeStr():
        now = datetime.now()
        now = now.strftime('%Y-%m-%d %H:%M:%S')
        return now

    def run(self):

        while True:
            now_str = self.getCurrentTimeStr()
            # 没有到新的一天
            if now_str.split(" ")[0] == self.computedDay.split(" ")[0]:
                time.sleep(45*60)
                continue
            # 到达新的一天
            markTime = now_str.split(" ")
            markTime[1] = "02:30:00"
            markTimeStr = " ".join(markTime)
            markTimestamp = datetime.strptime(markTimeStr, '%Y-%m-%d %H:%M:%S').timestamp()
            # 还没有到达指定时间
            if markTimestamp > datetime.strptime(now_str, '%Y-%m-%d %H:%M:%S').timestamp():
                time.sleep(45*60)
                continue
            # 超过指定时间开始计算
            # 17.5小时
            
class StaticGoldETFPriceNorm():
    def __init__(self):
        self.databaseOperation = DatabaseOperation()
        self.tradeCalender,self.tradeYear = self.getTradeCalender()

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
    
    def getNextDay(self,date):
        tmp_timestamp = date.timestamp() + 24*60*60
        nextDay = datetime.fromtimestamp(tmp_timestamp)
        nextDay = nextDay.strftime("%Y-%m-%d")
        nextDay = nextDay + " 00:00:01"
        nextDay = datetime.strptime(nextDay,'%Y-%m-%d %H:%M:%S')
        return nextDay
    
    def getBeforeDay(self,date):
        tmp_timestamp = date.timestamp() - (24*60*60+1)
        beforeDay = datetime.fromtimestamp(tmp_timestamp)
        beforeDay = beforeDay.strftime("%Y-%m-%d")
        beforeDay = beforeDay + " 00:00:01"
        beforeDay = datetime.strptime(beforeDay,'%Y-%m-%d %H:%M:%S')
        return beforeDay

    def getDay(self,date):
        date_day = date.strftime("%Y-%m-%d")
        date = date_day + " 00:00:00"
        date = datetime.strptime(date,'%Y-%m-%d %H:%M:%S')
        return date

    def get4OclockTimestamp(self,date):
        date = date.strftime("%Y-%m-%d")
        date = date + " 16:00:00"
        date = datetime.strptime(date,'%Y-%m-%d %H:%M:%S')
        date = date.timestamp()
        return date

    def getTradeTimestampInterval(self,date):
        # 计算开市的时间戳
        date_str = date.strftime("%Y-%m-%d %h:%M:%S").split(" ")[0]
        morningDate = [" ".join([date_str,"09:30:00"])," ".join([date_str,"11:30:00"])]
        afternoonDate = [" ".join([date_str,"13:00:00"])," ".join([date_str,"15:00:00"])]
        morningTimestamp = [datetime.strptime(i, '%Y-%m-%d %H:%M:%S').timestamp() for i in morningDate]
        afternoonTimestamp = [datetime.strptime(i, '%Y-%m-%d %H:%M:%S').timestamp() for i in afternoonDate]
        return morningTimestamp,afternoonTimestamp
    
    def getMeanAndStd(self,prices):
        mean = sum(prices) / len(prices)
        variance = sum((x - mean)**2 for x in prices) / len(prices)
        std_dev = variance**0.5
        return mean,std_dev

    def run(self):
        
        while True:
            currentDate = self.getCurrentDate()
            # 判断判断日历年号和当前年号是否一致,如果不一致，那么就将更新日历
            if currentDate.strftime("%Y") != self.tradeYear:
                self.tradeCalender,self.tradeYear = self.getTradeCalender()
            
            # 判断是否是开市日
            if self.tradeCalender[currentDate.strftime("%Y-%m-%d")]["trade_status"] == 0:
                # 没有开市,休眠到第二天的
                nextDay = self.getNextDay(currentDate)
                nextDayTimestamp = nextDay.timestamp()
                currentTimestamp = currentDate.timestamp()
                log.info(":{}不是开市日，休眠到下一个开市日".format(currentDate.strftime("%Y-%m-%d")))
                time.sleep(nextDayTimestamp - currentTimestamp)
                continue

            # 判断是否是下午四点之后
            currentDate = self.getCurrentDate()
            currentDateTimestamp = currentDate.timestamp()
            currentDay4OclockTimestamp = self.get4OclockTimestamp(currentDate)
            if currentDateTimestamp < currentDay4OclockTimestamp:
                log.info("休眠到下午四点整")
                time.sleep(currentDay4OclockTimestamp - currentDateTimestamp)

            # 计算均值均值和标准差
            currentDate = self.getCurrentDate()
            currentDayTimestamp = self.getDay(currentDate).timestamp()
            result = []
            for id in ["518880","159934","159937","518800","518660"]:
                oneline = {}
                oneline["fund_code"] = id
                oneline["timestamp"] = currentDayTimestamp
                # 当天的数据
                morningTimestamp,afternoonTimestamp = self.getTradeTimestampInterval(currentDate)
                sql = self.databaseOperation.searchSql_ETFprice(morningTimestamp[1], morningTimestamp[1]-morningTimestamp[0], id)
                prices1 = self.databaseOperation.executeSearchSql(sql)
                sql = self.databaseOperation.searchSql_ETFprice(afternoonTimestamp[1], afternoonTimestamp[1]-afternoonTimestamp[0], id)
                prices2 = self.databaseOperation.executeSearchSql(sql)
                price = prices1 + prices2
                # 计算当天的均值和标准差
                price_tmp = [i["price"] for i in price]
                mean,std_dev = self.getMeanAndStd(price_tmp)
                oneline["std_1day"] = std_dev
                oneline["average_1day"] = mean
                # 读取前一天的数据
                _2daysAgo = self.getBeforeDay(currentDate)
                while self.tradeCalender[_2daysAgo.strftime("%Y-%m-%d")]["trade_status"] == 0:
                    _2daysAgo = self.getBeforeDay(_2daysAgo)
                morningTimestamp,afternoonTimestamp = self.getTradeTimestampInterval(_2daysAgo)
                sql = self.databaseOperation.searchSql_ETFprice(morningTimestamp[1], morningTimestamp[1]-morningTimestamp[0], id)
                prices1 = self.databaseOperation.executeSearchSql(sql)
                sql = self.databaseOperation.searchSql_ETFprice(afternoonTimestamp[1], afternoonTimestamp[1]-afternoonTimestamp[0], id)
                prices2 = self.databaseOperation.executeSearchSql(sql)
                price = price + prices1 + prices2
                # 计算当天+前一天数据的均值和标准差
                price_tmp = [i["price"] for i in price]
                mean,std_dev = self.getMeanAndStd(price_tmp)
                oneline["std_2day"] = std_dev
                oneline["average_2day"] = mean
                # 读取前两天的数据
                _3daysAgo = self.getBeforeDay(_2daysAgo)
                while self.tradeCalender[_3daysAgo.strftime("%Y-%m-%d")]["trade_status"] == 0:
                    _3daysAgo = self.getBeforeDay(_3daysAgo)
                morningTimestamp,afternoonTimestamp = self.getTradeTimestampInterval(_3daysAgo)
                sql = self.databaseOperation.searchSql_ETFprice(morningTimestamp[1], morningTimestamp[1]-morningTimestamp[0], id)
                prices1 = self.databaseOperation.executeSearchSql(sql)
                sql = self.databaseOperation.searchSql_ETFprice(afternoonTimestamp[1], afternoonTimestamp[1]-afternoonTimestamp[0], id)
                prices2 = self.databaseOperation.executeSearchSql(sql)
                price = price + prices1 + prices2
                # 计算当天+前一天+前两天数据的均值和标准差
                price_tmp = [i["price"] for i in price]
                mean,std_dev = self.getMeanAndStd(price_tmp)
                oneline["std_3day"] = std_dev
                oneline["average_3day"] = mean
                result.append(oneline)

            # 写入到数据库
            sql = self.databaseOperation.insertSql_norm("GoldETFPriceNorm",result)
            self.databaseOperation.executeInsertSql(sql)

            # 休眠到下一天
            currentDate = self.getCurrentDate()
            nextDay = self.getNextDay(currentDate)
            nextDayTimestamp = nextDay.timestamp()
            currentTimestamp = currentDate.timestamp()
            log.info("休眠到下一天")
            time.sleep(nextDayTimestamp - currentTimestamp) 
            

                






            

            




    

        
if __name__ == "__main__":
    static = staticNorm()




# import math
# from scipy.stats import norm

# def calculate_normal_cdf(x, mean=0, std_dev=1):
#     """
#     计算正态分布的累积分布函数值
#     参数:
#         x: 要计算的值
#         mean: 均值(默认0)
#         std_dev: 标准差(默认1)
#     返回:
#         累积分布概率值
#     """
#     # 使用scipy的norm.cdf函数计算
#     return norm.cdf(x, loc=mean, scale=std_dev)

# # 示例用法
# if __name__ == "__main__":
#     mean = 10    # 均值
#     std_dev = 2  # 标准差
#     x = 12       # 要计算的值
    
#     cdf_value = calculate_normal_cdf(x, mean, std_dev)
#     print(f"P(X ≤ {x}) = {cdf_value:.4f}")

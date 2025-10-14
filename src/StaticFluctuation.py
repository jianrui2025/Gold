import json
from DatabaseOperation import DatabaseOperation
import logging
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler
import numpy as np
import time
from abc import abstractclassmethod
from datetime import datetime
import adata


logging.basicConfig(format='%(asctime)s %(message)s')
log = logging.getLogger(name="log")
log.setLevel(level=logging.INFO)


class StaticGoldETFPriceBase():
    def __init__(self):
        self.tradeCalender,self.tradeYear = self.getTradeCalender()
        
    @abstractclassmethod
    def getMaxTimestamp(self,fund_code):
        pass

    @abstractclassmethod
    def getMaxOneTimestamp(self,fund_code,fund_code_maxTimestamp):
        pass

    @abstractclassmethod
    def getResultAndSave(self,fund_code,timestamp):
        pass

    def getCurrentDate(slef):
        # 读取当前时间，返回年月日时分秒
        return datetime.now()

    def getNextDay(self,date):
        tmp_timestamp = date.timestamp() + 24*60*60
        nextDay = datetime.fromtimestamp(tmp_timestamp)
        nextDay = nextDay.strftime("%Y-%m-%d")
        nextDay = nextDay + " 00:00:01"
        nextDay = datetime.strptime(nextDay,'%Y-%m-%d %H:%M:%S')
        return nextDay
    
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

    def run(self):
        while True:
            currentDate = self.getCurrentDate()
            # 判断是否是开市日,如果不是开市日，则休眠到下一天。
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

            while True:

                currentDate = self.getCurrentDate()
                currentDateTimestamp = currentDate.timestamp()

                # 还未到达开市时间
                if currentDateTimestamp < morningTimestamp[0]:
                    deltaTimestamp = morningTimestamp[0] - currentDateTimestamp
                    log.info("休眠到开早市的时间")
                    time.sleep(deltaTimestamp)

                # 开市时间
                elif morningTimestamp[0] < currentDateTimestamp < morningTimestamp[1] or afternoonTimestamp[0] < currentDateTimestamp < afternoonTimestamp[1]:
                    start = datetime.now()
                    # 获取最大时间戳
                    self.fund_code_maxTimestamp = {}
                    for id in self.fund_code_list:
                        maxTimestamp = self.getMaxTimestamp(id)
                        self.fund_code_maxTimestamp[id] = maxTimestamp

                    # 获取尚未被统计的时间戳
                    self.fund_code_price = {}
                    for id in self.fund_code_list:
                        timestamps = self.getMaxOneTimestamp(id, self.fund_code_maxTimestamp)
                        self.fund_code_price[id] = timestamps

                    # 将新统计的数据插入到新数据库
                    for id in self.fund_code_list:
                        if self.fund_code_price[id]:
                            for oneTimestamp in self.fund_code_price[id]:
                                self.getResultAndSave(id,oneTimestamp["timestamp"])
                                log.info("插入:"+id+":"+str(oneTimestamp["timestamp"]))
                        else:
                            log.info(id+"无插入！！")
                    end = datetime.now()
                    timeDelta = end - start
                    sleepTime = 15 - timeDelta.seconds if 15 - timeDelta.seconds > 0 else 0
                    time.sleep(sleepTime)
                # 中午休市时间
                elif morningTimestamp[1]  < currentDateTimestamp < afternoonTimestamp[0]:
                    log.info("中午休市的时间")
                    sleep_time = afternoonTimestamp[0] - self.getCurrentDate().timestamp()
                    time.sleep(sleep_time)
                # 今日完成开市
                elif currentDateTimestamp > afternoonTimestamp[1]:
                    # 晚上休市
                    # 睡眠到下一天
                    currentDate = self.getCurrentDate()
                    nextDay = self.getNextDay(currentDate)
                    nextDayTimestamp = nextDay.timestamp()
                    currentTimestamp = currentDate.timestamp()
                    log.info(":{}任务结束，休眠到下一日".format(currentDate.strftime("%Y-%m-%d")))
                    time.sleep(nextDayTimestamp - currentTimestamp)
                    break
                else:
                    time.sleep(1)
    



class StaticGoldETFPriceFluctuation(StaticGoldETFPriceBase):
    def __init__(self):
        super().__init__()
        self.databaseOperation = DatabaseOperation()
        self.fund_code_list = ["518880"]
        self.sampleInvertal = 15 # 采样间隔, 单位秒
        self.sample_minuem = [1,2,4,8,16,32]
        self.sample_seconds = [15]


    def getMaxTimestamp(self,fund_code):
        sql = self.databaseOperation.searchSql_maxTimestamp("GoldETFPriceFluctuation", "fund_code={}".format(fund_code))
        maxTimestamp = self.databaseOperation.executeSearchSql(sql)
        maxTimestamp = str(maxTimestamp[0]["timestamp"]) if maxTimestamp[0]["timestamp"] else str(0)
        return maxTimestamp
    
    def getMaxOneTimestamp(self,fund_code,fund_code_maxTimestamp):
        sql = self.databaseOperation.searchSql_maxOneTimestamp("GoldETFPrice", fund_code_maxTimestamp[fund_code], "fund_code={}".format(fund_code))
        timestamps = self.databaseOperation.executeSearchSql(sql)
        return timestamps
    
    def getResultAndSave(self, fund_code, timestamp):
        '''
        计算均值和方差，还是斜率
        '''
        oneline = {}
        oneline["fund_code"] = fund_code
        oneline["timestamp"] = str(timestamp)

        # 读取数据
        max_minum = max(self.sample_minuem)
        sql = self.databaseOperation.searchSql_ETFprice(timestamp,max_minum*60,fund_code)
        row = self.databaseOperation.executeSearchSql(sql)

        for time_long in self.sample_minuem:
            tmp_row = [i for i in row if timestamp - i["timestamp"] <= time_long*60]

            # 计算波动，均值，方差
            slope, mean, variance = self.getFluctuation(tmp_row,timestamp)
            if slope != None:
                oneline["fluctuation_{}min".format(str(time_long))] = str(slope)
            else:
                oneline["average_{}min".format(str(time_long))] = "NULL"
            if mean != None:
                oneline["average_{}min".format(str(time_long))] = str(mean)
            else:
                oneline["fluctuation_{}min".format(str(time_long))] = "NULL"
            if variance != None:
                oneline["variance_{}min".format(str(time_long))] = str(variance)
            else:
                oneline["variance_{}min".format(str(time_long))] = "NULL"

            # 计算内盘交易量和外盘交易量
            insidePanDelta,outsidePanDelta = self.getInsideOutsidePanNum(tmp_row)
            if insidePanDelta != None:
                oneline["insidePan_{}min".format(str(time_long))] = str(insidePanDelta)
            else:
                oneline["insidePan_{}min".format(str(time_long))] = "NULL"
            if outsidePanDelta != None:
                oneline["outsidePan_{}min".format(str(time_long))] = str(outsidePanDelta)
            else:
                oneline["outsidePan_{}min".format(str(time_long))] = "NULL"
        # 计算15s的内盘交易量和外盘交易量
        for time_long in self.sample_seconds:
            insidePanDelta,outsidePanDelta = self.getInsideOutsidePanNum_Second(row,self.sampleInvertal)
            if insidePanDelta != None:
                oneline["insidePan_{}sec".format(str(time_long))] = str(insidePanDelta)
            else:
                oneline["insidePan_{}sec".format(str(time_long))] = "NULL"
            if outsidePanDelta != None:
                oneline["outsidePan_{}sec".format(str(time_long))] = str(outsidePanDelta)
            else:
                oneline["outsidePan_{}sec".format(str(time_long))] = "NULL"

        sql = self.databaseOperation.insertSql_norm("GoldETFPriceFluctuation",[oneline])
        log.info(sql)
        self.databaseOperation.executeInsertSql(sql)
        
    def getFluctuation(self,row,timestamp):        
        if len(row) <2:
            return None,None,None
        y = [i["price"] for i in row]
        x = [(i["timestamp"]-timestamp)/self.sampleInvertal  for i in row]
        length = max(x) - min(x)
        x = np.array(x).reshape(-1, 1)
        y = np.array(y)
        model = LinearRegression()
        model.fit(x,y)
        slope = model.coef_[0] * length # 波动
        mean = sum(y)/len(y)
        variance = sum((i-mean)**2 for i in y)/len(y)
        return slope, mean, variance
    
    def getInsideOutsidePanNum(self,row):
        # 一定时间内内盘和外盘的交易数量
        if len(row) <= 1:
            return None,None
        row.sort(key=lambda x: x["timestamp"])
        insidePanDelta = row[-1]["inside_pan"] - row[0]["inside_pan"]
        outsidePanDelta = row[-1]["outside_pan"] - row[0]["outside_pan"]
        return insidePanDelta,outsidePanDelta
    
    def getInsideOutsidePanNum_Second(self,row,timestamp_long):
        if len(row) <= 1:
            return None,None
        row.sort(key=lambda x: x["timestamp"])
        insidePanDelta = (row[-1]["inside_pan"] - row[-2]["inside_pan"])/(( row[-1]["timestamp"] - row[-2]["timestamp"])/timestamp_long)
        outsidePanDelta = (row[-1]["outside_pan"] - row[-2]["outside_pan"])/(( row[-1]["timestamp"] - row[-2]["timestamp"])/timestamp_long)
        return int(insidePanDelta),int(outsidePanDelta)







if __name__ == "__main__":
    static = StaticGoldETFPriceFluctuation()
    static.run()


import json
import pandas as pd
from datetime import datetime
from DatabaseOperation import DatabaseOperation
from torch.utils.tensorboard import SummaryWriter
import time
import os
import shutil
import logging
import adata

logging.basicConfig(format='%(asctime)s %(message)s')
log = logging.getLogger(name="log")
log.setLevel(level=logging.INFO)


class writeToTensorboardBase():
    def __init__(self, sampleInvertal,fund_code_list):
        self.databaseOperation = DatabaseOperation()
        self.log_dir = "/home/jianrui/Gold/TensorboardLog"
        self.writer = SummaryWriter(log_dir=self.log_dir)
        self.sampleInvertal = sampleInvertal
        self.fund_code_list = fund_code_list
        self.tradeCalender,self.tradeYear = self.getTradeCalender()

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
    
    def getCurrentDay(self,timestamp):
        currentDatetime = datetime.fromtimestamp(timestamp)
        currentDay = currentDatetime.strftime("%Y-%m-%d")
        return currentDay
    
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

    def emptyTensorboard(self):
        for filename in os.listdir(self.log_dir):
            file_path = os.path.join(self.log_dir, filename)
            # print(file_path)
            if os.path.isfile(file_path):
                # print(file_path)
                os.remove(file_path)
            else:
                shutil.rmtree(file_path)

    def getMaxTimestamp(self,fund_code):
        pass

    def getMaxOneTimestamp(self,fund_code,fund_code_maxTimestamp):
        pass

    def toTensorboard(self,id,timestamp):   
        pass


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

            # self.emptyTensorboard()
            # log.info("清空tensorboard数据!")
            self.fund_code_index = {}
            log.info("初始化index!")
            self.fund_code_maxTimestamp = {}
            for id in self.fund_code_list:
                self.fund_code_maxTimestamp[id] = morningTimestamp[0]
                self.fund_code_index[id] = 1
            log.info("初始化最大时间戳!")

            while True:

                currentDate = self.getCurrentDate()
                currentDateTimestamp = currentDate.timestamp()

                # 还未到达开市时间
                if currentDateTimestamp < morningTimestamp[0]:
                    deltaTimestamp = morningTimestamp[0] - self.getCurrentDate().timestamp()
                    log.info("休眠到开早市的时间")
                    time.sleep(deltaTimestamp)

                # 开市时间
                if morningTimestamp[0] < currentDateTimestamp < morningTimestamp[1] or afternoonTimestamp[0] < currentDateTimestamp < afternoonTimestamp[1]:
                    start = datetime.now()
                    # 获取尚未被统计的时间戳
                    self.fund_code_price = {}
                    for id in self.fund_code_list:
                        timestamps = self.getMaxOneTimestamp(id, self.fund_code_maxTimestamp)
                        self.fund_code_price[id] = timestamps

                    # 将新统计的数据插入到新数据库
                    for id in self.fund_code_list:
                        if self.fund_code_price[id]:
                            self.fund_code_price[id].sort(key=lambda x: x["timestamp"])
                            for oneTimestamp in self.fund_code_price[id]:
                                self.toTensorboard(id,oneTimestamp["timestamp"],self.fund_code_index[id])
                                log.info("插入:"+id+":"+str(oneTimestamp["timestamp"]))
                                #更新参数
                                self.fund_code_index[id] += 1
                                self.fund_code_maxTimestamp[id] = oneTimestamp["timestamp"] if oneTimestamp["timestamp"] > self.fund_code_maxTimestamp[id] else self.fund_code_maxTimestamp[id]
                        else:
                            log.info(id+"无插入！！")
                    end = datetime.now()
                    timeDelta = end - start
                    sleepTime = self.sampleInvertal - timeDelta.seconds if self.sampleInvertal - timeDelta.seconds > 0 else 0
                    time.sleep(sleepTime)
                # 中午休市时间
                elif morningTimestamp[1]  < currentDateTimestamp < afternoonTimestamp[0]:
                    log.info("中午休市的时间")
                    sleep_time = afternoonTimestamp[0] - currentDateTimestamp
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
 
class writeToTensorboard(writeToTensorboardBase):
    def __init__(self):
        self.sampleInvertal = 15 # 采样间隔
        self.fund_code_list = ["518880"]
        super().__init__(self.sampleInvertal,self.fund_code_list)

    def getMaxTimestamp(self,fund_code):
        sql = self.databaseOperation.searchSql_maxTimestamp("GoldETFPriceFluctuation", "fund_code={}".format(fund_code))
        maxTimestamp = self.databaseOperation.executeSearchSql(sql)
        maxTimestamp = str(maxTimestamp[0]["timestamp"]) if maxTimestamp[0]["timestamp"] else str(0)
        return maxTimestamp
    
    def getMaxOneTimestamp(self,fund_code,fund_code_maxTimestamp):
        sql = self.databaseOperation.searchSql_maxOneTimestamp("GoldETFPriceFluctuation", fund_code_maxTimestamp[fund_code], "fund_code={}".format(fund_code))
        timestamps = self.databaseOperation.executeSearchSql(sql)
        return timestamps
    
    def ETFPriceToTensorboard(self,id,timestamp,index):
        sql = self.databaseOperation.searchSql_byTimestamp("GoldETFPrice",timestamp,"fund_code={fund_code}".format(fund_code=id))
        row = self.databaseOperation.executeSearchSql(sql)[0]
        currentDay = self.getCurrentDay(timestamp)
        self.writer.add_scalar(tag=currentDay+"/"+id+"/price",scalar_value=row["price"],global_step=index,walltime=timestamp)

    def InsideOutsidePanToTensorboard(self,id,timestamp,index):
        sql = self.databaseOperation.searchSql_byTimestamp("GoldETFPriceFluctuation",timestamp,"fund_code={fund_code}".format(fund_code=id))
        row = self.databaseOperation.executeSearchSql(sql)[0]

        insidePan_15sec = row["insidePan_15sec"]
        outsidePan_15sec = row["outsidePan_15sec"]
        try :
            Difference_15sec = row["outsidePan_15sec"] - row["insidePan_15sec"]
        except:
            Difference_15sec = row["outsidePan_15sec"]
        
        currentDay = self.getCurrentDay(timestamp)
        if isinstance(row["insidePan_15sec"],int) and isinstance(row["outsidePan_15sec"],int):
            tag_scalar_dict = {"insidePan_15sec":insidePan_15sec,"outsidePan_15sec":outsidePan_15sec,"Difference_15sec":Difference_15sec}
            self.writer.add_scalars(main_tag=currentDay+"/"+id+"/PanKou",
                                    tag_scalar_dict=tag_scalar_dict,
                                    global_step=index,
                                    walltime=timestamp)

    def toTensorboard(self,id,timestamp,index):
        # 价格
        self.ETFPriceToTensorboard(id,timestamp,index)
        # 内盘，外盘交易量
        self.InsideOutsidePanToTensorboard(id,timestamp,index)
        log.info("写出:timestamp={timestamp},index={index}".format(timestamp=str(timestamp),index=str(index)))
        self.writer.flush()
        



if __name__ == "__main__":
    writer = writeToTensorboard()
    writer.run()
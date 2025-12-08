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
import tushare as ts
from datetime import datetime
import statistics
from tensorboard.backend.event_processing import event_accumulator
from tqdm import tqdm

logging.basicConfig(format='%(asctime)s %(message)s')
log = logging.getLogger(name="log")
log.setLevel(level=logging.INFO)


class writeToTensorboardBase():
    def __init__(self, sampleInvertal,fund_code_list):
        self.databaseOperation = DatabaseOperation()
        self.log_dir = "/home/jianrui/workspace/Gold/tensorboard_log/"
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

class base_amount_and_price():
    def __init__(self) -> None:
        pass

    def get_lastdays(self, date, dates, last_days):
        # 抽取指定日期的前n天日期。
        index = [i["cal_date"] for i in dates].index(date)
        return dates[index+1-last_days:index+1]
    
class Tensorboard():
    def __init__(self,log_dir):
        self.log_dir = log_dir
        self.writer = SummaryWriter(log_dir=self.log_dir)

    def emptyTensorboard(self):
        for filename in os.listdir(self.log_dir):
            file_path = os.path.join(self.log_dir, filename)
            # print(file_path)
            if os.path.isfile(file_path):
                # print(file_path)
                os.remove(file_path)
            else:
                shutil.rmtree(file_path)

    def addScalar(self,tag,value,index,timestamp):
        self.writer.add_scalar(tag=tag, scalar_value=value, global_step=index, walltime=timestamp)

    def addScalarDict(self,tag, scalar_dict, index, timestamp):
        self.writer.add_scalars(main_tag=tag,
                                    tag_scalar_dict=scalar_dict,
                                    global_step = index,
                                    walltime=timestamp
                                    )
    
    def search_log_dir(self,log_dir,fund_code,tag_info):
        # 查询tensorboard日志，确定已经写入日志的日期和对应的index 

        files = [f for f in os.listdir(log_dir) if "tfevents" in f]
        dete2step = {}
        date2value = {}
        for file in files:
            ea = event_accumulator.EventAccumulator(log_dir+"/"+file)
            ea.Reload()
            scalars = ea.Tags()["scalars"]
            for scalar in scalars:
                if fund_code in scalar and tag_info in scalar:
                    events = ea.Scalars(scalar)
                    for e in events:
                        date2value[datetime.fromtimestamp(int(e.wall_time)).strftime('%Y%m%d')] = e.value
                        dete2step[datetime.fromtimestamp(int(e.wall_time)).strftime('%Y%m%d')] = e.step
        return dete2step,date2value
        
class fund_amount_and_price(base_amount_and_price):
    def __init__(self,):
        super().__init__()
        self.pro = ts.pro_api('3085222731857622989')
        self.pro._DataApi__http_url = "http://47.109.97.125:8080/tushare"
        self.log_dir = "./tensorboard_log_fund/"
        self.tensorboard = Tensorboard(self.log_dir)
        # self.tensorboard.emptyTensorboard()
        self.stock_daily = {}
        self.last_days = 30 # 计算前30天的均值
        self.dir_name = "间接使用权重"

    def search_CondexAndWeight(self,date,index_code):
        def get_CondexAndWeight(index_code):
            df = self.pro.index_weight(index_code=index_code)
            self.index_weight = df.to_dict(orient="records")
            for i in self.index_weight:
                i["trade_date"] = int(i["trade_date"])
            return True
        
        mark = False
        if not mark:
            mark = get_CondexAndWeight(index_code)
        
        date = int(date)
        last_date = 0
        for i in self.index_weight:
            if date < i["trade_date"]: 
                pass
            elif date - i["trade_date"] < date - last_date:
                last_date = i["trade_date"]
                
        return [i for i in self.index_weight if i["trade_date"] == last_date]

    def stock_get_daily(self,search_days):
        for day in search_days:
            if day["cal_date"] not in self.stock_daily:
                daily = self.pro.daily(trade_date=day["cal_date"]).to_dict(orient="records")
                self.stock_daily[day["cal_date"]] = daily
        return [self.stock_daily[i["cal_date"]] for i in search_days]

    def run(self,fund_code,start_date="20241004",end_date="20251204"):
        date = self.pro.trade_cal(exchange='SSE', start_date=start_date, end_date=end_date)
        date = date.to_dict(orient="records")
        dates = [i for i in date if i["is_open"]==1]
        dates.sort(key=lambda x: int(x["cal_date"]))
        fund_code = fund_code
        self.fund_code = fund_code

        # 获取etf基本信息
        fund_code_info = self.pro.etf_basic(ts_code=fund_code)
        fund_code_info = fund_code_info.to_dict(orient="records")[0]
        index_code = fund_code_info["index_code"]
        
        # 查询已经写入tensorboard的信息。
        dete2step,date2net_mf_amount_a_year = self.tensorboard.search_log_dir(self.log_dir,fund_code,"net_mf_amount_a_year")

        for date_info in tqdm(dates[self.last_days:]):
            date = date_info["cal_date"]
            pretrade_date = date_info["pretrade_date"]
            timestamp = datetime.strptime(date, '%Y%m%d').timestamp()

            if date in date2net_mf_amount_a_year:
                continue

            # 计算当前的step
            if dete2step:
                num = 1 + dete2step[pretrade_date]
                dete2step[date] = num
            else:
                num = 0
                dete2step[date] = num

            # 查询当时的成分和权重，资金流向
            index_weight = self.search_CondexAndWeight(date,index_code)
            index_weight = {i["con_code"]:i["weight"] for i in index_weight}
            stock_code_list = [i for i in index_weight.keys()]
            

            # 计算前30天价格变化的均值            
            search_days = self.get_lastdays(date, dates, self.last_days)
            daily = self.stock_get_daily(search_days)
            index_delta_closeprice = {}
            for stock_list in daily:
                for stock in stock_list:
                    if stock["ts_code"] in stock_code_list:
                        index_delta_closeprice.setdefault(stock["ts_code"],[])
                        index_delta_closeprice[stock["ts_code"]].append(abs(stock["close"]-stock["pre_close"]))
            index_delta_closeprice_mean = {k:statistics.mean(v) for k,v in index_delta_closeprice.items()}



            # 计算资金流向前的系数
            days = search_days[-1:]
            daily = self.stock_get_daily(days)[0]
            daily = {i["ts_code"]:i for i in daily}
            index_param = {}
            for key in index_weight.keys():
                try:
                    index_param[key] = index_weight[key]/daily[key]["close"]*index_delta_closeprice_mean[key]
                    # index_param[key] = index_weight[key]
                except:
                    print("资金流向前系数计算:",key,days)

            # 合并资金流向
            if date2net_mf_amount_a_year:
                net_mf_amount_a_year = date2net_mf_amount_a_year[pretrade_date]
            else:
                net_mf_amount_a_year = 0
            tmp = self.pro.moneyflow(trade_date=date).to_dict(orient="records")
            for stock in tmp:
                if stock["ts_code"] in stock_code_list:
                    net_mf_amount_a_year = net_mf_amount_a_year + stock["net_mf_amount"]*index_param[stock["ts_code"]]
            self.tensorboard.addScalar(tag=self.fund_code+"/"+self.dir_name+"/net_mf_amount_a_year",value=net_mf_amount_a_year,index=num,timestamp=timestamp)
            date2net_mf_amount_a_year[date] = net_mf_amount_a_year

            # 查询当时的etf价格
            try:
                price = self.pro.fund_daily(ts_code=fund_code,trade_date=date).to_dict(orient="records")[0]
            except:
                print("当前时间，fund无价格",fund_code,date)
                price = {"open":0,"close":0}

            self.tensorboard.addScalarDict(tag=self.fund_code+"/"+self.dir_name+"/price",scalar_dict={"open":price["open"],"close":price["close"]}, index=num, timestamp=timestamp)
            time.sleep(0.12)

class Stock_amount_and_price(base_amount_and_price):
    def __init__(self):
        super().__init__()
        self.pro = ts.pro_api('3085222731857622989')
        self.pro._DataApi__http_url = "http://47.109.97.125:8080/tushare"
        self.log_dir = "./tensorboard_log_stock/"
        self.tensorboard = Tensorboard(self.log_dir )
        # self.tensorboard.emptyTensorboard()
        self.last_days = 10 # 计算前30天的均值
        self.stock_daily = {}
    
    def get_stock_daily(self,stock_code,search_days):
        for day in search_days:
            if day["cal_date"] not in self.stock_daily:
                self.stock_daily[day["cal_date"]] = {}
            if stock_code not in self.stock_daily[day["cal_date"]]:
                daily = self.pro.daily(ts_code=stock_code,trade_date=day["cal_date"]).to_dict(orient="records")[0]
                self.stock_daily[day["cal_date"]][stock_code] = daily
        
        return [self.stock_daily[day["cal_date"]][stock_code] for day in search_days]

    def run(self,stock_code,start_date="20241204",end_date="20251204"):
        date = self.pro.trade_cal(exchange='SSE', start_date=start_date, end_date=end_date)
        date = date.to_dict(orient="records")
        dates = [i for i in date if i["is_open"]==1]
        dates.sort(key=lambda x: int(x["cal_date"]))
        self.stock_code = stock_code

        mf_amount_a_year = 0
        for num,date in tqdm(enumerate(dates[self.last_days:])):
            date = date["cal_date"]
            timestamp = datetime.strptime(date, '%Y%m%d').timestamp()

            # 获取资金流向数据
            tmp = self.pro.moneyflow(ts_code=self.stock_code,trade_date=date).to_dict(orient="records")[0]
            net_mf_amount =  tmp["net_mf_amount"]
            mf_amount_a_year = mf_amount_a_year + net_mf_amount
            self.tensorboard.addScalar(tag=self.stock_code+"/mf_amount_a_year",value=mf_amount_a_year,index=num,timestamp=timestamp)

            # 获取成交量 价格
            search_days = self.get_lastdays(date, dates, self.last_days)
            dailys = self.get_stock_daily(self.stock_code,search_days)
            mean_volume = statistics.mean([i["vol"] for i in dailys])
            mean_delta_price = statistics.mean([abs(i["close"] - i["pre_close"]) for i in dailys])
            current_daily = dailys[-1]
            delta_price = abs(dailys[-1]["close"] - dailys[-1]["pre_close"])
            self.tensorboard.addScalarDict(tag=self.stock_code+"/volume",scalar_dict={"volume":current_daily["vol"],"mean_volume":mean_volume},index=num, timestamp=timestamp)
            self.tensorboard.addScalarDict(tag=self.stock_code+"/delta_price",scalar_dict={"mean_delta_price":mean_delta_price,"delta_price":delta_price},index=num, timestamp=timestamp)
            self.tensorboard.addScalarDict(tag=self.stock_code+"/price",scalar_dict={"open":current_daily["open"],"close":current_daily["close"]},index=num, timestamp=timestamp)
            time.sleep(0.12)






if __name__ == "__main__":
    writer = fund_amount_and_price()
    writer.run("515120.SH",end_date="20251208")

    # writer = Stock_amount_and_price()
    # writer.run("603259.SH", end_date="20251205")
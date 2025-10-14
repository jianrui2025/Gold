#  base类 抓取数据 从前到后依次遍历数据
# 抓取数据： 判断数据是否已经抓取下来，抓取最新的数据。？ 不至于
# 默认使用，增量抓取一遍数据。
# 遍历数据可以可以准备一个迭代器。从头到尾直接遍历操作直接放在子类里面实现。

import pandas as pd
from xtquant import xtdata
import json
import copy


class CallBackBase():
    def __init__(self, stock_code, period, start_time, end_time, incrementally=True):
        self.stock_code = stock_code
        self.period = period
        self.start_time = start_time
        self.end_time = end_time
        self.incrementally = incrementally

    def downAndLoadData(self):
        # 下载数据和载入数据
        xtdata.download_history_data(stock_code=self.stock_code, period=self.period, start_time=self.start_time, end_time=self.end_time, incrementally=self.incrementally)
        data = xtdata.get_market_data_ex(stock_list=[self.stock_code], period=self.period, start_time=self.start_time,end_time=self.end_time)
        return data

    def build_iter(self,data):
        pass

    def call(self,start_price, price_interval, stock_num_all, stock_num_wai, stock_num_lei, oneHand):
        return self.check(start_price, price_interval, stock_num_all, stock_num_wai, stock_num_lei, oneHand)

    def check(self,start_price, price_interval, stock_num_all, stock_num_wai, stock_num_lei, oneHand):
        # 重写
        pass

class CallBack_WangGe(CallBackBase):
    def __init__(self,  
                 stock_code, period, start_time, end_time):
        super().__init__(
            stock_code, period, start_time, end_time)

        # 数据载入和整理数据
        self.data = self.downAndLoadData()
        self.data_iter = self.build_iter(self.data)

    def build_iter(self,data):
        # 创建生成器对象
        data = data[self.stock_code].to_dict(orient="records")
        data = [i for i in data if 25200000 > i["time"]%86400000 > 5400000]
        data.sort(key=lambda x: x["time"])
        print(data[0])
        print(data[-1])
        return data

    def guandan(self):
        # 买
        if self.stock_num_wai >= self.oneHand:
            self.buy_price = self.start_price - self.price_interval
        else:
            self.buy_price = None
        # 卖
        if self.stock_num_lei >= self.oneHand:
            self.sale_price = self.start_price + self.price_interval
        else:
            self.sale_price = None

        print("挂单：基准价:{},买:{},卖:{}".format(str(self.start_price),str(self.buy_price),str(self.sale_price)))
    
    def init_conf(self,start_price, price_interval, stock_num_all, stock_num_wai, stock_num_lei, oneHand):
        self.start_price = start_price
        self.price_interval = price_interval
        self.stock_num_all = stock_num_all
        self.stock_num_wai = stock_num_wai
        self.stock_num_lei = stock_num_lei
        self.oneHand = oneHand
        self.shouxufei = 0.001 # 手续费：只在卖的时候收取

        # self.start_price = 7.411 # 起始价格
        # self.price_interval = 0.025 # 价格区间
        # self.stock_num_all = 30000 # 总量
        # self.stock_num_wai = 15000 # 外盘，钱
        # self.stock_num_lei = 15000 # 内盘，票
        # self.oneHand = 5000 # 一手交易数量
        
        self.shouyi = 0.0

    def check(self,start_price, price_interval, stock_num_all, stock_num_wai, stock_num_lei, oneHand):
        # 初始化参数
        self.init_conf(start_price, price_interval, stock_num_all, stock_num_wai, stock_num_lei, oneHand)
        # 挂单
        self.guandan()

        # 开始执行
        for oneline in self.data_iter:
            # 触发买单
            if self.buy_price != None and self.buy_price > float(oneline["lastPrice"]):
                self.stock_num_wai = self.stock_num_wai - self.oneHand
                self.stock_num_lei = self.stock_num_lei + self.oneHand
                self.start_price = self.buy_price
                self.guandan()
            
            # 触发卖单+结算收益
            elif self.sale_price != None and self.sale_price < float(oneline["lastPrice"]):
                #
                self.stock_num_wai = self.stock_num_wai + self.oneHand
                self.stock_num_lei = self.stock_num_lei - self.oneHand
                self.start_price = self.sale_price
                self.guandan()

                #
                self.shouyi = self.shouyi + (self.price_interval-self.shouxufei)*self.oneHand

            else:
                pass
        
        return self.shouyi

class CallBack_1d(CallBackBase):
    def __init__(self,
                 stock_code, period, start_time, end_time):
        super().__init__(
            stock_code, period, start_time, end_time)
        
        # 数据载入和整理数据
        self.data = self.downAndLoadData()
        self.data_iter = self.build_iter(self.data)
    
    def build_iter(self,data):
        data = data[self.stock_code].to_dict(orient='records')
        data.sort(key=lambda x: x["time"])
        return data
    
    def init_conf(self):
        self.summ = 0.0
        self.sample_num = 0
        self.snmm_volume = 0

    def call(self):
        self.init_conf()
        ave,ave_volume = self.check()
        return ave,ave_volume

    def check(self):
        for i in self.data_iter:
            diff = i["high"] - i["low"]
            volume = i["volume"]
            self.summ += diff
            self.snmm_volume += volume
            self.sample_num += 1
        ave = self.summ/self.sample_num
        ave_volume = self.snmm_volume/self.sample_num
        return ave,ave_volume
    
class CallBack_1d_simi(CallBackBase):
    def __init__(self,
                 stock_code, period, start_time, end_time, incrementally):
        super().__init__(
            stock_code, period, start_time, end_time, incrementally)
        
        # 数据载入和整理数据
        self.data = self.downAndLoadData()
        self.data_iter = self.build_iter(self.data)
    
    def build_iter(self,data):
        data = data[self.stock_code].to_dict(orient='records')
        data.sort(key=lambda x: x["time"])
        return data
    
    def init_conf(self):
        self.static_num  = 5
        self.aim_num = 1

    def call(self):
        self.init_conf()
        ave,ave_volume = self.check()
        return ave,ave_volume

    def msg_loss(self,base,sample,keys):
        loss = 0
        for key in keys:
            loss = loss + abs(base[key]-sample[key])
        return loss

    def check(self):
        # 裁剪分类+归一化操作
        sample = []
        for i in range(len(self.data_iter)-(self.static_num + self.aim_num)):
            sample.append(self.data_iter[i:i+self.static_num+self.aim_num])
        sample_tmp = []
        for one_com in sample:
            keys = {"open":0.0, "high":0.0, "low":0.0, "close":0.0 }  # volume
            for key in keys:
                for one in one_com[:-self.aim_num]:
                    keys[key] += one[key]
                keys[key] = keys[key] / self.static_num
            one_com_tmp = copy.deepcopy(one_com)
            for key in keys:
                for one in one_com_tmp:
                    one[key] = one[key] - keys[key]
            sample_tmp.append(one_com_tmp)
        sample = sample_tmp
        # 
        # 先测试最后一组数字
        base = sample[-1]
        sample = sample[:-1]
        loss = []
        for index in range(len(sample)):
            tmp = self.msg_loss(base[:-self.aim_num],sample[index][:-self.aim_num],keys)
            loss.append([index,tmp])
        
        loss.sort(key=lambda x: x[1],reverse=True)
        loss = loss[:20]
        for i in loss:
            print("最后一日",sample[i[0]][-2])
            print("最后一日",sample[i[0]][-1])
            print("----")

        return 

if __name__ == "__main__":

    # 测试网格交易的价格回测
    # start_price_list = [7.250+i*0.005 for i in range(0,61)]
    # # price_interval_list = [0.005,0.01,0.015,0.02,0.025,0.03,0.035,0.04,0.05,0.055,0.06,0.065,0.07]
    # price_interval_list = [0.005,0.01,0.015,0.02,0.025,0.03,0.035,0.04,0.05,0.055,0.06,0.065,0.07,0.075,0.08,0.085,0.09,0.95,0.1,0.105,0.110,0.115,0.120,0.125,0.130,0.135,0.140]
    # oneHand_list = [1500,3000,5000,7500,15000,30000]
    
    # start_price_list = [7.51]
    # price_interval_list = [0.13]
    # oneHand_list = [30000]

    # cb = CallBack_WangGe(
    #                         "518880.SH","tick","20250805","20250812")
    # with open("../result.json_20250805_20250812","w") as f:
    #     for start_price in start_price_list:
    #         for price_interval in price_interval_list:
    #             for oneHand in oneHand_list:
    #                 tmp = int(30000/oneHand)
    #                 for i in range(tmp+1):
    #                     stock_num_wai = oneHand*i
    #                     stock_num_lei = 30000 - stock_num_wai
    #                     shouyi = cb.call(start_price, price_interval, 30000, stock_num_wai, stock_num_lei, oneHand)
    #                     f.write(json.dumps({"start_price":start_price, "price_interval":price_interval, "oneHand":oneHand, "stock_num_wai":stock_num_wai, "stock_num_lei":stock_num_lei, "shouyi":shouyi})+"\n")
    #                     print({"start_price":start_price, "price_interval":price_interval, "oneHand":oneHand, "stock_num_wai":stock_num_wai, "stock_num_lei":stock_num_lei,"shouyi":shouyi})


    # 均值回测
    # date = [["20250512","20250612"],["20250612","20250712"],["20250712","20250812"],["20250512","20250812"]]
    # for s,e in date:
    #     cd = CallBack_1d(
    #         "518880.SH","1d",s,e
    #     )
    #     ave,ave_volume = cd.call()
    #     print(s,e,ave,ave_volume)
                        
    # 相似度查询
    cd = CallBack_1d_simi(
            "518880.SH","1d","20140101","",True
        )

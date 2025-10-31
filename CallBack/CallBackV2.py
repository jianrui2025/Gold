import pandas as pd
import itertools
import adata
import statistics
import json
import multiprocessing as mp
import requests
import time
import io
import os
import pickle
import copy
from datetime import datetime, timedelta

# 架构设计：
# 在一个对象中，将所有的fund_code测试一遍。
# 网络搜索所有的参数
# 按照策略进行回调

class CallBackV2Base():
    def __init__(self) -> None:
        self.fund_code_list = []
        self.HyperParam_dict = {}

    def downLoadData(self,**kwargs):
        # 下载数据
        pass

    def buildHyperParam(self,HyperParam_dict):
        # 网络搜索超参数集合
        if not HyperParam_dict:
            raise "self.HyperParam_dict为空，请设置超参数"
        
        keys = list(HyperParam_dict.keys())
        values = list(HyperParam_dict.values())
        HyperParam_list = []
        for item in itertools.product(*values):
            tmp = {k:v for k,v in zip(keys,item)}
            HyperParam_list.append(tmp)
        
        return HyperParam_list
        
    def run_onece(self,**kwargs):
        # 一个数据，一套超参数下的收益
        pass

    def one_process(self,fund_code):
        data = self.downLoadData(fund_code=fund_code)
        with open(self.output_file.format(fund_code=fund_code),"w") as f:
            print("创建：",self.output_file.format(fund_code=fund_code))
        HyperParam_list = self.buildHyperParam(self.HyperParam_dict)
        for one_HyperParam in HyperParam_list:
            start = time.time()
            self.run_onece(data=data,fund_code=fund_code,**one_HyperParam)
            end = time.time()

    def run(self):
        if not self.fund_code_list:
            raise "self.fund_code_list为空，请设置"
        
        with mp.Pool(processes=50) as pool:
            pool.map(self.one_process, self.fund_code_list)

        # self.one_process(self.fund_code_list[0])
    
    def get_code(self,path):
        path = path.replace("{fund_code}","")
        files1 = [os.path.join(path, f) for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
        fund_code = [i.split("/")[-1] for i in files1]
        return fund_code

    def compute_precise(self,result):
        i = result
        log = i["log"]
        buy_num = 0
        yingli_num = 0
        for index,k in enumerate(log):
            if index%2 == 0:
                buy_num += 1
            else:
                if "盈利卖出" in log[k]:
                    yingli_num += 1
        if buy_num != 0:
            i["precion"] = yingli_num/buy_num
            i["buy_num"] = buy_num
        else:
            i["precion"] = 0
            i["buy_num"] = 0
    
    def compute_last_money(self,result):
        i = result
        log = i["log"]
        last_money = 200000
        buy_days = ""
        keep_days = []
        for k,v in log.items():
            if "卖出" in v:
                last_money = v.split("==>")[1]
                last_money = json.loads(last_money)["money"]
                tmp_day = self.compute_days(buy_days,k)
                keep_days.append(tmp_day)
                buy_days = ""
            else:
                buy_days = k
        i["last_money"] = last_money
        if keep_days:
            i["mean_keep_day"] = statistics.mean(keep_days)
        else:
            i["mean_keep_day"] = 0

    def compute_days(self,start_date,end_date):
        start = datetime.strptime(start_date, "%Y%m%d")
        end = datetime.strptime(end_date, "%Y%m%d")
        day_count = 0
        current = start
        while current <= end:
            # weekday(): 周一=0, 周日=6
            if current.weekday() < 5:
                if current.strftime('%m%d') not in ["0501","0502","0503","0504","0505","1001","1002","1003","1004","1005","1006","1007"]:
                    day_count += 1
            current += timedelta(days=1)
        return day_count

    def max_precise(self,re):
        max_ = -1
        max_re = ""
        for i in re:
            if max_ < i["precion"]:
                max_ = i["precion"]
                max_re = i
        return max_re
    
    def max_money(self,re):
        max_ = -1
        max_re = ""
        for i in re:
            if max_ < i["last_money"]:
                max_ = i["last_money"]
                max_re = i
        return max_re

    def get_info(self,kwargs):
        path = kwargs["path"]
        with open(path,"r") as f:
            re1 = [json.loads(i.strip()) for i in f]
            re1 = [i for i in re1 if i["mean_long_day"]>i["mean_short_day"]]
        for i in re1:
            self.compute_precise(i)
            self.compute_last_money(i)

        max_re1 = self.max_precise(re1)
        max_re2 = self.max_money(re1)

        return {"precise":max_re1,"last_money":max_re2}

    def get_money_info(self,kwargs):
        def build_key(i):
            key = ["mean_long_day","mean_short_day",'Volume_day',"ZhiShun","ShouYi"]
            return "*".join([str(i[k]) for k in key])
        
        path = kwargs["path"]
        with open(path,"r") as f:
            re1 = [json.loads(i.strip()) for i in f]
            re1 = [i for i in re1 if i["mean_long_day"]>i["mean_short_day"]]
            if len(re1) !=  730:
                return False
            # re1 = [i for i in re1 if i["ZhiShun"]==-0.03 and i["ShouYi"]==0.01]
            for i in re1:
                self.compute_precise(i)
                self.compute_last_money(i)
            re_1_dict = {build_key(i):i for i in re1}

        
        # 准确率比较， 比较准确率最高的参数下，不同的收益率之间，准确率的差异
        max_ = -1
        max_re = ""
        for i in re1:
            if i["last_money"] > max_:
                max_ = i["last_money"]
                max_re = i
        key = build_key(max_re)
        key_list = key.split("*")
        key_list_Shouyi = [copy.deepcopy(key_list) for i in range(len(self.HyperParam_dict["ZhiShun"]))]
        for i in range(len(key_list_Shouyi)):
            key_list_Shouyi[i][3] = str(self.HyperParam_dict["ZhiShun"][i])
        key_list_Shouyi = ["*".join(i) for i in key_list_Shouyi]

        return [re_1_dict[i] for i in key_list_Shouyi if i in re_1_dict]
        
    def select_conf(self,kwargs):
        def build_key(i):
            key = ["mean_long_day","mean_short_day",'Volume_day',"ZhiShun","ShouYi"]
            return "*".join([str(i[k]) for k in key])
        
        path = kwargs["path"]
        with open(path,"r") as f:
            re1 = [json.loads(i.strip()) for i in f]
            re1 = [i for i in re1 if i["mean_long_day"]>i["mean_short_day"]]
            # if len(re1) !=  675:
            #     return False
            for i in re1:
                self.compute_precise(i)
                self.compute_last_money(i)
            re_1_dict = {build_key(i):i for i in re1}

        # 挑选准确率90
        re1 = [i for i in re1 if i["last_money"]>200000]
        re1 = [i for i in re1 if i["precion"]>0.80 and i["buy_num"]>4]
        # re1 = [i for i in re1 if i["mean_keep_day"]<5]
      
        # precion 最大
        max_ = -1
        for i in re1:
            if i["precion"] > max_:
                max_ = i["precion"]
        re1 = [i for i in re1 if i["precion"]==max_]
        
        # last_money最大的参数。
        max_ = -1
        max_re = ""
        for i in re1:
            if i["last_money"] > max_:
                max_ = i["last_money"]
                max_re = i

        return max_re

    def precise(self):
        fund_code = self.get_code(self.output_file)
        # fund_code = ["513500.SH"]
        fund_path = [self.output_file.format(fund_code=i) for i in fund_code]
        fund_path_list = [{"path":i}  for i in fund_path]
        with mp.Pool(processes=4) as pool:
            precise = pool.map(self.select_conf, fund_path_list)
            numm = 0
            numm_none = 0
            with open("/home/jianrui/workspace/Gold/CallBack/conf/MeanLineAndVolume_v10.jsonl","w") as f:
                for i in precise:
                    if i:
                        # i.pop("log")
                        f.write(json.dumps(i,ensure_ascii=False)+"\n")
                        numm += 1
                    else:
                        numm_none += 1
            print("未筛选出策略:",numm_none)
            print("筛选出策略:",numm)


        
class CallBackV2_MeanLineAndVolumeV1(CallBackV2Base):
    # 使用 均线和柏林带 的策略
    # 当短期均线突破长期均线 → 买入；
    # 当短期均线跌破长期均线 → 卖出；
    # 但是在实际操作中，固定固定盈利卖出

    def __init__(self):
        super().__init__()
        self.fund_code_list = ["515250", "588810", "159378", "513230", "513110", "159866", "561300", "588400", "512080", "159757", "159996", "515100", "516350", "159822", "515290", "159242", "159359", "520860", "159241", "516920", "513850", "159360", "159929", "159692", "513880", "159867", "159368", "589960", "159628", "512560", "159837", "513660", "159767", "588230", "159323", "516970", "159545", "159205", "560510", "588990", "159876", "159297", "159637", "510360", "159696", "159622", "159102", "515720", "159752", "520510", "159707", "159246", "159790", "588890", "159718", "517380", "515180", "159839", "159699", "588120", "516290", "159583", "520720", "588870", "513390", "515260", "589020", "516120", "516880", "159715", "515710", "589720", "515800", "159901", "159632", "516650", "588330", "560080", "516100", "589560", "560980", "512900", "513350", "159531", "512290", "513820", "589520", "510330", "560770", "512820", "589010", "510100", "159814", "560780", "159562", "512700", "515650", "159259", "159278", "515300", "159994", "159745", "159566", "589000", "513910", "159775", "513220", "513600", "560050", "159938", "159608", "159593", "159768", "159568", "515450", "513920", "159358", "588300", "159366", "513520", "520990", "588800", "589680", "159227", "588930", "159934", "515030", "512810", "513280", "159659", "159845", "159339", "513560", "159269", "520970", "588830", "588180", "159206", "512930", "515080", "159286", "520690", "159922", "159937", "512680", "513150", "588780", "510760", "589800", "516010", "513550", "516160", "588730", "159502", "511990", "516820", "588710", "159735", "159615", "561980", "520840", "159518", "510720", "159652", "159977", "513890", "513500", "516090", "516510", "560090", "159750", "588380", "515400", "159993", "159713", "159595", "159919", "159501", "562510", "562880", "159272", "516860", "159741", "159381", "159828", "510880", "515000", "159998", "510150", "159681", "560530", "159611", "159751", "513190", "588290", "159825", "512200", "515230", "159399", "520920", "513970", "159691", "511880", "510310", "159607", "560860", "520600", "520830", "516020", "159780", "159887", "515070", "159559", "588750", "513690", "516780", "159875", "563300", "515050", "520700", "513040", "510900", "588090", "513360", "561160", "510500", "159202", "513300", "159952", "513630", "513020", "159201", "513860", "159859", "588170", "159353", "159840", "562800", "588030", "159682", "510210", "159529", "159101", "159857", "159688", "159801", "512670", "517520", "159506", "588050", "159842", "512760", "588220", "159732", "513700", "515170", "513780", "159262", "513100", "512890", "159883", "159329", "159316", "159865", "513580", "159781", "159217", "588060", "520500", "159967", "515980", "512100", "518880", "159841", "520980", "159766", "159605", "159770", "512070", "159869", "515120", "513260", "159813", "159530", "588760", "159509", "159852", "512400", "159516", "159636", "513160", "515220", "520880", "515880", "159995", "159992", "588790", "159941", "159796", "510300", "159742", "513750", "159819", "561910", "563220", "515790", "563800", "513380", "159851", "159892", "588080", "512710", "513980", "159363", "512480", "159870", "512010", "159570", "512800", "159567", "159755", "159351", "512690", "562500", "588200", "159915", "513310", "159949", "513010", "513050", "513060", "159361", "512000", "512880", "563360", "159338", "159352", "159792", "159740", "588000", "512050", "513120", "513090", "513180", "513130", "513330"]
        # self.fund_code_list = ["159929", "159692", "513660", "159901", "159632", "516650", "510330", "510100", "513600", "159934", "515030", "512810", "159845", "159339", "512930", "515080", "159922", "159937", "512680", "513150", "588780", "510760", "516010", "513550", "516160", "511990", "516820", "159615", "561980", "159977", "513890", "513500", "516090", "516510", "560090", "159750", "588380", "515400", "159993", "159713", "159595", "159919", "159501", "562510", "562880", "516860", "159741", "159381", "159828", "510880", "515000", "159998", "510150", "159681", "560530", "159611", "159751", "513190", "588290", "159825", "512200", "515230", "159399", "520920", "513970", "159691", "511880", "510310", "159607", "560860", "520600", "520830", "516020", "159780", "159887", "515070", "159559", "588750", "513690", "516780", "159875", "563300", "515050", "520700", "513040", "510900", "588090", "513360", "561160", "510500", "159202", "513300", "159952", "513630", "513020", "159201", "513860", "159859", "588170", "159353", "159840", "562800", "588030", "159682", "510210", "159529", "159101", "159857", "159688", "159801", "512670", "517520", "159506", "588050", "159842", "512760", "588220", "159732", "513700", "515170", "513780", "159262", "513100", "512890", "159883", "159329", "159316", "159865", "513580", "159781", "159217", "588060", "520500", "159967", "515980", "512100", "518880", "159841", "520980", "159766", "159605", "159770", "512070", "159869", "515120", "513260", "159813", "159530", "588760", "159509", "159852", "512400", "159516", "159636", "513160", "515220", "520880", "515880", "159995", "159992", "588790", "159941", "159796", "510300", "159742", "513750", "159819", "561910", "563220", "515790", "563800", "513380", "159851", "159892", "588080", "512710", "513980", "159363", "512480", "159870", "512010", "159570", "512800", "159567", "159755", "159351", "512690", "562500", "588200", "159915", "513310", "159949", "513010", "513050", "513060", "159361", "512000", "512880", "563360", "159338", "159352", "159792", "159740", "588000", "512050", "513120", "513090", "513180", "513130", "513330"]
        # self.fund_code_list = ["159681", "159607", "159559", "588750", "513690", "515050", "520700", "588090", "513360", "561160", "510500", "159202", "513300", "159952", "513630", "588030", "159682", "159857", "159688", "517520", "159506", "588050", "512760", "588220", "159732", "513700", "513100", "512890", "159316", "159865", "513580", "588060", "520500", "159967", "512100", "518880", "159841", "520980", "159605", "159770", "512070", "159869", "515120", "513260", "159813", "159530", "588760", "159509", "159852", "512400", "159516", "159636", "513160", "515220", "520880", "515880", "159995", "159992", "588790", "159941", "159796", "510300", "159742", "513750", "159819", "561910", "563220", "515790", "563800", "513380", "159851", "159892", "588080", "512710", "513980", "159363", "512480", "159870", "512010", "159570", "512800", "159567", "159755", "159351", "512690", "562500", "588200", "159915", "513310", "159949", "513010", "513050", "513060", "159361", "512000", "512880", "563360", "159338", "159352", "159792", "159740", "588000", "512050", "513120", "513090", "513180", "513130", "513330"]
        self.HyperParam_dict = {
            "mean_long_day":[9,12,13,18,21,24,27,30,33,36,39,42,45,48,51,54,57,60,63],
            "mean_short_day":[3,6,9,12,13,18,21,24,27,30],
            "BollingerBands_day":[20],
            "k":[2], # 布林值中的K倍标准差
            "ShouYi":[0.005,0.01,0.015,0.02,0.025,0.03,0.035,0.04],
            "ZhiShun":[-0.005,-0.01,-0.015,-0.02,-0.025,-0.03,-0.035,-0.04],
        }
        self.output_file = "/home/jianrui/workspace/Gold/CallBack/log_前400/{fund_code}"
        self.max_len = 400
    
    def downLoadData(self,**kwargs):
        if "fund_code" in kwargs:
            df = adata.fund.market.get_market_etf(fund_code=kwargs["fund_code"])
            df = df.to_dict(orient="records")
            if self.max_len > 0:
                df = df[:-self.max_len]
            return df
        else:
            raise

    def run_onece(self,**kwrags):
        data = kwrags["data"]
        kwrags.pop("data")
        mean_long_day = kwrags["mean_long_day"]
        mean_short_day = kwrags["mean_short_day"]
        BollingerBands_day = kwrags["BollingerBands_day"]
        k = kwrags["k"]
        ShouYi = kwrags["ShouYi"]
        ZhiShun = kwrags["ZhiShun"]
        fund_code = kwrags["fund_code"]       

        if mean_long_day <= mean_short_day:
            return False


        max_day = max(mean_long_day,mean_short_day,BollingerBands_day)

        status = {"money":200000,"stock":0}
        single = 0
        log = {}

        # 接下来就是一天的时间
        for index_day in range(max_day,len(data)):
            current_data = data[index_day]
            if single == 0:
                price = float(current_data["low"])+0.002
                while price <= float(current_data["high"])-0.002:
                    mean_long_value = statistics.mean(
                        [float(i["close"])  for i in data[index_day-mean_long_day:index_day]] + [price]
                        )
                    mean_short_value = statistics.mean(
                        [float(i["close"])  for i in data[index_day-mean_short_day:index_day]] + [price]
                        )
                    BollingerBands_meanValue = statistics.mean(
                        [float(i["close"])  for i in data[index_day-BollingerBands_day:index_day]] + [price]
                        )
                    BollingerBands_std = statistics.stdev(
                        [float(i["close"])  for i in data[index_day-BollingerBands_day:index_day]] + [price]
                    )

                    BollingerBands_meanValue_upper = BollingerBands_meanValue + k*BollingerBands_std
                    BollingerBands_meanValue_lower = BollingerBands_meanValue - k*BollingerBands_std

                    # if mean_short_value >  mean_long_value and price < BollingerBands_meanValue_lower:
                    if mean_short_value >  mean_long_value:
                        single = 1
                        status["stock"] = status["money"]/price
                        status["money"] = 0
                        bought_price = price
                        log[current_data["trade_time"].split(" ")[0]] = "买入==>"+json.dumps(status,ensure_ascii=False)
                        break
                    else:
                        pass
                    price = price + 0.001
                    
            elif single == 1:
                price = float(current_data["low"])+0.002
                while price <= float(current_data["high"])-0.002:
                    if (price - bought_price)/bought_price > ShouYi or (price - bought_price)/bought_price < ZhiShun:
                        single = 0
                        status["money"] = status["stock"]*price
                        status["stock"] = 0
                        if (price - bought_price)/bought_price > ShouYi:
                            bought_price = None
                            log[current_data["trade_time"].split(" ")[0]] = "盈利卖出==>"+json.dumps(status,ensure_ascii=False)
                        elif (price - bought_price)/bought_price < ZhiShun:
                            bought_price = None
                            log[current_data["trade_time"].split(" ")[0]] = "止损卖出==>"+json.dumps(status,ensure_ascii=False)
                        break
                    else:
                        pass
                    price = price + 0.001
        
        # 输出日志
        kwrags["log"] = log

        with open(self.output_file.format(fund_code=fund_code),"a+") as f:
            f.write(json.dumps(kwrags,ensure_ascii=False)+"\n")

        return True


class CallBackV2_MeanLineAndVolumeV2(CallBackV2Base):
    # 使用 均线和成交量 的策略
    # 当短期均线突破长期均线，且成交量大于历史平均 → 买入；
    # 当短期均线跌破长期均线 → 卖出；
    # 但是在实际操作中，固定固定盈利卖出

    def __init__(self):
        super().__init__()
        self.fund_code_list1 = ["159001.SZ", "159001.SZ", "159101.SZ", "159101.SZ", "159201.SZ", "159201.SZ", "159202.SZ", "159202.SZ", "159205.SZ", "159205.SZ", "159206.SZ", "159206.SZ", "159217.SZ", "159217.SZ", "159218.SZ", "159218.SZ", "159222.SZ", "159222.SZ", "159227.SZ", "159227.SZ", "159232.SZ", "159232.SZ", "159235.SZ", "159235.SZ", "159241.SZ", "159241.SZ", "159242.SZ", "159242.SZ", "159246.SZ", "159246.SZ", "159259.SZ", "159259.SZ", "159262.SZ", "159262.SZ", "159265.SZ", "159265.SZ", "159269.SZ", "159269.SZ", "159272.SZ", "159272.SZ", "159273.SZ", "159273.SZ", "159278.SZ", "159278.SZ", "159281.SZ", "159281.SZ", "159286.SZ", "159286.SZ", "159297.SZ", "159297.SZ", "159300.SZ", "159300.SZ", "159316.SZ", "159316.SZ", "159323.SZ", "159323.SZ", "159326.SZ", "159326.SZ", "159327.SZ", "159327.SZ", "159329.SZ", "159329.SZ", "159338.SZ", "159338.SZ", "159339.SZ", "159339.SZ", "159351.SZ", "159351.SZ", "159352.SZ", "159352.SZ", "159353.SZ", "159353.SZ", "159356.SZ", "159356.SZ", "159358.SZ", "159358.SZ", "159359.SZ", "159359.SZ", "159360.SZ", "159360.SZ", "159361.SZ", "159361.SZ", "159363.SZ", "159363.SZ", "159366.SZ", "159366.SZ", "159368.SZ", "159368.SZ", "159381.SZ", "159381.SZ", "159382.SZ", "159382.SZ", "159388.SZ", "159388.SZ", "159399.SZ", "159399.SZ", "159501.SZ", "159501.SZ", "159502.SZ", "159502.SZ", "159506.SZ", "159506.SZ", "159509.SZ", "159509.SZ", "159513.SZ", "159513.SZ", "159516.SZ", "159516.SZ", "159518.SZ", "159518.SZ", "159529.SZ", "159529.SZ", "159530.SZ", "159530.SZ", "159531.SZ", "159531.SZ", "159545.SZ", "159545.SZ", "159552.SZ", "159552.SZ", "159558.SZ", "159558.SZ", "159559.SZ", "159559.SZ", "159561.SZ", "159561.SZ", "159562.SZ", "159562.SZ", "159566.SZ", "159566.SZ", "159567.SZ", "159567.SZ", "159568.SZ", "159568.SZ", "159570.SZ", "159570.SZ", "159577.SZ", "159577.SZ", "159582.SZ", "159582.SZ", "159583.SZ", "159583.SZ", "159592.SZ", "159592.SZ", "159593.SZ", "159593.SZ", "159595.SZ", "159595.SZ", "159599.SZ", "159599.SZ", "159605.SZ", "159605.SZ", "159607.SZ", "159607.SZ", "159608.SZ", "159608.SZ", "159611.SZ", "159611.SZ", "159615.SZ", "159615.SZ", "159622.SZ", "159622.SZ", "159623.SZ", "159623.SZ", "159628.SZ", "159628.SZ", "159629.SZ", "159629.SZ", "159632.SZ", "159632.SZ", "159633.SZ", "159633.SZ", "159636.SZ", "159636.SZ", "159652.SZ", "159652.SZ", "159655.SZ", "159655.SZ", "159659.SZ", "159659.SZ", "159660.SZ", "159660.SZ", "159667.SZ", "159667.SZ", "159681.SZ", "159681.SZ", "159682.SZ", "159682.SZ", "159687.SZ", "159687.SZ", "159688.SZ", "159688.SZ", "159691.SZ", "159691.SZ", "159692.SZ", "159692.SZ", "159696.SZ", "159696.SZ", "159699.SZ", "159699.SZ", "159713.SZ", "159713.SZ", "159715.SZ", "159715.SZ", "159732.SZ", "159732.SZ", "159735.SZ", "159735.SZ", "159740.SZ", "159740.SZ", "159741.SZ", "159741.SZ", "159742.SZ", "159742.SZ", "159745.SZ", "159745.SZ", "159747.SZ", "159747.SZ", "159750.SZ", "159750.SZ", "159751.SZ", "159751.SZ", "159755.SZ", "159755.SZ", "159766.SZ", "159766.SZ", "159770.SZ", "159770.SZ", "159775.SZ", "159775.SZ", "159780.SZ", "159780.SZ", "159781.SZ", "159781.SZ", "159783.SZ", "159783.SZ", "159790.SZ", "159790.SZ", "159792.SZ", "159792.SZ", "159796.SZ", "159796.SZ", "159801.SZ", "159801.SZ", "159807.SZ", "159807.SZ", "159813.SZ", "159813.SZ", "159814.SZ", "159814.SZ", "159819.SZ", "159819.SZ", "159825.SZ", "159825.SZ", "159828.SZ", "159828.SZ", "159840.SZ", "159840.SZ", "159841.SZ", "159841.SZ", "159842.SZ", "159842.SZ", "159845.SZ", "159845.SZ", "159851.SZ", "159851.SZ", "159852.SZ", "159852.SZ", "159857.SZ", "159857.SZ", "159859.SZ", "159859.SZ", "159865.SZ", "159865.SZ", "159866.SZ", "159866.SZ", "159869.SZ", "159869.SZ", "159870.SZ", "159870.SZ", "159871.SZ", "159871.SZ", "159875.SZ", "159875.SZ", "159876.SZ", "159876.SZ", "159883.SZ", "159883.SZ", "159887.SZ", "159887.SZ", "159892.SZ", "159892.SZ", "159901.SZ", "159901.SZ", "159908.SZ", "159908.SZ", "159915.SZ", "159915.SZ", "159919.SZ", "159919.SZ", "159920.SZ", "159920.SZ", "159922.SZ", "159922.SZ", "159925.SZ", "159925.SZ", "159928.SZ", "159928.SZ", "159929.SZ", "159929.SZ", "159930.SZ", "159930.SZ", "159934.SZ", "159934.SZ", "159937.SZ", "159937.SZ", "159941.SZ", "159941.SZ", "159948.SZ", "159948.SZ", "159949.SZ", "159949.SZ", "159952.SZ", "159952.SZ", "159957.SZ", "159957.SZ", "159967.SZ", "159967.SZ", "159971.SZ", "159971.SZ", "159977.SZ", "159977.SZ", "159992.SZ", "159992.SZ", "159993.SZ", "159993.SZ", "159994.SZ", "159994.SZ", "159995.SZ", "159995.SZ", "159996.SZ", "159996.SZ", "159998.SZ", "159998.SZ", "510050.SH", "510050.SH", "510100.SH", "510100.SH", "510150.SH", "510150.SH", "510180.SH", "510180.SH", "510210.SH", "510210.SH", "510230.SH", "510230.SH", "510300.SH", "510300.SH", "510310.SH", "510310.SH", "510330.SH", "510330.SH", "510360.SH", "510360.SH", "510500.SH", "510500.SH", "510670.SH", "510670.SH", "510720.SH", "510720.SH", "510760.SH", "510760.SH", "510880.SH", "510880.SH", "510900.SH", "510900.SH", "510980.SH", "510980.SH", "511160.SH", "511160.SH", "511660.SH", "511660.SH", "511700.SH", "511700.SH", "511850.SH", "511850.SH", "511880.SH", "511880.SH", "511990.SH", "511990.SH", "512000.SH", "512000.SH", "512010.SH", "512010.SH", "512020.SH", "512020.SH", "512050.SH", "512050.SH", "512070.SH", "512070.SH", "512080.SH", "512080.SH", "512090.SH", "512090.SH", "512100.SH", "512100.SH", "512170.SH", "512170.SH", "512200.SH", "512200.SH", "512290.SH", "512290.SH", "512400.SH", "512400.SH", "512480.SH", "512480.SH", "512500.SH", "512500.SH", "512560.SH", "512560.SH", "512570.SH", "512570.SH", "512630.SH", "512630.SH", "512660.SH", "512660.SH", "512670.SH", "512670.SH", "512680.SH", "512680.SH", "512690.SH", "512690.SH", "512700.SH", "512700.SH", "512710.SH", "512710.SH", "512760.SH", "512760.SH", "512800.SH", "512800.SH", "512810.SH", "512810.SH", "512820.SH", "512820.SH", "512880.SH", "512880.SH", "512890.SH", "512890.SH", "512900.SH", "512900.SH", "512930.SH", "512930.SH", "512980.SH", "512980.SH", "513000.SH", "513000.SH", "513010.SH", "513010.SH", "513020.SH", "513020.SH", "513030.SH", "513030.SH", "513040.SH", "513040.SH", "513050.SH", "513050.SH", "513060.SH", "513060.SH", "513080.SH", "513080.SH", "513090.SH", "513090.SH", "513100.SH", "513100.SH", "513110.SH", "513110.SH", "513120.SH", "513120.SH", "513130.SH", "513130.SH", "513140.SH", "513140.SH", "513150.SH", "513150.SH", "513160.SH", "513160.SH", "513180.SH", "513180.SH", "513190.SH", "513190.SH", "513200.SH", "513200.SH", "513220.SH", "513220.SH", "513260.SH", "513260.SH", "513280.SH", "513280.SH", "513290.SH", "513290.SH", "513300.SH", "513300.SH", "513310.SH", "513310.SH", "513330.SH", "513330.SH", "513350.SH", "513350.SH", "513360.SH", "513360.SH", "513380.SH", "513380.SH", "513390.SH", "513390.SH", "513400.SH", "513400.SH", "513500.SH", "513500.SH", "513520.SH", "513520.SH", "513530.SH", "513530.SH", "513550.SH", "513550.SH", "513560.SH", "513560.SH", "513580.SH", "513580.SH", "513600.SH", "513600.SH", "513630.SH", "513630.SH", "513650.SH", "513650.SH", "513660.SH", "513660.SH", "513690.SH", "513690.SH", "513700.SH", "513700.SH", "513730.SH", "513730.SH", "513750.SH", "513750.SH", "513770.SH", "513770.SH", "513780.SH", "513780.SH", "513800.SH", "513800.SH", "513820.SH", "513820.SH", "513850.SH", "513850.SH", "513860.SH", "513860.SH", "513870.SH", "513870.SH", "513880.SH", "513880.SH", "513890.SH", "513890.SH", "513910.SH", "513910.SH", "513920.SH", "513920.SH", "513950.SH", "513950.SH", "513970.SH", "513970.SH", "513980.SH", "513980.SH", "515000.SH", "515000.SH", "515010.SH", "515010.SH", "515020.SH", "515020.SH", "515030.SH", "515030.SH", "515050.SH", "515050.SH", "515070.SH", "515070.SH", "515080.SH", "515080.SH", "515100.SH", "515100.SH", "515120.SH", "515120.SH", "515170.SH", "515170.SH", "515180.SH", "515180.SH", "515210.SH", "515210.SH", "515220.SH", "515220.SH", "515230.SH", "515230.SH", "515290.SH", "515290.SH", "515300.SH", "515300.SH", "515400.SH", "515400.SH", "515450.SH", "515450.SH", "515650.SH", "515650.SH", "515700.SH", "515700.SH", "515790.SH", "515790.SH", "515800.SH", "515800.SH", "515880.SH", "515880.SH", "515980.SH", "515980.SH", "516010.SH", "516010.SH", "516020.SH", "516020.SH", "516090.SH", "516090.SH", "516100.SH", "516100.SH", "516120.SH", "516120.SH", "516150.SH", "516150.SH", "516160.SH", "516160.SH", "516290.SH", "516290.SH", "516310.SH", "516310.SH", "516350.SH", "516350.SH", "516510.SH", "516510.SH", "516640.SH", "516640.SH", "516650.SH", "516650.SH", "516670.SH", "516670.SH", "516780.SH", "516780.SH", "516820.SH", "516820.SH", "516860.SH", "516860.SH", "516880.SH", "516880.SH", "516920.SH", "516920.SH", "516970.SH", "516970.SH", "517520.SH", "517520.SH", "517900.SH", "517900.SH", "518600.SH", "518600.SH", "518660.SH", "518660.SH", "518680.SH", "518680.SH", "518800.SH", "518800.SH", "518850.SH", "518850.SH", "518860.SH", "518860.SH", "518880.SH", "518880.SH", "518890.SH", "518890.SH", "520500.SH", "520500.SH", "520550.SH", "520550.SH", "520600.SH", "520600.SH", "520660.SH", "520660.SH", "520690.SH", "520690.SH", "520700.SH", "520700.SH", "520830.SH", "520830.SH", "520840.SH", "520840.SH", "520860.SH", "520860.SH", "520880.SH", "520880.SH", "520890.SH", "520890.SH", "520900.SH", "520900.SH", "520920.SH", "520920.SH", "520970.SH", "520970.SH", "520980.SH", "520980.SH", "520990.SH", "520990.SH", "560010.SH", "560010.SH", "560050.SH", "560050.SH", "560080.SH", "560080.SH", "560090.SH", "560090.SH", "560350.SH", "560350.SH", "560510.SH", "560510.SH", "560530.SH", "560530.SH", "560570.SH", "560570.SH", "560610.SH", "560610.SH", "560770.SH", "560770.SH", "560780.SH", "560780.SH", "560860.SH", "560860.SH", "561160.SH", "561160.SH", "561300.SH", "561300.SH", "561560.SH", "561560.SH", "561910.SH", "561910.SH", "561980.SH", "561980.SH", "562500.SH", "562500.SH", "562510.SH", "562510.SH", "562590.SH", "562590.SH", "562800.SH", "562800.SH", "562880.SH", "562880.SH", "563020.SH", "563020.SH", "563220.SH", "563220.SH", "563300.SH", "563300.SH", "563360.SH", "563360.SH", "563750.SH", "563750.SH", "563800.SH", "563800.SH", "563880.SH", "563880.SH", "588000.SH", "588000.SH", "588020.SH", "588020.SH", "588030.SH", "588030.SH", "588050.SH", "588050.SH", "588060.SH", "588060.SH", "588080.SH", "588080.SH", "588090.SH", "588090.SH", "588100.SH", "588100.SH", "588120.SH", "588120.SH", "588170.SH", "588170.SH", "588180.SH", "588180.SH", "588190.SH", "588190.SH", "588200.SH", "588200.SH", "588220.SH", "588220.SH", "588230.SH", "588230.SH", "588290.SH", "588290.SH", "588300.SH", "588300.SH", "588330.SH", "588330.SH", "588380.SH", "588380.SH", "588400.SH", "588400.SH", "588460.SH", "588460.SH", "588710.SH", "588710.SH", "588730.SH", "588730.SH", "588750.SH", "588750.SH", "588760.SH", "588760.SH", "588780.SH", "588780.SH", "588790.SH", "588790.SH", "588800.SH", "588800.SH", "588810.SH", "588810.SH", "588830.SH", "588830.SH", "588870.SH", "588870.SH", "588890.SH", "588890.SH", "588920.SH", "588920.SH", "588930.SH", "588930.SH", "588990.SH", "588990.SH", "589000.SH", "589000.SH", "589010.SH", "589010.SH", "589020.SH", "589020.SH", "589630.SH", "589630.SH", "589660.SH", "589660.SH", "589680.SH", "589680.SH", "589720.SH", "589720.SH", "589800.SH", "589800.SH", "589850.SH", "589850.SH"] 
        self.fund_code_list1 = list(set(self.fund_code_list1))
        self.fund_code_list = self.fund_code_list1
        print("fund code数量",len(self.fund_code_list))
        self.HyperParam_dict = {
            "mean_long_day":[9,12,13,15,18,21,24,27,30,33,36,39,42,45,48,51,54,57],
            "mean_short_day":[3,6,9,12,15,18,21,24,27,30,33],
            "Volume_day":[4,8,12,16,20],
            "ShouYi":[0.008],
            "ZhiShun":[-0.032],
        }

        # self.fund_code_list = ["560770.SH"]
        # print("fund code数量",len(self.fund_code_list))
        # self.HyperParam_dict = {
        #     "mean_long_day":[15],
        #     "mean_short_day":[3],
        #     "Volume_day":[4],
        #     "ShouYi":[0.008],
        #     "ZhiShun":[-0.032],
        # }

        self.output_file = "/home/jianrui/workspace/Gold/CallBack/log_MeanLineAndVolume1/{fund_code}"
        self.max_len = -1

    def _request_post(self,**kwargs):
        response = requests.post("http://106.13.59.142:6010/download_history_data",json=kwargs)
        while response.status_code != 200:
            time.sleep(range.sample([i for i in range(30,69)],1)[0])
            response = requests.post("http://106.13.59.142:6010/download_history_data",json=kwargs)
            print("该参数在获取数据时，暴露问题:",kwargs)
        buffer = io.BytesIO(response.content)
        df = pd.read_pickle(buffer)
        return df

    def downLoadData(self,**kwargs):
        if "fund_code" in kwargs:
            
            kwargs["field_list"] = ["time","open","close","low","high","volume"]
            kwargs["stock_code"] = kwargs["fund_code"]
            kwargs["period"] = "1m"
            kwargs["incrementally"] = True
            kwargs.pop("fund_code")

            # df_k_5m = self._request_post(**kwargs)
            # df_k_5m = df_k_5m.to_dict(orient="index")
            # df_k_5m_divideByDay = {}
            # for k,v in df_k_5m.items():
            #     day = k[:8]
            #     df_k_5m_divideByDay.setdefault(day,[])
            #     df_k_5m_divideByDay[day].append(v)
            
            # kwargs["period"] = "1d"
            # df_k_1d = self._request_post(**kwargs)
            # df_k_1d = df_k_1d.to_dict(orient="index")

            df_k_1d = pd.read_pickle("/home/jianrui/workspace/Gold/CallBack/history_data/{}_1d.pkl".format(kwargs["stock_code"]))
            df_k_1d = df_k_1d.to_dict(orient="index")
            df_k_5m = pd.read_pickle("/home/jianrui/workspace/Gold/CallBack/history_data/{}_1m.pkl".format(kwargs["stock_code"]))
            df_k_5m = df_k_5m.to_dict(orient="index")
            df_k_5m_divideByDay = {}
            for k,v in df_k_5m.items():
                day = k[:8]
                df_k_5m_divideByDay.setdefault(day,[])
                df_k_5m_divideByDay[day].append(v)

            # 校验
            df_k_1d_key = [i for i in df_k_1d.keys()]
            df_k_5m_divideByDay_key = [i for i in df_k_5m_divideByDay.keys()]
            intersection = list(set(df_k_1d_key) & set(df_k_5m_divideByDay_key))
            try:
                assert len(intersection) == len(df_k_5m_divideByDay_key)
            except:
                print("5m数据不是1d数据的子集")
            for key in df_k_1d_key:
                if key not in intersection:
                    df_k_1d.pop(key)
            for key in df_k_5m_divideByDay_key:
                if key not in intersection:
                    df_k_5m_divideByDay.pop(key)
            df_k_1d_key = [i for i in df_k_1d.keys()]
            df_k_5m_divideByDay_key = [i for i in df_k_5m_divideByDay.keys()]
            try:
                for i,j in zip(df_k_1d_key,df_k_5m_divideByDay_key):
                    assert i==j
            except:
                print(kwargs["stock_code"],"数据没有对齐")

            return {"df_k_5m_divideByDay":df_k_5m_divideByDay,"df_k_1d":df_k_1d}
        else:
            raise

    def run_onece(self,**kwrags):
        data = kwrags["data"]
        df_k_5m_divideByDay = data["df_k_5m_divideByDay"]
        df_k_1d = data["df_k_1d"]
        kwrags.pop("data")
        mean_long_day = kwrags["mean_long_day"]
        mean_short_day = kwrags["mean_short_day"]
        Volume_day = kwrags["Volume_day"]
        ShouYi = kwrags["ShouYi"]
        ZhiShun = kwrags["ZhiShun"]
        fund_code = kwrags["fund_code"]       

        if mean_long_day <= mean_short_day+3:
            return False


        max_day = max(mean_long_day,mean_short_day,Volume_day)

        status = {"money":200000,"stock":0}
        single = 0
        log = {}

        # 接下来就是一天的计算
        df_k_1d_key = [i for i in df_k_1d.keys()]
        for index_day in range(max_day,len(df_k_1d_key)):
            # 当天数据预处理
            current_data = df_k_1d_key[index_day]
            current_k_1d = df_k_1d[current_data]
            current_k_5m = df_k_5m_divideByDay[current_data]
            current_k_5m.sort(key = lambda x: x["time"],reverse=False)
            current_k_5m_dict = {int(datetime.fromtimestamp(int(i["time"])/1000).strftime("%H%M%S")):i for i in current_k_5m}
            current_k_5m_dict_key = list(current_k_5m_dict.keys())
            current_k_5m_dict_key.sort(key = lambda x: x,reverse=False)
            
            if single == 0:
                # 计算交易量平均演变过程
                com_day = df_k_1d_key[index_day-Volume_day:index_day]
                df_k_1d_tmp = [df_k_1d[i] for i in com_day]
                df_k_5m_divideByDay_tmp = [df_k_5m_divideByDay[i] for i in com_day]
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

                # 求解当前条件下，满足金叉价格的最小值
                com_day_long = df_k_1d_key[index_day-mean_long_day:index_day]
                df_k_1d_tmp_long = [df_k_1d[i] for i in com_day_long]
                com_day_short = df_k_1d_key[index_day-mean_short_day:index_day]
                df_k_1d_tmp_short = [df_k_1d[i] for i in com_day_short]
                meanLong = sum([i["close"] for i in df_k_1d_tmp_long])/(mean_long_day+1)
                meanShort = sum([i["close"] for i in df_k_1d_tmp_short])/(mean_short_day+1)
                min_price = (mean_long_day+1)*(mean_short_day+1)*(meanLong-meanShort)/(mean_long_day-mean_short_day)

                # 获取前一天的收盘价
                yesterday_key = df_k_1d_key[index_day-1:index_day]
                yesterday = [df_k_1d[i] for i in yesterday_key][0]
                yesterday_close = yesterday["close"]

                # 开始k线遍历价格
                volume_sum = 0
                last_diff_price = False
                for index in current_k_5m_dict_key:
                    # 检验价格是否满足
                    tmp_time_k_5m = current_k_5m_dict[index]
                    price = tmp_time_k_5m["close"]
                    volume = tmp_time_k_5m["volume"]
                    volume_sum += volume
                    if min_price > 0 and \
                            min_price > yesterday_close and \
                            price > min_price and \
                            volume_sum > df_k_5m_volume_mean[index] and \
                            last_diff_price != True :
                            # and last_diff_price < 0:
                        single = 1
                        status["stock"] = status["money"]/price
                        status["money"] = 0
                        bought_price = price
                        log[current_data] = "买入==>"+json.dumps(status,ensure_ascii=False)
                        break
                    elif last_diff_price != True:
                        last_diff_price = price - min_price

            elif single == 1:
                for index in current_k_5m_dict_key:
                    mark = False
                    tmp_time_k_5m = current_k_5m_dict[index]
                    price = tmp_time_k_5m["low"]
                    while price <= tmp_time_k_5m["high"]:
                        if (price - bought_price)/bought_price > ShouYi or (price - bought_price)/bought_price < ZhiShun:
                            single = 0
                            status["money"] = status["stock"]*price
                            status["stock"] = 0
                            if (price - bought_price)/bought_price > ShouYi:
                                bought_price = None
                                log[current_data] = "盈利卖出==>"+json.dumps(status,ensure_ascii=False)
                                # print(price,log[current_data])
                            elif (price - bought_price)/bought_price < ZhiShun:
                                bought_price = None
                                log[current_data] = "止损卖出==>"+json.dumps(status,ensure_ascii=False)
                                # print(price,log[current_data])
                            mark = True
                            break
                        price = price + 0.001
                    if mark:
                        break

        
        # 输出日志
        kwrags["log"] = log

        with open(self.output_file.format(fund_code=fund_code),"a+") as f:
            f.write(json.dumps(kwrags,ensure_ascii=False)+"\n")

        return True

if __name__ == "__main__":
    # callback = CallBackV2_MeanLineAndBollingerBands()
    # callback.run()

    callback = CallBackV2_MeanLineAndVolumeV2()
    # callback.run()
    callback.precise()


        
        
        



        

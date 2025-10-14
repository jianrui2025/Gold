import pandas as pd
import itertools
import adata
import statistics
import json
import multiprocessing as mp

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
            self.run_onece(data=data,fund_code=fund_code,**one_HyperParam)

    def run(self):
        if not self.fund_code_list:
            raise "self.fund_code_list为空，请设置"
        
        # for fund_code in self.fund_code_list:
        #     data = self.downLoadData(fund_code=fund_code)
        #     with open(self.output_file.format(fund_code=fund_code),"w") as f:
        #         print("创建：",self.output_file.format(fund_code=fund_code))
        #     HyperParam_list = self.buildHyperParam(self.HyperParam_dict)
        #     for one_HyperParam in HyperParam_list:
        #         self.run_onece(data=data,fund_code=fund_code,**one_HyperParam)

        with mp.Pool(processes=28) as pool:
            pool.map(self.one_process, self.fund_code_list)

        


class CallBackV2_MeanLineAndBollingerBands(CallBackV2Base):
    # 使用 均线和柏林带 的策略
    # 当短期均线突破长期均线 → 买入；
    # 当短期均线跌破长期均线 → 卖出；
    # 但是在实际操作中，固定固定盈利卖出

    def __init__(self):
        super().__init__()
        self.fund_code_list = ["515250", "588810", "159378", "513230", "513110", "159866", "561300", "588400", "512080", "159757", "159996", "515100", "516350", "159822", "515290", "159242", "159359", "520860", "159241", "516920", "513850", "159360", "159929", "159692", "513880", "159867", "159368", "589960", "159628", "512560", "159837", "513660", "159767", "588230", "159323", "516970", "159545", "159205", "560510", "588990", "159876", "159297", "159637", "510360", "159696", "159622", "159102", "515720", "159752", "520510", "159707", "159246", "159790", "588890", "159718", "517380", "515180", "159839", "159699", "588120", "516290", "159583", "520720", "588870", "513390", "515260", "589020", "516120", "516880", "159715", "515710", "589720", "515800", "159901", "159632", "516650", "588330", "560080", "516100", "589560", "560980", "512900", "513350", "159531", "512290", "513820", "589520", "510330", "560770", "512820", "589010", "510100", "159814", "560780", "159562", "512700", "515650", "159259", "159278", "515300", "159994", "159745", "159566", "589000", "513910", "159775", "513220", "513600", "560050", "159938", "159608", "159593", "159768", "159568", "515450", "513920", "159358", "588300", "159366", "513520", "520990", "588800", "589680", "159227", "588930", "159934", "515030", "512810", "513280", "159659", "159845", "159339", "513560", "159269", "520970", "588830", "588180", "159206", "512930", "515080", "159286", "520690", "159922", "159937", "512680", "513150", "588780", "510760", "589800", "516010", "513550", "516160", "588730", "159502", "511990", "516820", "588710", "159735", "159615", "561980", "520840", "159518", "510720", "159652", "159977", "513890", "513500", "516090", "516510", "560090", "159750", "588380", "515400", "159993", "159713", "159595", "159919", "159501", "562510", "562880", "159272", "516860", "159741", "159381", "159828", "510880", "515000", "159998", "510150", "159681", "560530", "159611", "159751", "513190", "588290", "159825", "512200", "515230", "159399", "520920", "513970", "159691", "511880", "510310", "159607", "560860", "520600", "520830", "516020", "159780", "159887", "515070", "159559", "588750", "513690", "516780", "159875", "563300", "515050", "520700", "513040", "510900", "588090", "513360", "561160", "510500", "159202", "513300", "159952", "513630", "513020", "159201", "513860", "159859", "588170", "159353", "159840", "562800", "588030", "159682", "510210", "159529", "159101", "159857", "159688", "159801", "512670", "517520", "159506", "588050", "159842", "512760", "588220", "159732", "513700", "515170", "513780", "159262", "513100", "512890", "159883", "159329", "159316", "159865", "513580", "159781", "159217", "588060", "520500", "159967", "515980", "512100", "518880", "159841", "520980", "159766", "159605", "159770", "512070", "159869", "515120", "513260", "159813", "159530", "588760", "159509", "159852", "512400", "159516", "159636", "513160", "515220", "520880", "515880", "159995", "159992", "588790", "159941", "159796", "510300", "159742", "513750", "159819", "561910", "563220", "515790", "563800", "513380", "159851", "159892", "588080", "512710", "513980", "159363", "512480", "159870", "512010", "159570", "512800", "159567", "159755", "159351", "512690", "562500", "588200", "159915", "513310", "159949", "513010", "513050", "513060", "159361", "512000", "512880", "563360", "159338", "159352", "159792", "159740", "588000", "512050", "513120", "513090", "513180", "513130", "513330"]
        self.HyperParam_dict = {
            "mean_long_day":[9,12,13,18,21,24,27,30,33,36,39,42,45,48,51,54,57,60,63],
            "mean_short_day":[3,6,9,12,13,18,21,24,27,30],
            "BollingerBands_day":[20],
            "k":[2], # 布林值中的K倍标准差
            "ShouYi":[0.005,0.01,0.015,0.02,0.025,0.03,0.035,0.04],
            "ZhiShun":[-0.005,-0.01,-0.015,-0.02,-0.025,-0.03,-0.035,-0.04],
        }
        self.output_file = "/home/jianrui/Gold/CallBack/log_all/{fund_code}"
        self.max_len = -1
    
    def downLoadData(self,**kwargs):
        if "fund_code" in kwargs:
            df = adata.fund.market.get_market_etf(fund_code=kwargs["fund_code"])
            df = df.to_dict(orient="records")
            if self.max_len > 0:
                df = df[-self.max_len:]
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


if __name__ == "__main__":
    callback = CallBackV2_MeanLineAndBollingerBands()
    callback.run()


        
        
        



        

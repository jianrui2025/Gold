# # 迅投QMT测试
# from xtquant.xttrader import XtQuantTrader
# from xtquant.xttype import StockAccount
# import time
# session_id = int(time.time())
# path = "C:\\Program Files\\国金证券QMT交易端\\userdata_mini"
# xt_trader = XtQuantTrader(path, session_id)

# # 订阅账户
# xt_trader.start()
# acc = StockAccount('8886238623')
# subscribe_result = xt_trader.subscribe(acc)


# import xtquant
# from xtquant import xtdata

# xtdata.download_history_data("518880.SH",period="tick",start_time="20250512")

# a = xtdata.get_market_data_ex(stock_list=["518880.SH"],period='tick')
# print(type(a["518880.SH"]))
# print(a["518880.SH"].columns)
# print(a["518880.SH"].to_dict(orient="records")[0]["time"])
# print(type(a["518880.SH"].to_dict(orient="records")[0]["time"]))

# for k,v in a.items():
#     print(k)
# print(len(a["518880.SH"]))
# print(type(a["518880.SH"]))
# print(a["518880.SH"][0])


# import xtquant
# from xtquant import xtdata

# xtdata.download_history_data("518880.SH",period="1d",start_time="20250512")
# a = xtdata.get_market_data_ex(stock_list=["518880.SH"],period='1d')
# print(type(a["518880.SH"]))
# print(a["518880.SH"].columns)
# print(a["518880.SH"].to_dict(orient="records")[0])
# print(len(a["518880.SH"]))

# import json
# with open("../result.json3","r") as f:
#     re = [json.loads(i.strip()) for i in f]
# print(len(re))
# with open("../result.json4","r") as f:
#     re = re + [json.loads(i.strip()) for i in f]
# print(len(re))
# with open("../result.json5","r") as f:
#     re = re + [json.loads(i.strip()) for i in f]
# print(len(re))

# import json
# with open("../result.json_20250805_20250812","r") as f:
#     re = [json.loads(i.strip()) for i in f]

# re.sort(key=lambda x: x["shouyi"],reverse=True)
# re = [i for i in re if i["stock_num_lei"]==15000]
# for i in re[:100]:
#     print(i)
# max_line = re[0]
# for i in re[1:]:
#     if i["shouyi"] > max_line["shouyi"]:
#         max_line = i
# print(max_line)

# import requests
# response = requests.post("http://106.13.59.142:6010/get_fund_info_with_index_weight",json={"reload":True,"fund_code":"515120.SH"})
# print(response.json())

import requests
import tushare as ts
import io
import pandas as pd
pro = ts.pro_api('3085222731857622989')
pro._DataApi__http_url = "http://47.109.97.125:8080/tushare"
df = pro.etf_basic(ts_code="159590.SZ")
print(df)
df = pro.index_weight(index_code="H30202.CSI")
print(df)


# QMT_kwargs = {}
# QMT_kwargs["field_list"] = ["time","open","close","low","high","volume","preClose"]
# QMT_kwargs["stock_code"] = "603259.SH"
# QMT_kwargs["incrementally"] = False
# QMT_kwargs["start_time"] = "20251202"
# QMT_kwargs["end_time"] = "20251202"
# QMT_kwargs["period"] = "1m"
# response = requests.post("http://106.13.59.142:6010/download_history_data",json=QMT_kwargs)
# buffer = io.BytesIO(response.content)
# df = pd.read_pickle(buffer)
# index = set([i[:8] for i in df.index.to_list()])
# index = [int(i) for i in index]
# index.sort(key=lambda x:x)
# print(index)
# print(df)


# from tensorboard.backend.event_processing import event_accumulator
# import os

# files = [f for f in os.listdir("/home/jianrui/workspace/Gold/tensorboard_log/") if "tfevents" in f]
# print(files)
# ea = event_accumulator.EventAccumulator(
#     "/home/jianrui/workspace/Gold/tensorboard_log/events.out.tfevents.1764844238.a.50191.0"
# )
# ea.Reload()

# # 列出 scalar 标签
# print(ea.Tags()["scalars"])

# # 获取某个 scalar 的所有事件
# events = ea.Scalars("515120.SH/直接使用权重/net_mf_amount_a_year")

# for e in events:
#     print(e.step, e.value, e.wall_time)
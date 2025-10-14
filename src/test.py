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

import json
with open("../result.json_20250805_20250812","r") as f:
    re = [json.loads(i.strip()) for i in f]

re.sort(key=lambda x: x["shouyi"],reverse=True)
re = [i for i in re if i["stock_num_lei"]==15000]
for i in re[:100]:
    print(i)
max_line = re[0]
for i in re[1:]:
    if i["shouyi"] > max_line["shouyi"]:
        max_line = i
print(max_line)
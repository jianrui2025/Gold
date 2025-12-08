import sys
import sys
import pandas as pd
# from xtquant import xtdata
import akshare as ak
import adata
import tushare as ts
pro = ts.pro_api('3085222731857622989')
pro._DataApi__http_url = "http://47.109.97.125:8080/tushare"
import json
from datetime import datetime
import time

def get_all_stock(all_code_path):
    """
    获取股市全部的票号代码
    """
    df = pro.etf_basic(list_status="L")
    df.to_json(all_code_path,orient="records",lines=True,force_ascii=False)

def get_code_info(all_code_path, code_info_path):
    """
    获取所有的基金信息
    """
    with open(all_code_path,"r") as f:
        all_code_list = json.loads(f.read().strip())
    
    # 去除掉市场信息之后，查看是否有重复
    all_code_dict = {}
    for i in all_code_list:
        code,market = i.split(".")
        all_code_dict.setdefault(code,[])
        all_code_dict[code].append(i)
    print("去重前数量：",len(all_code_list))
    print("去重后数量：",len(all_code_dict))
    print("去掉的重复数据大部分是债券")

    # 读取基金信息
    all_code_info = {}
    with open(code_info_path,"r") as f:
        for i in f:
            i = json.loads(i.strip())
            all_code_info[i["基金代码"]] = i
    all_code_info_key = set([i.split(".")[0] for i in all_code_info.keys()])    

    f = open(code_info_path,"a+")

    for code in  all_code_dict:
        if len(all_code_dict[code]) != 1 or code in all_code_info_key:
            continue
        else:
            fund_overview_em_df = ak.fund_overview_em(symbol=code)
            fund_overview_em_df = fund_overview_em_df.to_dict(orient="records")[0]
            fund_overview_em_df["基金代码"] = all_code_dict[code][0]
            f.write(json.dumps(fund_overview_em_df,ensure_ascii=False)+"\n")
    
    f.close()

def get_fund(code_info_path,fund_info_path):
    """
    将基金筛选出来
    """
    with open(code_info_path,"r") as f:
        code_info = [json.loads(i.strip()) for i in f]
    fund_info = [i for i in code_info if i["基金全称"] != "---"]

    with open(fund_info_path,"w") as f:
        for i in fund_info:
            f.write(json.dumps(i, ensure_ascii=False)+"\n")

def read_ZS_info(info_path,sorce):
    """
    读取中证和国证的指数信息
    """
    if sorce == "ZZ":
        df = pd.read_excel(info_path)
        print(df.columns)
        return df.to_dict(orient="records")
    elif sorce == "GZ":
        df = pd.read_excel(info_path)
        print(df.columns)
        return df.to_dict(orient="records")
    else:
        raise

def match_fund_ZhiShu(fund,df):
    mark = False
    if fund["跟踪标的"] == "该基金无跟踪标的":
        return mark
    for i in df:
        if fund["跟踪标的"] == i["指数全称"]:
            i.setdefault("命中基金",[])
            i["命中基金"].append(fund["基金代码"])
            mark = True
            break
    return mark

def build_project(GuoZheng_info,ZhongZheng_info,fund_info_path):
    df_GuoZheng = read_ZS_info(GuoZheng_info,"GZ")
    df_ZhongZheng = read_ZS_info(ZhongZheng_info,"ZZ")
    with open(fund_info_path,"r") as f:
        fund_info = [json.loads(i.strip()) for i in f]

    for fund in fund_info:
        mark = match_fund_ZhiShu(fund,df_GuoZheng)
        if mark:
            continue
        mark = match_fund_ZhiShu(fund,df_ZhongZheng)
        if mark:
            continue
        if fund["跟踪标的"] != "该基金无跟踪标的":
            print(fund)

def build_project_fund_index_tushare(pro,fund_info_path,fund_info_with_index_path):

    with open(fund_info_with_index_path,"r") as f:
        fund_info_with_index = [json.loads(i.strip()) for i in f]
    fund_info_with_index_dict = {i["基金代码"]:i for i in fund_info_with_index}

    with open(fund_info_path,"r") as f:
        fund_info = [json.loads(i.strip()) for i in f]

    with open(fund_info_with_index_path,"a+") as f:
        for fund in fund_info:
            if fund["基金代码"] in fund_info_with_index_dict or fund["跟踪标的"] == "该基金无跟踪标的":
                continue
            df = pro.etf_basic(ts_code=fund["基金代码"]).to_dict(orient="records")
            time.sleep(0.3)
            if not df:
                continue
            df = df[0]
            index_code = df["index_code"]
            index_name = df["index_name"]
            fund["指数代码"] = index_code
            fund["指数名称"] = index_name
            f.write(json.dumps(fund,ensure_ascii=False)+"\n")

def get_index_weight_tushare(pro,fund_info_with_index_path,index_weight_path):
    with open(fund_info_with_index_path,"r") as f:
        fund_info_with_index = [json.loads(i.strip()) for i in f]
        index_code_list = [i["index_code"] for i in fund_info_with_index if i["index_code"]]
        index_code_list = list(set(index_code_list))
    with open(index_weight_path,"w") as f:
        for i in index_code_list:
            df = pro.index_weight(index_code=i)
            time.sleep(0.3)
            df = df.to_dict(orient="records")
            if not df:
                continue
            df_trade_date = [int(i["trade_date"]) for i in df]
            df_trade_date = list(set(df_trade_date))
            df_trade_date_max = str(max(df_trade_date))
            df = [i for i in df if i["trade_date"]==df_trade_date_max]
            f.write(json.dumps(df,ensure_ascii=False)+"\n")

def build_project_fund_index_weight_tuahre(all_code_path, index_weight_path, fund_info_with_index_weight_path):
    with open(index_weight_path,"r") as f:
        index_weight = [json.loads(i.strip()) for i in f]
        index_weight_dict = {i[0]["index_code"].lower():i for i in index_weight}
    with open(all_code_path,"r") as f:
        fund_info_with_index = [json.loads(i.strip()) for i in f]
        for i in fund_info_with_index:
            if i["index_code"] and i["index_code"].lower() in index_weight_dict:
                i["index_weight"] = index_weight_dict[i["index_code"].lower()]
    with open(fund_info_with_index_weight_path,"w") as f:
        for i in fund_info_with_index:
            f.write(json.dumps(i,ensure_ascii=False)+"\n")

if __name__ == "__main__":
    pro = ts.pro_api('3085222731857622989')
    pro._DataApi__http_url = "http://47.109.97.125:8080/tushare"

    start_stage = 1
    end_stage = 3

    # 获取etf全部票号代码
    all_code_path = "../conf/all_etf_code_info.json"
    if start_stage <= 1 and end_stage >= 1:
        get_all_stock(all_code_path)
        print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),":第一步完成")

    # 读取相关指数的权重
    index_weight_path = "../conf/index_weight.json"
    if start_stage <= 2 and end_stage >= 2:
        get_index_weight_tushare(pro,all_code_path,index_weight_path)
        print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),":第二步完成")


    # 将etf和指数权重进行关联
    fund_info_with_index_weight_path = "../conf/fund_info_with_index_weight.json"
    if start_stage <= 3 and end_stage >= 3:
        build_project_fund_index_weight_tuahre(all_code_path, index_weight_path, fund_info_with_index_weight_path)
        print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),":第三步完成")
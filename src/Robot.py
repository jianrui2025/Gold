import requests
import json
import logging

logging.basicConfig(format='%(asctime)s %(message)s')
log = logging.getLogger(name="log")
log.setLevel(level=logging.INFO)

class Robot():
    def __init__(self):
        self.url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=a12e0f3d-da5c-4b2b-a834-fc4ac913fe33"
    
    def transMessage_MarkDown(self,data):
        # 将消息转换为 markdown_v2格式
        title_mode = "注意： {fund_code} 处在价格 {priceType} ，单价：{price}。"
        mode_b = '<font color="warning">{}</font>'
        mode_a = '> {key}:<font color="comment">{value}</font>'

        final_dict = {}
        for k,v in data.items():
            if k in ["fund_code","priceType","price"]:
                final_dict[k] = mode_b.format(v)
            else:
                final_dict[k] = mode_a.format(key=k,value=v)
                
        final_list = []
        final_list.append(title_mode.format(fund_code=final_dict["fund_code"],priceType=final_dict["priceType"],price=final_dict["price"]))
        for k,v in final_dict.items():
            if k not in ["fund_code","priceType","price"]:
                final_list.append(final_dict[k])

        result = {
            "msgtype" : "markdown",
            "markdown": {
                "content": "\n".join(final_list)
            }
        }
        return result
    
    def transMessage_Point(self,data):
        title_mode = "定点预警： {fund_code} 价格满足 {condition}，当前单价：{price}。"
        mode_b = '<font color="warning">{}</font>'
        mode_a = '> {key}:<font color="comment">{value}</font>'

        final_dict = {}
        for k,v in data.items():
            if k in ["fund_code","condition","price","aim_price"]:
                final_dict[k] = mode_b.format(v)
            else:
                final_dict[k] = mode_a.format(key=k,value=v)

        final_list = []
        final_list.append(title_mode.format(fund_code=final_dict["fund_code"],condition=final_dict["condition"],price=final_dict["price"]))
        for k,v in final_dict.items():
            if k not in ["fund_code","condition","price","aim_price"]:
                final_list.append(final_dict[k])

        result = {
            "msgtype" : "markdown",
            "markdown": {
                "content": "\n".join(final_list)
            }
        }
        return result
    
    def transMessage_PointReBond(self,data):
        title_mode = "回落反弹预警： {fund_code} 价格满足 {condition}，当前单价：{price}。"
        mode_b = '<font color="warning">{}</font>'
        mode_a = '> {key}:<font color="comment">{value}</font>'

        final_dict = {}
        for k,v in data.items():
            if k in ["fund_code","condition","price","aim_price"]:
                final_dict[k] = mode_b.format(v)
            else:
                final_dict[k] = mode_a.format(key=k,value=v)

        final_list = []
        final_list.append(title_mode.format(fund_code=final_dict["fund_code"],condition=final_dict["condition"],price=final_dict["price"]))
        for k,v in final_dict.items():
            if k not in ["fund_code","condition","price","aim_price"]:
                final_list.append(final_dict[k])

        result = {
            "msgtype" : "markdown",
            "markdown": {
                "content": "\n".join(final_list)
            }
        }
        return result
    
    def transMessage_dataCraw(self,data):
        # 将消息转换为 markdown_v2格式
        title_mode = "注意：resquest.get错误。"
        mode_a = '> {key}:<font color="comment">{value}</font>'

        final_dict = {}
        for k,v in data.items():
            final_dict[k] = mode_a.format(key=k,value=v)

        final_list = []
        final_list.append(title_mode)
        for k,v in final_dict.items():
            final_list.append(final_dict[k])

        result = {
            "msgtype" : "markdown",
            "markdown": {
                "content": "\n".join(final_list)
            }
        }

        return result
        

    def sendMessage(self,data,func=None):
        data = func(data)
        headers_check = {
            "Content-Type": "application/json"  # 明确指定 JSON 类型
            }
        response = requests.post(
                url=self.url,
                headers=headers_check,
                # data = data
                data=json.dumps(data,ensure_ascii=False)  # 自动序列化为 JSON 并设置 Content-Type
            )
        log.info(json.dumps(data,ensure_ascii=False))


if __name__ == "__main__":
    robot = Robot()
    data = {"fund_code":"12345", "priceType":"低点", "price":"rice","fluctuation_5min":"AAA","低价占比":"AA","data":"BB"}
    robot.sendMessage(data,robot.transMessage_MarkDown)
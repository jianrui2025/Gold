from flask import Flask, request, jsonify
import random
import json
from datetime import datetime

app = Flask(__name__)

class Writer():
    def __init__(self):
        self.output_file_paht = "/home/jianrui/Gold/conf/wornning.json"
        self.output_file_paht_ReBound = "/home/jianrui/Gold/conf/wornning_ReBound.json"

    def write(self,data):
        with open(self.output_file_paht,"a") as f:
            f.write(json.dumps(data,ensure_ascii=False)+"\n")

    def writeReBound(self,data):
        with open(self.output_file_paht_ReBound,"a") as f:
            f.write(json.dumps(data,ensure_ascii=False)+"\n")

    def get(self):
        data = []
        with open(self.output_file_paht,"r") as f:
            data = [json.loads(i.strip()) for i in f]
        with open(self.output_file_paht_ReBound,"r") as f:
            data = data + [json.loads(i.strip()) for i in f]

        for i in data:
            i.pop("status")
            i.pop("id")
        return data

writer = Writer()

@app.route('/warnPrice', methods=['POST'])
def handle_data():
    header = request.headers
    if header["Authorization"] == "4132":
        if header["Content-Type"] == "application/json":
            try:
                tmp = {"condition":header["condition"],"time":int(header["time"]),"onlineday":int(header["onlineday"]),"fund_code":header["fund-code"]}
                tmp["id"] = "".join(random.sample("1234567890poiuytrewqasdfghjklmnbvcxz",10))
                tmp["status"] = "开启"
                tmp["datatime"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                body = request.get_json() 
                tmp["info"] = body["info"]
                writer.write(tmp)
                response = {
                            'status': 'success',
                            'message': '纳入报警!',
                            'received_data': {
                                    "condition":tmp["condition"],
                                    "date":tmp["datatime"],
                                    "info":tmp["info"] 
                                    }
                            }
            except:
                response = {
                            'status': 'success',
                            'message': '格式错误!',
                            'received_data': {
                                    "date": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                    }
                            }
        elif "multipart/form-data" in header["Content-Type"]:
            try:
                tmp = {"condition":header["condition"],"time":int(header["time"]),"onlineday":int(header["onlineday"]),"fund_code":header["fund-code"]}
                tmp["id"] = "".join(random.sample("1234567890poiuytrewqasdfghjklmnbvcxz",10))
                tmp["status"] = "开启"
                tmp["datatime"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                body = request.form
                body = dict(body)
                tmp["info"] = body["info"] 
                writer.write(tmp)
                response = {
                            'status': 'success',
                            'message': '纳入报警!',
                            'received_data': {
                                    "condition":tmp["condition"],
                                    "date":tmp["datatime"],
                                    "info":tmp["info"] 
                                    }
                            }
            except:
                response = {
                            'status': 'success',
                            'message': '格式错误!',
                            'received_data': {
                                    "date": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                    }
                            }
        else:
            response = {
            'status': 'success',
            'message': 'Content-Type错误！' 
        }

    else:
        response = {
            'status': 'success',
            'message': '校验失败！'
        }
    return jsonify(response)

@app.route('/warnReBoundPrice', methods=['POST'])
def warnReBoundPrice():
    header = request.headers
    if header["Authorization"] == "4132":
        if header["Content-Type"] == "application/json":
            try:
                tmp = {"condition":header["condition"],"time":int(header["time"]),"onlineday":int(header["onlineday"])}
                tmp["id"] = "".join(random.sample("1234567890poiuytrewqasdfghjklmnbvcxz",10))
                tmp["status"] = "开启"
                tmp["datatime"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                body = request.get_json() 
                tmp["info"] = body["info"]
                writer.writeReBound(tmp)
                response = {
                            'status': 'success',
                            'message': '纳入报警!',
                            'received_data': {
                                    "condition":tmp["condition"],
                                    "date":tmp["datatime"],
                                    "info":tmp["info"] 
                                    }
                            }
            except:
                response = {
                            'status': 'success',
                            'message': '格式错误!',
                            'received_data': {
                                    "date": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                    }
                            }
        elif "multipart/form-data" in header["Content-Type"]:
            try:
                tmp = {"condition":header["condition"],"time":int(header["time"]),"onlineday":int(header["onlineday"])}
                tmp["id"] = "".join(random.sample("1234567890poiuytrewqasdfghjklmnbvcxz",10))
                tmp["status"] = "开启"
                tmp["datatime"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                body = request.form
                body = dict(body)
                tmp["info"] = body["info"] 
                writer.writeReBound(tmp)
                response = {
                            'status': 'success',
                            'message': '纳入报警!',
                            'received_data': {
                                    "condition":tmp["condition"],
                                    "date":tmp["datatime"],
                                    "info":tmp["info"] 
                                    }
                            }
            except:
                response = {
                            'status': 'success',
                            'message': '格式错误!',
                            'received_data': {
                                    "date": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                    }
                            }
        else:
            response = {
            'status': 'success',
            'message': 'Content-Type错误！' 
        }

    else:
        response = {
            'status': 'success',
            'message': '校验失败！'
        }
    return jsonify(response)

@app.route('/getPrice', methods=['POST'])
def get_date():
    header = request.headers
    if header["Authorization"] == "4132":
        data = writer.get()
        response = {
                        'status': 'success',
                        'message': '获取报警!',
                        'received_data': data
                        }
    else:
        response = {
            'status': 'success',
            'message': '校验失败！'
        }
    return jsonify(response)



if __name__ == '__main__':
    app.run(host='0.0.0.0', port=6010, debug=True)

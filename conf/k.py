import json
with open("k.json","r") as f:
    re = [json.loads(i.strip()) for i in f][-5:]
    print(re)
ave_low = sum([i["low"] for i in re])/len(re)
ave_high = sum([i["high"] for i in re])/len(re)
diff = ave_high - ave_low
diff_20 = diff*0.1 + ave_low
print(diff_20)

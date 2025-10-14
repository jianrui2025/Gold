nohup python run_DataCraw_GoldETFPrice_TuShare.py >../nohup.run_DataCraw_GoldETFPrice_TuShare 2>&1 &
nohup python run_Strategy_PointPrice_GoldETFPrice.py >../nohup.run_Strategy_PointPrice_GoldETFPrice 2>&1 &
nohup python run_Strategy_LowPriceAndHighPrice_Line_Prediction.py >../nohup.run_Strategy_LowPriceAndHighPrice_Line_Prediction 2>&1 & 


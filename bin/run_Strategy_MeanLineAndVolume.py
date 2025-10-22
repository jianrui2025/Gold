import sys
sys.path.append("./src")
from Strategy import Strategy_MeanLineAndVolume

strategy = Strategy_MeanLineAndVolume()
strategy.run()

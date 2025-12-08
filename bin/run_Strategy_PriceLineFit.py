import sys
sys.path.append("./src")
from Strategy import Strategy_price_linear_fit

strategy = Strategy_price_linear_fit()
strategy.run()

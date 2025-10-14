import sys
sys.path.append("/home/jianrui/Gold/src/")
from StaticNorm import StaticGoldETFPriceNorm

data = StaticGoldETFPriceNorm()
data.run()
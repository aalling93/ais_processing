
#missing to dooo
import matplotlib.pyplot as plt
import matplotlib
from matplotlib import rc





def initi(size: int = 32):
    matplotlib.rcParams.update({"font.size": size})
    rc("font", **{"family": "sans-serif", "sans-serif": ["Helvetica"]})
    rc("font", **{"family": "serif", "serif": ["Palatino"]})
    rc("text", usetex=True)





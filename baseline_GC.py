import sys
from datetime import datetime

import pandas as pd
from granger_automated import (Granger_automated, a_test_causality)
from load_data import load_data
from statsmodels.tsa.api import VAR
from statsmodels.tsa.vector_ar.var_model import VARResults

startTime = datetime.now()
print("starting time: ", startTime)

maxlag = int(sys.argv[1])
data_file_name = sys.argv[2]
alpha = 0.05

data_ori, header = load_data(data_file_name)


def test_gc(data, index, maxlag, header, alpha):
    VARResults.test_causality = a_test_causality

    # g = Digraph('G', filename='granger_all_new.gv', strict=True)

    # edgegranger = []

    model = VAR(data)
    result = {}
    lag_dic = {}
    res_output = []
    Granger_automated(maxlag, model, lag_dic, res_output, result, header, alpha, index)
    print(result)
    print(res_output)

    output_df = pd.DataFrame(res_output)
    output_df.columns = ['Effect-Node', 'Cause-Node', 'Time-Lag', 'Strength', 'Method', 'Partition']
    output_df = output_df.sort_values(by=['Strength'])

    print(output_df.head(20))

    # print(g)
    # print(g.view())
    # g

    output_df.to_csv("gc_baseline_out.csv", header=False, index=False)
    # numpy_output = output_df.to_numpy
    # print(numpy_output)

    return res_output


test_gc(data_ori, 0, maxlag, header, alpha)
print("total time")
print(datetime.now() - startTime)

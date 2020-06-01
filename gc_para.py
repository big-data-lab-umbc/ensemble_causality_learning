import numpy as np
import pandas as pd
from statsmodels.tsa.api import VAR
from statsmodels.tsa.vector_ar.var_model import VARResults
from granger_automated import (Granger_automated, a_test_causality)

def test_gc(rdd_data, index, maxlag, header, alpha):
    VARResults.test_causality = a_test_causality

    # g = Digraph('G', filename='granger_all_new.gv', strict=True)

    # edgegranger = []
    data = np.array(list(rdd_data))
    print(data)
    # exit()
    model = VAR(data)
    result = {}
    lag_dic = {}
    res_output = []
    Granger_automated(maxlag, model, lag_dic, res_output, result, header, alpha, index)
    print(result)
    print(res_output)

    if not len(res_output) == 0:
        output_df = pd.DataFrame(res_output)
        output_df.columns = ['Effect-Node', 'Cause-Node', 'Time-Lag', 'Strength', 'Method', 'Partition']
        output_df = output_df.sort_values(by=['Strength'])

        print(output_df.head(20))

        # print(g)
        # print(g.view())
        # g

        output_df.to_csv("var_para_out{}.csv".format(index), header=False, index=False)
        # numpy_output = output_df.to_numpy
        # print(numpy_output)

    return res_output


def run_gc(maxlag, rdd, header, alpha):
    res = rdd.mapPartitionsWithIndex(lambda i, iterator: test_gc(iterator, i, maxlag, header, alpha)).collect()
    print("!!!!!!!!!!")
    print(res)

    return res

# run_gc()

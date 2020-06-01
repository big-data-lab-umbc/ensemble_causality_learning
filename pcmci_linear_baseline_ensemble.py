# Imports
import csv
from datetime import datetime
from tigramite import data_processing as pp
from tigramite.independence_tests import RCOT
from tigramite.independence_tests import ParCorr
from tigramite.pcmci import PCMCI
import numpy as np

startTime = datetime.now()
print("starting time: ", startTime)


def pcmci_causality(data, dt, index, headers, T_data, N_data, maxlag):
    T = T_data
    N = N_data
    tau_max = maxlag

    # Verbosity:
    # 0 - nothing
    # 1 - final graph only
    # 2 - everything
    verbose_max = 2
    verbose = 2
    print("======")
    # print(list(data))  # got 100 records as itertools.chain object, not numpy df

    data = np.array(list(data))
    # data = np.fromiter(data, float)
    # print(data)
    # Initialize dataframe object, specify time axis and variable names
    dataframe = pp.DataFrame(data, datatime=dt, var_names=headers)
    print(dataframe.var_names)
    parcorr = ParCorr(significance='analytic')
    pcmci = PCMCI(dataframe=dataframe, cond_ind_test=parcorr, verbosity=1)

    # correlations = pcmci.get_lagged_dependencies(tau_max=tau_max)

    pcmci.verbosity = 1
    results = pcmci.run_pcmci(tau_max=tau_max, pc_alpha=None)

    # Print results
    print("p-values")
    print(results['p_matrix'].round(3))
    print("MCI partial correlations")
    print(results['val_matrix'].round(2))

    # print("inside def pcmci_causality")

    # output edges
    result_arr = []
    # result_arr.append(["effect","cause"])

    for index_cause, item in enumerate(results['p_matrix']):
        # print("index is")
        # print(index)
        # print("item is")
        # print(item)
        # print("cause is")
        cause = headers[index_cause]
        # print(headers[index_cause])
        for index_effect, arr in enumerate(item):
            # print("effect arr is ")
            # print(arr)
            # print("effect name is")
            effect = headers[index_effect]
            # print(headers[index_effect])
            for arrItem in arr:
                if arrItem < 0.05 and cause != effect:
                    result_arr.append([effect, cause, index])
                    print("{} caused by {}".format(effect, cause))
                    break
    #
    # with open("pcmci_baseline_out.csv", "w", newline='') as f:
    #     for row in result_arr:
    #         f.write("%s\n" % ','.join(str(col) for col in row))
    # print(pcmci)
    print(result_arr)

    return result_arr

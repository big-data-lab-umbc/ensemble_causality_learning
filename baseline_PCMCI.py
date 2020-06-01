# Imports
import sys
from datetime import datetime

import numpy as np
from load_data import load_data
from tigramite import data_processing as pp
from tigramite.independence_tests import RCOT
from tigramite.pcmci import PCMCI

startTime = datetime.now()
print("starting time: ", startTime)

maxlag = int(sys.argv[1])
data_file_name = sys.argv[2]

data_ori, header = load_data(data_file_name)

dt = np.arange(len(data_ori))
t, n = data_ori.shape
print(data_ori.shape)


def pcmci_causality(data, dt, index, headers, T_data, N_data, maxlag):
    T = T_data
    N = N_data
    # Run settings
    # there is another tau_max in lagged dependencies that might be much longer!
    tau_max = maxlag

    # Verbosity:
    # 0 - nothing
    # 1 - final graph only
    # 2 - everything
    verbose_max = 2
    verbose = 2
    print("======")
    # print(list(data))  # got 100 records as itertools.chain object, not numpy df

    # Initialize dataframe object, specify time axis and variable names
    dataframe = pp.DataFrame(data, datatime=dt, var_names=headers)
    print(dataframe.var_names)
    rcot = RCOT(significance='analytic')
    pcmci_rcot = PCMCI(
        dataframe=dataframe,
        cond_ind_test=rcot,
        verbosity=0)

    pcmci_rcot.verbosity = 1
    results = pcmci_rcot.run_pcmci(tau_max=tau_max, pc_alpha=0.05)

    # Print results
    print("p-values")
    print(results['p_matrix'].round(3))
    print("MCI partial correlations")
    print(results['val_matrix'].round(2))

    # Save results to file
    # p_matrix = results['p_matrix']
    # with open("p-values_baseline.csv", "w") as csv_file:
    #     writer = csv.writer(csv_file, delimiter=",", quotechar="|", quoting=csv.QUOTE_MINIMAL)
    #     # [[[1 2 3]]] Three brackets to get through.
    #     for sector in p_matrix:
    #         print("sector: ", sector)
    #         for row in sector:
    #             print("row: ", row)
    #             writer.writerow(row)
    #         writer.writerow([])
    #
    # print("inside def pcmci_causality")

    # output edges
    result_arr = []

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

    with open("pcmci_baseline_out.csv", "w", newline='') as f:
        for row in result_arr:
            f.write("%s\n" % ','.join(str(col) for col in row))
    # print(pcmci)
    print(result_arr)

    return result_arr


pcmci_causality(data_ori, dt, 0, header, t, n, maxlag)
print("total time")
print(datetime.now() - startTime)

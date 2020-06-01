import numpy as np
from tigramite import data_processing as pp
from tigramite.independence_tests import RCOT
from tigramite.pcmci import PCMCI

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
    print("data len is ")
    print(len(data))
    # data = np.fromiter(data, float)
    # print(data)
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

    # print("inside def pcmci_causality")

    # output edges
    result_arr = []
    # result_arr.append(["effect","cause"])

    for index_cause, item in enumerate(results['p_matrix']):
        print("index is")
        print(index)
        print("item is")
        print(item)
        print("cause is")
        cause = headers[index_cause]
        print(headers[index_cause])
        for index_effect, arr in enumerate(item):
            print("effect arr is ")
            print(arr)
            print("effect name is")
            effect = headers[index_effect]
            print(headers[index_effect])
            for arrItem in arr:
                if arrItem < 0.05 and cause != effect:
                    result_arr.append([effect, cause, index])
                    print("{} caused by {}".format(effect, cause))
                    break

        with open("pcmci_para_out{}.csv".format(index), "w", newline='') as f:
            for row in result_arr:
                f.write("%s\n" % ','.join(str(col) for col in row))
    # print(pcmci)
    return result_arr


def run_pcmci(maxlag, rdd, header, dt, t, n):
    T = t
    N = n

    res = rdd.mapPartitionsWithIndex(
        lambda i, iterator: pcmci_causality(iterator, dt, i, header, T, N, maxlag)).collect()
    # res = rdd.map(mult).collect()
    print("!!!!!!!!!!")
    print(res)

    return res

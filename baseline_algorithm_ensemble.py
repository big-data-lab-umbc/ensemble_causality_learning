import csv
import sys
from datetime import datetime
import numpy as np

import dbn_baseline_ensemble
import gc_baseline_ensemble
import pcmci_baseline_ensemble
from load_data import load_data

startTime = datetime.now()
print("starting time: ", startTime)

if len(sys.argv) < 3:
    print("arguments: maxlag, data file name, number of bins of DBN")

maxlag = int(sys.argv[1])
data_file_name = sys.argv[2]
bin_num = int(sys.argv[3])

num_partitions = 1
alpha = 0.05

data_ori, header = load_data(data_file_name)

dt = np.arange(len(data_ori))
t, n = data_ori.shape
print(data_ori.shape)

res_gc = gc_baseline_ensemble.test_gc(data_ori, 0, maxlag, header, alpha)
res_pcmci = pcmci_baseline_ensemble.pcmci_causality(data_ori, dt, 0, header, t, n, maxlag)
res_dbn = dbn_baseline_ensemble.dbn(data_ori, header, 0, maxlag, bin_num)

dic_gc = {}
dic_pcmci = {}
dic_dbn = {}

en_gc = {}
en_pcmci = {}
en_dbn = {}

en_res = {}

# Granger causality post_processing
for item_gc in res_gc:
    i = 0
    # print(item_gc)
    if str(item_gc[0]) + str(item_gc[1]) not in dic_gc:
        dic_gc[str(item_gc[0]) + str(item_gc[1])] = 1
    else:
        dic_gc[str(item_gc[0]) + str(item_gc[1])] += 1
    print(dic_gc)

for dic_gc_item in dic_gc:
    if dic_gc[dic_gc_item] >= num_partitions / 2:
        print("granger causality ensemble results: effect, cause")
        print(dic_gc_item)
        print("this pair appear {} times".format(dic_gc[dic_gc_item]))
        en_gc[dic_gc_item] = 1

# PCMCI post_processing
for item_pcmci in res_pcmci:
    i = 0
    # print(item_pcmci)
    if str(item_pcmci[0]) + str(item_pcmci[1]) not in dic_pcmci:
        dic_pcmci[str(item_pcmci[0]) + str(item_pcmci[1])] = 1
    else:
        dic_pcmci[str(item_pcmci[0]) + str(item_pcmci[1])] += 1
    print(dic_pcmci)

for dic_pcmci_item in dic_pcmci:
    if dic_pcmci[dic_pcmci_item] >= num_partitions / 2:
        print("pcmci ensemble results: effect, cause")
        print(dic_pcmci_item)
        print("this pair appear {} times".format(dic_pcmci[dic_pcmci_item]))
        en_pcmci[dic_pcmci_item] = 1

# Dynamic Bayesian Network Post Processing
for item_dbn in res_dbn:
    i = 0
    # print(item_dbn)
    if str(item_dbn[0]) + str(item_dbn[1]) not in dic_dbn:
        dic_dbn[str(item_dbn[0]) + str(item_dbn[1])] = 1
    else:
        dic_dbn[str(item_dbn[0]) + str(item_dbn[1])] += 1
    print(dic_dbn)

for dic_dbn_item in dic_dbn:
    if dic_dbn[dic_dbn_item] >= num_partitions / 2:
        print("granger causality ensemble results: effect, cause")
        print(dic_dbn_item)
        print("this pair appear {} times".format(dic_dbn[dic_dbn_item]))
        en_dbn[dic_dbn_item] = 1

# put ensemble results from each method into a new dictionary for final ensemble
en_res["gc"] = en_gc
en_res["pcmci"] = en_pcmci
en_res["dbn"] = en_dbn

final_ensemble_result = {}
# for en_gc_item in en_gc:
# print(en_res)
for item in en_res:
    print(en_res[item].keys())
    for each_key in en_res[item].keys():
        print(each_key)
        if each_key not in final_ensemble_result:
            final_ensemble_result[each_key] = 1
        else:
            final_ensemble_result[each_key] += 1
print(final_ensemble_result)

# if causal relationship appear in two methods or more, its final
for final_item in final_ensemble_result:
    if final_ensemble_result[final_item] >= 2:
        print("Final Ensemble Result:")
        print(final_item)

with open('baseline_algorithm_ensemble.csv', 'w') as f:  # Just use 'w' mode in 3.x
    w = csv.DictWriter(f, final_ensemble_result.keys())
    w.writeheader()
    w.writerow(final_ensemble_result)

print("total time")
print(datetime.now() - startTime)
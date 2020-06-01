import csv
import sys
from datetime import datetime
from load_data import load_data
import dbn_para
import gc_para
import numpy as np
import pcmci_linear_para
from pyspark.sql import SparkSession

startTime = datetime.now()
print("starting time: ", startTime)

spark = SparkSession \
    .builder \
    .appName("two_phase_linear_algorithm_data") \
    .getOrCreate()

spark.sparkContext.addPyFile("sources.zip")

if len(sys.argv) < 4:
    print("arguments: maxlag, data file name, number of partitions, number of bins")

maxlag = int(sys.argv[1])
data_file_name = sys.argv[2]
num_partitions = int(sys.argv[3])
bin_num = int(sys.argv[4])

alpha = 0.05

data_ori, header = load_data(data_file_name)

dt = np.arange(len(data_ori))
t, n = data_ori.shape
print(data_ori.shape)

rdd = spark.sparkContext.parallelize(data_ori, num_partitions)
print(rdd.glom().map(len).collect())

res_gc = gc_para.run_gc(maxlag, rdd, header, alpha)
res_pcmci = pcmci_linear_para.run_pcmci(maxlag, rdd, header, dt, t, n)
res_dbn = dbn_para.run_dbn(maxlag, rdd, header, bin_num)

# print("res_gc is")
# print(res_gc)
# print("res_pcmci is")
# print(res_pcmci)
# print("res_dbn is")
# print(res_dbn)
#
# exit()

# a hash map for each algorithm to get majority voting results
# key is effect, value is cause
en_gc = {}
en_pcmci = {}
en_dbn = {}

en_res = {}

for iter_num_partition in range(0, num_partitions):
    dic_name = 'dic_partition_' + str(iter_num_partition)
    ensembled_dic_name_partition = 'en_partition_' + str(iter_num_partition)
    locals()[dic_name] = {}
    locals()[ensembled_dic_name_partition] = {}

# print(dic_partition_1)

# Granger causality post_processing
# ('x2', 'x1', -1, 0.008025050318966942, 'GC', 0)
for item_gc in res_gc:
    # print(item_gc)
    for iter_partition in range(0, num_partitions):
        # print(iter_partition)
        if item_gc[5] == iter_partition:
            exec('get_dic_name = dic_partition_{}'.format(iter_partition))
            # get_dic_name
            if str(item_gc[0]) + str(item_gc[1]) not in get_dic_name:
                get_dic_name[str(item_gc[0]) + str(item_gc[1])] = 1
            else:
                get_dic_name[str(item_gc[0]) + str(item_gc[1])] += 1

# print("partition 0 ")
# print(dic_partition_0)
# print("partition 1 ")
# print(dic_partition_1)

for item_pcmci in res_pcmci:
    # print(item_pcmci)
    for iter_partition in range(0, num_partitions):
        # print(iter_partition)
        if item_pcmci[2] == iter_partition:
            exec('get_dic_name = dic_partition_{}'.format(iter_partition))
            # get_dic_name
            if str(item_pcmci[0]) + str(item_pcmci[1]) not in get_dic_name:
                get_dic_name[str(item_pcmci[0]) + str(item_pcmci[1])] = 1
            else:
                get_dic_name[str(item_pcmci[0]) + str(item_pcmci[1])] += 1

# print("partition 0 ")
# print(dic_partition_0)
# print("partition 1 ")
# print(dic_partition_1)

for item_dbn in res_dbn:
    # print(item_dbn)
    for iter_partition in range(0, num_partitions):
        # print(iter_partition)
        if item_dbn[2] == iter_partition:
            exec('get_dic_name = dic_partition_{}'.format(iter_partition))
            # get_dic_name
            if str(item_dbn[0]) + str(item_dbn[1]) not in get_dic_name:
                get_dic_name[str(item_dbn[0]) + str(item_dbn[1])] = 1
            else:
                get_dic_name[str(item_dbn[0]) + str(item_dbn[1])] += 1

# print("partition 0 ")
# print(dic_partition_0)
# print("partition 1 ")
# print(dic_partition_1)
# print("partition 2 ")
# print(dic_partition_2)


# local ensemble
for iter_num in range(0, num_partitions):
    # exec('print(dic_partition_{})'.format(iter_num))
    exec('current_dic = dic_partition_{}'.format(iter_num))
    # print(current_dic)
    exec('ensembled_partition_dic = en_partition_{}'.format(iter_num))
    for item_en_partition in current_dic:
        if current_dic[item_en_partition] >= 2:
            print("partition{} ensemble results: effect, cause".format(iter_num))
            print(item_en_partition)
            print("this pair appear {} times".format(current_dic[item_en_partition]))
            ensembled_partition_dic[item_en_partition] = 1

# print(en_partition_0)

# global ensemble

for iter_num_partition in range(0, num_partitions):
    ensembled_dic_name_partition = 'en_partition_' + str(iter_num_partition)
    exec('en_res[ensembled_dic_name_partition] = en_partition_{}'.format(iter_num_partition))

print(en_res)

#
# # put ensemble results from each method into a new dictionary for final ensemble
# en_res["gc"] = en_gc
# en_res["pcmci"] = en_pcmci
# en_res["db"] = en_db

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
    if final_ensemble_result[final_item] >= num_partitions / 2:
        print("Final Ensemble Result:")
        print(final_item)

with open('algo_level_final_res_linear.csv', 'w') as f:  # Just use 'w' mode in 3.x
    w = csv.DictWriter(f, final_ensemble_result.keys())
    w.writeheader()
    w.writerow(final_ensemble_result)

print("total time")
print(datetime.now() - startTime)
import csv
import sys
from datetime import datetime

import dbn_para
from load_data import load_data
import numpy as np
from pyspark.sql import SparkSession

startTime = datetime.now()
print("starting time: ", startTime)

spark = SparkSession \
    .builder \
    .appName("data_level_Ensemble_DBN") \
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
# print(rdd.glom().map(len).collect())

res_dbn = dbn_para.run_dbn(maxlag, rdd, header, bin_num)


# print("res_gc is")
# print(res_gc)
for iter_num_partition in range(0, num_partitions):
    dic_name = 'dic_partition_' + str(iter_num_partition)
    #     ensembled_dic_name_partition = 'en_partition_' + str(iter_num_partition)
    locals()[dic_name] = {}
#     locals()[ensembled_dic_name_partition] = {}

for item_dbn in res_dbn:
    # print(item_gc)
    for iter_partition in range(0, num_partitions):
        # print(iter_partition)
        if item_dbn[2] == iter_partition:
            exec('get_dic_name = dic_partition_{}'.format(iter_partition))
            # get_dic_name
            if str(item_dbn[0]) + str(item_dbn[1]) not in get_dic_name:
                get_dic_name[str(item_dbn[0]) + str(item_dbn[1])] = 1
            else:
                get_dic_name[str(item_dbn[0]) + str(item_dbn[1])] += 1

print("partition 0 ")
print(dic_partition_0)
print("partition 1 ")
print(dic_partition_1)

ensemble_result = {}
ensembled_partition_dic = {}

for iter_num in range(0, num_partitions):
    # exec('print(dic_partition_{})'.format(iter_num))
    exec('current_dic = dic_partition_{}'.format(iter_num))
    print(current_dic)
    for item_en_partition in current_dic:
        # if that edge exists in partition dictionary, the value of key x1x2 is 1
        if current_dic[item_en_partition] == 1:
            print("partition{} ensemble results: effect, cause".format(iter_num))
            print(item_en_partition)
            print("this pair appear {} times".format(current_dic[item_en_partition]))
            if item_en_partition not in ensembled_partition_dic:
                ensembled_partition_dic[item_en_partition] = 1
            else:
                ensembled_partition_dic[item_en_partition] += 1

print(ensembled_partition_dic)

final_res_arr = []
for ensembled_partition_dic_iter in ensembled_partition_dic:
    if ensembled_partition_dic[ensembled_partition_dic_iter] >= num_partitions/2:
        print("data ensemble results: {}".format(ensembled_partition_dic_iter))
        final_res_arr.append(ensembled_partition_dic_iter)

with open("data_parallel_ensemble_dbn.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(final_res_arr)

print("total time")
print(datetime.now() - startTime)
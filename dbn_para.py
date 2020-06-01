import sys

import numpy as np
import pandas as pd
from pgmpy.estimators import BicScore  # import scoring functions
from dbn import convertToBins, learnStructure_start, simplifyNetwork, reduceNetwork, getCurrentNodes


def dbn_para(rdd_data, index, header, maxlag, bin_num):
    data = np.array(list(rdd_data))
    # df_list = []
    # print(data)

    # print(header)
    df = pd.DataFrame(data, columns=header)
    # print(df)

    for x_name in list(df):
        for lag in range(1, maxlag + 1):
            df['{}|{}'.format(x_name, str(lag))] = df['{}'.format(x_name)].shift(lag)

    lagData = df

    # returns a dataframe as well as the bin information for decomposition purposes

    binData = convertToBins(lagData, bin_num)
    lagData = binData[0]
    # print(lagData)

    print("*BAYESIAN INFERENCE TESTS TO DO*\n(parent ----> child)")

    edges = learnStructure_start(lagData)

    # Eliminate all edges that do not have connections with the current nodes
    sEdges = simplifyNetwork(edges, getCurrentNodes(lagData.columns))
    print("sedges are")
    print(sEdges)
    # Eliminate all presistent edges (ex msl-02|2 ----> msl-02)
    rEdges = reduceNetwork(sEdges, getCurrentNodes(lagData.columns))
    print("redges are")
    print(rEdges)

    dynamicEdges = rEdges
    print("dynamic edges are")
    print(dynamicEdges)

    # Create connections given the edges
    finalEdges = []
    finalOutput = []
    for i in range(0, len(dynamicEdges)):
        parent = dynamicEdges[i][0]
        child = dynamicEdges[i][1]
        # label = str(dynamicEdges[i][2])
        edge = (parent, child)
        res_edge = (child, parent, index)

        #   if(isvalidPlacement(edge, finalEdges)):
        finalEdges.append(edge)
        finalOutput.append(res_edge)
        # g.edge(parent, child, label=label)

    print("Final edges are")
    print(finalEdges)
    print("Final outputs ")
    print(finalOutput)
    # g.view()
    # g
    with open("dbn_para_out{}.csv".format(index), "w", newline='') as f:
        for row in finalOutput:
            f.write("%s\n" % ','.join(str(col) for col in row))

    # return data
    return finalOutput


def run_dbn(maxlag, rdd, header, bin_num):
    res = rdd.mapPartitionsWithIndex(lambda i, iterator: dbn_para(iterator, i, header, maxlag, bin_num)).collect()

    return res

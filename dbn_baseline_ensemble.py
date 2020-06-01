import sys
from datetime import datetime
from dbn import convertToBins, learnStructure_start, simplifyNetwork, reduceNetwork, getCurrentNodes

import pandas as pd

startTime = datetime.now()
print("starting time: ", startTime)


def dbn(data, header, index, maxlag, bin_num):
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
    #
    # with open("dbn_baseline_out.csv", "w", newline='') as f:
    #     for row in finalOutput:
    #         f.write("%s\n" % ','.join(str(col) for col in row))
    # g.view()
    # g

    # return data
    return finalOutput

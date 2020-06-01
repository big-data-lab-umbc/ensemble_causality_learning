import sys
from datetime import datetime

import pandas as pd
from load_data import load_data
from dbn import convertToBins, learnStructure_start, simplifyNetwork, reduceNetwork, getCurrentNodes

startTime = datetime.now()
print("starting time: ", startTime)

maxlag = int(sys.argv[1])
data_file_name = sys.argv[2]
numBins = int(sys.argv[3])


def dbn(data_file_name, index, maxlag):
    data_ori, header = load_data(data_file_name)

    data = data_ori

    # print(header)
    df = pd.DataFrame(data, columns=header)
    # print(df)

    # Update:
    # df = pd.read_csv(data_file_name, header='infer')
    for x_name in list(df):
        for lag in range(1, maxlag + 1):
            df['{}|{}'.format(x_name, str(lag))] = df['{}'.format(x_name)].shift(lag)
            # df_list.append(df['{}_{}'.format(x_name, str(lag))])

    # print(df)

    lagData = df

    # returns a dataframe as well as the bin information for decomposition purposes

    binData = convertToBins(lagData, numBins)
    lagData = binData[0]
    print(lagData.columns)

    print("*BAYESIAN INFERENCE TESTS TO DO*\n(parent ----> child)")

    edges = learnStructure_start(lagData)

    print("edges are")
    print(edges)

    # Modeling Dynamic Bayesian Network

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
    # g = Digraph('Dynamic_Network', filename='Final_Network{}'.format(index))  # name, filename
    #
    # g.attr(rankdir='LR', size='15,15')
    # g.attr('node', shape='circle')
    # g.attr(fontsize='20')

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

    with open("dbn_baseline_out.csv", "w", newline='') as f:
        for row in finalOutput:
            f.write("%s\n" % ','.join(str(col) for col in row))
    # g.view()
    # g

    # return data
    return finalOutput


dbn(data_file_name, 0, maxlag)
print("total time")
print(datetime.now() - startTime)

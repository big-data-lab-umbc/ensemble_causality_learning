import sys
from pgmpy.estimators import BicScore  # import scoring functions

def getLag(string):
    if "|" in string:
        return str(string[string.rfind('|') + 1: len(string)])
    else:
        return str(0)


def withoutLag(string):
    if "|" in string:
        return str(string[0: string.rfind('|')])


def getLocation(string):
    if '|' in string:
        return str(string[0: string.rfind('|')])
    else:
        return string


def getCurrentNodes(columns):
    nodes = []
    for n in columns:
        if '|' not in n: nodes.append(n)
    return nodes


def isvalidPlacement(edge, alledges):
    reverseEdge = (edge[1], edge[0])
    return edge not in alledges and reverseEdge not in alledges


def createBins(low, high, nbins=5, giveValue=0.1):
    bins = []
    step = 0
    # Defining the step value (subset ranges length)
    if (low < 0):
        step = abs(low) / nbins + high / nbins
    else:
        step = high / nbins
    # Loop through N bins and create the ranges
    for i in range(0, nbins):
        bins.append([low, low + step])
        low = low + step
    # give lowest and highest bin values some give to avoid NaN of float numbers
    bins[0][0] -= giveValue
    bins[len(bins) - 1][1] += giveValue
    return bins


def assignBin(bins, value):
    for i in range(0, len(bins)):
        low = bins[i][0]
        high = bins[i][1]
        if (value >= low and value <= high):
            return i


def convertToBins(dataframe, amountOfBins, columnSet=''):
    data = dataframe
    columns = list(data)
    binInfo = []
    if not columnSet:
        for i in columns:
            maximum = data[i].max()
            minimum = data[i].min()
            bins = createBins(minimum, maximum, amountOfBins)  # Creating an array of bins for column
            binInfo.append((i, bins))
            for j in range(0, len(data[i])):
                try:
                    data[i][j] = int(assignBin(bins, data[i][j]))  # assigning new bin based on value of data
                except:
                    pass
    else:
        maximum = data[columnSet].max()
        minimum = data[columnSet].min()
        bins = createBins(minimum, maximum, amountOfBins)  # Creating an array of bins for column
        binInfo.append((columnSet, bins))
        for j in range(0, len(data[columnSet])):
            try:
                data[columnSet][j] = int(
                    assignBin(bins, data[columnSet][j]))  # assigning new bin based on value of data
            except:
                pass  # Leave Nan values alone
    return data, binInfo


def learnStructure_start(lagData):
    # g.attr(rankdir='LR', size='20,15')
    # g.attr('node', shape='circle')

    edges = []

    columns = lagData.columns
    initialNodes = getCurrentNodes(columns)

    bic = BicScore(lagData)

    # Loop through all nodes
    for testVariable in columns:

        print("\n==============================================================\n")

        # Define all potential parents for the node
        setOfParents = []
        for var in columns:
            if var is not testVariable and var not in initialNodes: setOfParents.append(var)

        # store the inital score of the node without parents
        initalScore = bic.local_score(testVariable, parents=[])

        print("(INITIAL SCORE)\nChecking: %s (NO PARENTS)" % (testVariable))
        print("Initial BIC Score: %s \n" % initalScore)

        newScore = float(-sys.maxsize - 1)  # initalize best score to the lowest value possible

        bestParents = []  # store the set of best parents here

        currentBestParent = ''

        parents = setOfParents.copy()

        while (True):  # loop until the newest set of parents is less than the inital score

            # Begin looping through possible parents and scoring them (finding the bestparent and setting newScore)
            for parent in parents:

                tempBestParents = bestParents.copy()  # Create a test set of parent(s)
                tempBestParents.append(parent)

                bicScore = bic.local_score(testVariable, parents=tempBestParents)

                print("Node(s): %s ----> %s" % (tempBestParents, testVariable))
                print("BIC Score: %s\n" % bicScore)

                if (bicScore > newScore):
                    newScore = bicScore
                    print("updated new score")
                    print(newScore)
                    currentBestParent = parent

            if (newScore > initalScore):
                initalScore = newScore
                bestParents.append(currentBestParent)
                print("Best Node(s): %s ----> %s" % (bestParents, testVariable))
                print("BIC Score: %s\n" % newScore)

                parents.remove(currentBestParent)

                edge = (currentBestParent, testVariable)
                if isvalidPlacement(edge, edges):
                    edges.append(edge)
                    # g.edge(currentBestParent, testVariable)

            else:  # terminate when newScore is no longer improved from the initial score
                break
    return edges


def simplifyNetwork(edges, currentNodes):
    newEdges = []
    for edge in edges:
        if edge[1] in currentNodes:
            newEdges.append(edge)
        elif int(str(edge[0])[str(edge[0]).rfind("|") + 1:len(edge[0])]) > int(
                str(edge[1])[str(edge[1]).rfind("|") + 1:len(edge[1])]):
            newEdges.append(edge)
        else:
            continue
    return newEdges


# Eliminate all presistent edges (ex msl_02|2 ----> msl_02)
def reduceNetwork(sEdges, currentNodes):
    newEdges = []
    for edge in sEdges:
        if edge[1] in currentNodes:
            print(edge)
            # print("00000")
            edge_cause = str(edge[0])[0:str(edge[0]).rfind("|")]
            newEdges.append((edge_cause, edge[1]))
        else:
            edge_cause = str(edge[0])[0:str(edge[0]).rfind("|")]
            edge_effect = str(edge[1])[0:str(edge[1]).rfind("|")]
            print(edge_cause, edge_effect)
            # print("*****")
            newEdges.append((edge_cause, edge_effect))

    newEdges = list(dict.fromkeys(newEdges))

    return newEdges


def getSubPriors(subEdges):
    priors = []
    for edge in subEdges:
        if (withoutLag(edge[0]) not in priors):
            priors.append(withoutLag(edge[0]))
    return sorted(priors)


# divides the priors with their respective posteriors and calculates the average lag given the prior node indicies
def calculateLags(edges, currentBins):
    dynamicEdges = []

    for cbin in currentBins:

        lagSum = 0
        lagsFound = 0

        subEdges = []
        for edge in edges:

            if edge[1] == cbin:
                subEdges.append((edge[0], cbin))

        subPriors = getSubPriors(subEdges)

        for element in subPriors:
            startPrior = element
            lagSum = 0
            lagsFound = 0

            for edge in subEdges:
                if withoutLag(edge[0]) == startPrior:
                    print(edge[0], edge[1])
                    lagSum += int(getLag(edge[0]))
                    lagsFound += 1

            print("_______________________")
            lagAverage = int(lagSum / lagsFound)
            print("Lag Average: ", lagAverage)
            dynamicEdges.append((element, edge[1], lagAverage))
            print("_______________________\n")

        print("\n====================================================\n")

    return sorted(dynamicEdges)



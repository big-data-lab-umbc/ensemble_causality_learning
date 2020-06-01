import csv
import numpy as np

def load_data(data_file, delimiter=',', quotechar='|', time_column=False):
    # Load data from file, put into data
    with open(data_file, newline="") as csvfile:
        data_reader = csv.reader(csvfile, delimiter=delimiter, quotechar=quotechar)
        data = []
        for line in data_reader:
            data.append(line)

    # Strip headers
    if type(data[0][0]) in [type(" "), type(np.array([" "])[0])]:
        if time_column:
            headers = data[0][1:]
        else:
            headers = data[0]
        data = data[1:]
    else:
        headers = ["None"] * len(data[0])

    # Cast cells and whole array
    newdata = []
    for line in data:
        if time_column:
            newdata.append([float(s) for s in line[1:]])
        else:
            newdata.append([float(s) for s in line])
    data = np.array(newdata)

    if False:
        print(headers)
        for line in data:
            print(line)

    return data, headers
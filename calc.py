

def mean(values):
    return sum(values) / len(values)

def variance(values):
    mu = mean(values)
    sumVar = 0.0
    for v in values:
        sumVar += (mu - v) ** 2
    return sumVar / len(values)

def timeTable(dataset):
    start = dataset['start']
    end = dataset['end']
    step = dataset['step']
    table = []
    current = start
    for datum in dataset['values']:
        table.append((current, datum))
        current += step
    return table

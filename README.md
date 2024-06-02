![](https://github.com/VitalyRomanov/nhmr/actions/workflows/run-tests.yaml/badge.svg)

# No Hassle MapReduce

A library to make map reduce more accessible for running large jobs on a single computer. Makes sense to use when you have relatively large jobs, but not too compute intensive. For large tasks you are probably better off with PySpark.  

```bash
pip install git+https://github.com/VitalyRomanov/nhmr.git
```
 
## Wordcount example

Take text file, read lines using `TextSource`, perform the wordcount and sort by count in the descending order.

```python
from nhmr import TextSource

TextSource("path/to/text") \
    .map(lambda x: x.strip()) \
    .filter(lambda x: len(x) > 0) \
    .flat_map(lambda x: x.split()) \
    .map(lambda x: (x, 1)) \
    .reduce_by_key(lambda x, y: x + y) \
    .sort(ascending=False, key_fn=lambda x: x[1]) \
    .persist("wordcount", serialize_fn=lambda x: f"{x[0]}\t{x[1]}")  # data is consumed during persist
```

## Cache

The output of any stage can be cached to avoid recomputing the results. After cache is created, previous stages are not executed. 

```python
from nhmr import DataSource

data = DataSource(range(1000)) \
    .map(lambda x: x) \
    .cache("path/to/cache/dir") \
    .map(lambda x: x-3)

# need to consume the data after defining the computation chain
for item in data:
    pass

# now the first map stage is cached and can be reused
data = DataSource([]) \
    .cache("path/to/cache/dir") \
    .map(lambda x: x ** 5)
```

## Multiprocessing

There is an option to run tasks in a multiprocessing pool (on one machine). Keep in mind that the tasks should be intensive enough so that advantage of parallelization is not overshadowed by the task distribution overhead.  

```python
from nhmr import DataSource

# need to define a function instead of lambda so that it is instantiated 
# together with other Python processes
def task(x):
    # do some processing here
    ...
    return x

def data_source():
    # acquire data
    ...
    yield x

data = DataSource(data_source()) \
    .map(task, allow_parallel=True, n_workers=4)

for item in data:
    print(item)
```

## Branching DAG

One of the main limitations at the moment is that only sequential transforms can be built. If want to do otherwise, need to:
1. create a transform
2. cache it or write to file
3. make sure all the results are written to file
4. create text reader that would stream from the disk

This approach requires a certain program structure and is not ideal. However, given that pipelines from this package are not reusable with other packages, it does not make much sense to me to mimic their api at this stage. If need something more flexible it is better to resort to torchdata, tfx, or apache beam.    


# Wukong Divide-and-Conquer

Wukong Divide-and-Conquer (aka Wukong Dynamic) is an attempt to realize a fully-generic serverless execution engine.

Built as a conceptual extension of the original Wukong project, Wukong DnC provides a generic programming model, enabling the execution of arbitrary workloads.

To run code in this library, set your working directory to be `DivideAndConquer/`. Then, you can run the driver programs as follows:
```
python mergesort_driver.py -h
python treereduction_driver.py -h
python fibonnaci_driver.py -h
```

<!---```
<>python -m wukongdnc.mergesort_driver -h
<>python -m wukongdnc.treereduction_driver -h
<>python -m wukongdnc.fibonnaci_driver -h
<>-->

## The TCP Server

This framework requires you have a Redis server available. The IP address should be specified in `DivideAndConquer/wukongdnc/constants.py`. Likewise, the TCP server
defined in `DivideAndConquer/wukongdnc/tcp_server.py` must be running as well (and its IP address should be set in the aforementioned `constants.py` file). Finally,
an AWS Lambda function must be configured and contain all of the code except the `coordinator/` direcotry, `data/` directory, and `programs/` directory.

### Running the TCP Server

The TCP server can be started by setting your working directory to be `DivideAndConquer/` and then executing the following command: 
`python -m wukongdnc.server.tcp_server`

The TCP server that uses synchronization objects stored in AWS Lambda functions can be started similarly. Set your working directory to be `DivideAndConquer/` and execute the following command:
`python -m wukongdnc.server.tcp_server_lambda`

## Running the Dask DAG Experiments

To generate the necessary pickle file, execute the following command:
`python -m wukongdnc.dag.dask_dag`

Next, to run the local experiment itself, execute the following commands (in order):
'python -m wukongdnc.server.tcp_server'
`python -m wukongdnc.dag.DAG_executor_driver`
`python -m wukongdnc.dag.BFS`

Note: To capture all output in windows DOS box: command > logfile 2>&1
(STDIN is file descriptor #0. STDOUT is file descriptor #1. STDERR is file descriptor #2. Just as "command > file" redirects STDOUT to a file, you may also redirect arbitrary file descriptors to each other. The ">&" operator redirects between file descriptors. So, "2 >& 1" redirects all STDERR output to STDOUT.)
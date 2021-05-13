Beaver is the project to deploy SQL on Hadoop automatically onto Hadoop cluster and run different workloads by providing minimum configuration parameters.

# Prerequisites
## Git command line

You should have already installed git command line tool on the node which you use to run Beaver and commands. If not, use the following command to install git for CentOS:

```
yum install git
```

Git Error Peer reports incompatible or unsupported protocol version
If you encountered ```Peer reports incompatible or unsupported protocol version``` when using git command, your system may not have the right version of libcurl. Run the following command to upgrade libcurl to solve the issue.

```
yum update -y nss curl libcurl
```

# Getting Started #

## Download Beaver Source ##

Select one node to act as working node on which you run Beaver commands. On the working node, run git command to clone the Beaver source code to your working directory.

```
git clone https://github.com/Intel-bigdata/Beaver
```

Assume Beaver was cloned to the directory $BEAVER_HOME

## Setting up Beaver Working Environment ##

On the working node, run the following command to prepare the Beaver working environment which include installing necessary packages and set up the env variables.

```
source $BEAVER_HOME/bin/setup-env.sh
```

## Prepare the cluster ##

We need one master node to run master roles of different components such as name node. We usually use the working node as master node. We call the nodes which is used to run tasks slave nodes. We need to do necessary preparations on the master & slave nodes.

## Config Rules to Follow ##

We organized the configurations in an inheritance hierarchy to make your own configuration as minimum as possible.

* When you run your own cluster, start from an empty conf folder. By default, your empty folder will inherit all the properties in the ```$BEAVER_HOME/conf``` folder. So DON’T try to make changes in the ```$BEAVER_HOME/conf``` unless we want to make the change apply to all.

* When you have an empty folder, only add new changes for config files to the folder. Don’t copy the whole config file from ```$BEAVER_HOME/conf```. Instead, create an empty file and add only the values that you need change. The unchanged values will inherit from ```$BEAVER_HOME/conf``` folder.

* There are other conf folders in the repo which also inherit from ```$BEAVER_HOME/conf``` folder. You can inherit from the conf in repo by creating a ```.base``` file in your conf folder and put the relative path to the conf you want to inherit in the first line, for example: ```../spark```

## Create a configuration folder ##

In ```$BEAVER_HOME/repo```, create a new directory with a meaningful name which will act as your configuration root for your workload. As an example, we create a configuration folder with name ```testconf```.

```
mkdir $BEAVER_HOME/repo/testconf
```

## Prepare the node list file ##

In ```$BEAVER_HOME/repo/testconf```, create a node list file named ```nodes.conf```. You can also copy a ```nodes.conf``` file from the ```$BEAVER_HOME/conf``` folder. Add or update with your own node information, each row contains a node information as following format:

```
hostname ip username password role(master or slave)
```

In our example, we have four nodes. We have the following ```nodes.conf``` file:

```
cattle-node1 192.168.26.134 root bdpe123 master
cattle-node2 192.168.26.34 root bdpe123 slave
cattle-node3 192.168.26.135 root bdpe123 slave
cattle-node4 192.168.26.35 root bdpe123 slave
```

## Run preparation process ##

Run the following commands to prepare the cluster environment.

```
source $BEAVER_HOME/bin/setup-cluster.sh $BEAVER_HOME/repo/testconf
```

# Deploy & Run SparkSQL #

## Decide Spark & Hadoop versions ##

In the ```env.conf``` of your conf folder, you can specify Spark version and Hadoop version working together. If you are building from a branch for the Spark version, you can specify the repository and branch name in SPARK\_GIT\_REPO and SPARK\_BRANCH.
Note that the Spark version in the branch needs to match the SPARK\_VERSION parameter. Also, you can specify the compiled options for compiling Spark into a distribution. Below is example:

```
HADOOP_VERSION=2.7.3
SPARK_VERSION=3.0.0-preview2
SPARK_GIT_REPO=https://github.com/apache/spark.git
SPARK_BRANCH=v3.0.0-preview2
SPARK_COMPILED_SETTING=--tgz --name dist -Pyarn -Phadoop-2.7 -Phive -Phive-thriftserver
```

Note: please do specify a “name” parameter and specify any name for it.

## Compile ##

Now you can compile, using the following command:

```
sh scripts/spark.sh compile $BEAVER_HOME/repo/testconf
```

## Deploy ##

Or you can deploy the cluster. Deploy will compile if there is not Spark distribution for that version is available at ```$BEAVER_HOME/package/build``` folder.
Deploy the cluster will install all the components needed for running Spark including HDFS and Hive on the worker nodes and master node and start the services for use.

```
sh scripts/spark.sh deploy $BEAVER_HOME/repo/testconf
```

## Update ##

If you have made some changes in the parameter, and needs to reapply the parameters to the cluster configuration and restart the cluster, you can execute update action:

```
sh scripts/spark.sh update $BEAVER_HOME/repo/testconf
```
## Undeploy ##

If you have finished using the cluster and no need any longer, you can execute deploy to stop the cluster services and uninstall the component from the worker and master nodes.

```
sh scripts/spark.sh undeploy $BEAVER_HOME/repo/testconf
```

# Run TPC-DS #

## Generate data ##

The first step to run TPC-DS is to gen data in the cluster, if you have deployed the cluster as above, you can generate data. To specify the data scale, data format you want, in the TPC-DS folder in your conf folder, create a file named ```config``` and add the scale and format value, for example:

```
scale 1
format parquet
partitionTables true
```

config to generate 1GB scale, parquet format and partitioned table. Refer to ```$BEAVER_HOME/conf``` if you want to change other aspects. And then execute the below command to gen data.

```
sh scripts/spark.sh gen_data $BEAVER_HOME/repo/testconf
```

## Run ##

Once the data is generated, you can execute the following command to run TPCDS queries:

```
sh scripts/spark.sh run $BEAVER_HOME/repo/testconf 2
```

The third parameter above is the iteration to run.

## Deploy and Run ##

If you want to do everything in a single call, you can execute deploy_and_run command:

```
sh scripts/spark.sh deploy_and_run $BEAVER_HOME/repo/testconf 2
```

which will compile (if not exists) and deploy the cluster, generate TPC-DS data and run the TPC-DS queries.






# SparkSQL integrate with OAP and run TPC-DS #

To integrate OAP with spark, just do the same steps [Prerequisites](#Prerequisites) and [Getting Started](#Getting-Started). We have provided oap_release_performance_test folder in the ```$BEAVER_HOME/repo/oap_release_performance_test``` which contains several repos folder of different module such as oap-cache, oap-shuffle. We choose oap-cache and there are different configurations such as ```TPCDS_3TB_parquet_DCPMM_Guava_ColumnVector```,  ```TPCDS_3TB_parquet_DCPMM_VM_Binary``` and so on. Please choose one of these repos as parent repo. As an example, we choose ```$BEAVER_HOME/repo/oap_release_performance_test/output/output_workflow/oap-cache/TPCDS_3TB_parquet_DCPMM_Guava_ColumnVector``` as parent repo and create a configuration folder with name ```oaptestconf```.（Note: Run 'python $BEAVER_HOME/utils/workflow.py $BEAVER_HOME/repo/oap_release_performance_test' to generate output folder.）

```
mkdir $BEAVER_HOME/repo/oaptestconf
```

In ```$BEAVER_HOME/repo/oaptestconf``` folder, please update ```.base``` and ```nodes.conf```.

# Deploy & Run SparkSQL with OAP #

## Decide Spark & Hadoop ##

In the ```env.conf``` of your conf folder, you can specify Spark version, Hadoop version working together. If you are building from a branch for the Spark and OAP, you can specify the repository and branch name in SPARK\_GIT\_REPO, OAP\_GIT\_REPO, SPARK\_BRANCH and OAP\_BRANCH. Note that the version of Spark and OAP must be matched. Below is example:

```
HADOOP_VERSION=2.7.3
SPARK_VERSION=2.4.4
SPARK_GIT_REPO=https://github.com/apache/spark.git
SPARK_BRANCH=v2.4.4
SPARK_COMPILED_SETTING=--tgz --name dist -Pyarn -Phadoop-2.7 -Phive -Phive-thriftserver
OAP_GIT_REPO=https://github.com/Intel-bigdata/OAP.git
OAP_BRANCH=v0.6.1-spark-2.4.4
```

## Compile Spark and OAP ##

Now you can compile Spark and OAP, using the following command:

```
sh scripts/spark.sh compile $BEAVER_HOME/repo/oaptestconf oap
```

## Deploy SparkSQL with OAP ##

Or you can deploy the cluster. If there are no matched packages at ```$BEAVER_HOME/package/build``` folder for Spark and OAP, the program will compile OAP and Spark first.
Before Deploy the cluster, please update ```$BEAVER_HOME/repo/oaptestconf/spark``` folder.

### Use DCPMM for cache ###

Please update ```$BEAVER_HOME/repo/oaptestconf/spark/persistent-memory.xml```. For example:

```
<persistentMemoryPool>
  <!--The numa id-->
  <numanode id="0">
    <!--The initial path for Intel Optane DC persistent memory-->
    <initialPath>/mnt/pmem0</initialPath>
  </numanode>
  <numanode id="1">
    <initialPath>/mnt/pmem1</initialPath>
  </numanode>
</persistentMemoryPool>
```

Please update ```$BEAVER_HOME/repo/oaptestconf/spark/spark-defaults.conf``` if you choose Guava as cache strategy.  For example:

```
spark.executor.cores 45                                            # half of total cores in one node
spark.executor.instances 2                                         # 2x number of your worker nodes
spark.executor.memory 90g                                          # 2x value of spark.executor.cores
spark.driver.memory 10g
spark.executor.sql.oap.cache.persistent.memory.initial.size 463g
spark.executor.sql.oap.cache.persistent.memory.reserved.size 30g
spark.executor.extraClassPath ./oap-<oap.version>-with-spark-<spark.verison>.jar
spark.driver.extraClassPath /path/oap-<oap.version>-with-spark-<spark.verison>.jar
spark.files /path/oap-<oap.version>-with-spark-<spark.verison>.jar
```

Note：these are the minimum configurations you must added if you use DCPMM for cache and the cache strategy is Guava (Please choose parent repo in ```$BEAVER_HOME/repo/oap_release_performance_test/output/output_workflow/oap-cache``` like ```TPCDS_3TB_parquet_DCPMM_Guava_ColumnVector```).
Please update ```$BEAVER_HOME/repo/oaptestconf/spark/spark-defaults.conf``` as followed if you choose vmemcache as cache strategy:

```
spark.executor.cores 45                                            # half of total cores in one node
spark.executor.instances 2                                         # 2x number of your worker nodes
spark.executor.memory 90g                                          # 2x value of spark.executor.cores
spark.driver.memory 10g
spark.executor.sql.oap.cache.persistent.memory.initial.size 463g
spark.executor.sql.oap.cache.guardian.memory.size  20g
spark.executor.extraClassPath ./oap-<oap.version>-with-spark-<spark.verison>.jar
spark.driver.extraClassPath /path/oap-<oap.version>-with-spark-<spark.verison>.jar
spark.files /path/oap-<oap.version>-with-spark-<spark.verison>.jar
```

Note：these are the minimum configurations you must added if you use DCPMM for cache and the cache strategy is Vmemcache (Please choose parent repo in ```$BEAVER_HOME/repo/output/output_workflow/oap_release_performance_test/oap-cache/``` like ```TPCDS_3TB_parquet_DCPMM_VM_ColumnVector```).

### Use DRAM for cache ###

If the version of oap is 0.7+, please update ```$BEAVER_HOME/repo/oaptestconf/spark/spark-defaults.conf``` as followed:

```
spark.executor.cores 45                                            # half of total cores in one node
spark.executor.instances 2                                         # 2x number of your worker nodes
spark.executor.memory 90g                                          # 2x value of spark.executor.cores
spark.driver.memory 10g
spark.memory.offHeap.enabled false                                 # oap-0.7+ won’t use spark.memory.offHeap
spark.sql.oap.cache.memory.manager offheap
spark.executor.sql.oap.cache.offheap.memory.size 50g # equal to the value of memoryOverhead
spark.executor.memoryOverhead 50g                                  # According to the resource of cluster
spark.executor.extraClassPath ./oap-<oap.version>-with-spark-<spark.verison>.jar
spark.driver.extraClassPath /path/oap-<oap.version>-with-spark-<spark.verison>.jar
spark.files /path/oap-<oap.version>-with-spark-<spark.verison>.jar
```

If the version of oap before 0.7, please update ```$BEAVER_HOME/repo/oaptestconf/spark/spark-defaults.conf``` as followed:

```
spark.executor.cores 45                                            # half of total cores in one node
spark.executor.instances 2                                         # 2x number of your worker nodes
spark.executor.memory 90g                                          # 2x value of spark.executor.cores
spark.driver.memory 10g
spark.sql.oap.cache.memory.manager offheap
spark.memory.offHeap.size 50g                                      # equal to the value of memoryOverhead
spark.executor.memoryOverhead 50g                                  # According to the resource of cluster
spark.executor.extraClassPath ./oap-<oap.version>-with-spark-<spark.verison>.jar
spark.driver.extraClassPath /path/oap-<oap.version>-with-spark-<spark.verison>.jar
spark.files /path/oap-<oap.version>-with-spark-<spark.verison>.jar
```

Note: these are the minimum configurations you must added if you use DRAM for cache and the cache strategy is Guava (Please choose parent repo in ```$BEAVER_HOME/repo/output/output_workflow/oap_release_performance_test/oap-cache/```  like ```TPCDS_3TB_parquet_DRAM_ColumnVector```).

Deploy the cluster will install all the components needed for running Spark including HDFS and Hive on the worker nodes and master node and start the services for use. And meanwhile the configurations of OAP will be added to ```$SPARK_HOME/conf/spark-defaults.conf```.

```
sh scripts/spark.sh deploy $BEAVER_HOME/repo/oaptestconf oap
```

## Update SparkSQL with OAP ##

If you have made some changes in the parameter, and needs to reapply the parameters to the cluster configuration and restart the cluster, you can execute update action:

```
sh scripts/spark.sh update $BEAVER_HOME/repo/oaptestconf oap
```

## Undeploy SparkSQL with OAP ##

If you have finished using the cluster and no need any longer, you can execute deploy to stop the cluster services and uninstall the component from the worker and master nodes.

```
sh scripts/spark.sh undeploy $BEAVER_HOME/repo/oaptestconf oap
```

# Run TPC-DS with OAP #

## Generate data && Run ##

The steps of [Generate data](#Generate-data) and [Run](#Run) are the same as without OAP.

## Deploy and Run ##

If you want to do everything in a single call, you can execute deploy_and_run command:

```
sh scripts/spark.sh deploy_and_run $BEAVER_HOME/repo/oaptestconf oap 2
```

which will compile (if not exists) and deploy the cluster, generate TPC-DS data and run the TPC-DS queries.

# SparkSQL integrate with OAP and run oap-perf-test #

To integrate OAP with spark, just do the same steps [Prerequisites](#Prerequisites) and [Getting Started](#Getting-Started). We have provided oap_release_function_test folder in the ```$BEAVER_HOME/repo/oap_release_function_test``` which contains several repos folder of different module such as oap-cache, oap-shuffle. We choose oap-cache and there are several repos for different configurations such as ```DailyTest_200GB_DCPMM_Guava```,  ```DailyTest_200GB_DCPMM_VM``` and so on. Please choose one of these repos as parent repo. As an example, we choose ```$BEAVER_HOME/repo/output/output_workflow/oap_release_function_test/oap-cache/DailyTest_200GB_DCPMM_Guava``` as parent repo and create a configuration folder with name ```oaptestconf```.(Note: Run 'python $BEAVER_HOME/utils/workflow.py $BEAVER_HOME/repo/oap_release_function_test' to generate output folder.)

```
mkdir $BEAVER_HOME/repo/oaptestconf
```

In ```$BEAVER_HOME/repo/oaptestconf``` folder, please update ```.base``` and ```nodes.conf```.

# Deploy & Run SparkSQL with OAP for oap-perf-test #

## Decide Spark & Hadoop for oap-perf-test ##

Please refer to [Decide Spark & Hadoop versions](#Decide-Spark-&-Hadoop-versions). This step is just same as before.

## Compile Spark and OAP for oap-perf-test ##

Now you can compile Spark and OAP, using the following command:

```
sh scripts/daily_test.sh compile $BEAVER_HOME/repo/oaptestconf oap
```

## Deploy SparkSQL with OAP for oap-perf-test ##

Or you can deploy the cluster. If there are no matched packages at ```$BEAVER_HOME/package/build``` folder for Spark and OAP, the program will compile OAP and Spark first.
Before Deploy the cluster, please update ```$BEAVER_HOME/repo/oaptestconf/spark``` folder.

### Use DCPMM for cache for oap-perf-test ###

Please update ```$BEAVER_HOME/repo/oaptestconf/spark/persistent-memory.xml```. For example:

```
<persistentMemoryPool>
  <!--The numa id-->
  <numanode id="0">
    <!--The initial path for Intel Optane DC persistent memory-->
    <initialPath>/mnt/pmem0</initialPath>
  </numanode>
  <numanode id="1">
    <initialPath>/mnt/pmem1</initialPath>
  </numanode>
</persistentMemoryPool>
```

Please update ```$BEAVER_HOME/repo/oaptestconf/spark/spark-defaults.conf``` if you choose Guava as cache strategy.  For example:

```
spark.executor.cores 45                                            # half of total cores in one node
spark.executor.instances 2                                         # 2x number of your worker nodes
spark.executor.memory 90g                                          # 2x value of spark.executor.cores
spark.driver.memory 10g
spark.executor.sql.oap.cache.persistent.memory.initial.size 463g
spark.executor.sql.oap.cache.persistent.memory.reserved.size 30g
spark.executor.extraClassPath ./oap-<oap.version>-with-spark-<spark.verison>.jar
spark.driver.extraClassPath /path/oap-<oap.version>-with-spark-<spark.verison>.jar
spark.files /path/oap-<oap.version>-with-spark-<spark.verison>.jar
```

Note：these are the minimum configurations you must added if you use DCPMM for cache and the cache strategy is Guava (Please choose parent repo in ```$BEAVER_HOME/repo/oap_release_function_test/oap-cache``` like ```DailyTest_200GB_DCPMM_Guava```).
Please update ```$BEAVER_HOME/repo/oaptestconf/spark/spark-defaults.conf``` as followed if you choose vmemcache as cache strategy:

```
spark.executor.cores 45                                            # half of total cores in one node
spark.executor.instances 2                                         # 2x number of your worker nodes
spark.executor.memory 90g                                          # 2x value of spark.executor.cores
spark.driver.memory 10g
spark.executor.sql.oap.cache.persistent.memory.initial.size 463g
spark.executor.sql.oap.cache.guardian.memory.size  20g
spark.executor.extraClassPath ./oap-<oap.version>-with-spark-<spark.verison>.jar
spark.driver.extraClassPath /path/oap-<oap.version>-with-spark-<spark.verison>.jar
spark.files /path/oap-<oap.version>-with-spark-<spark.verison>.jar
```

Note：these are the minimum configurations you must added if you use DCPMM for cache and the cache strategy is Vmemcache (Please choose parent repo in ```$BEAVER_HOME/repo/oap_release_function_test/oap-cache``` like ```DailyTest_200GB_DCPMM_VM```).

### Use DRAM for cache ###

If the version of oap is 0.7+, please update ```$BEAVER_HOME/repo/oaptestconf/spark/spark-defaults.conf``` as followed:

```
spark.executor.cores 45                                            # half of total cores in one node
spark.executor.instances 2                                         # 2x number of your worker nodes
spark.executor.memory 90g                                          # 2x value of spark.executor.cores
spark.driver.memory 10g
spark.memory.offHeap.enabled false                                 # oap-0.7+ won’t use spark.memory.offHeap
spark.sql.oap.cache.memory.manager offheap
spark.executor.sql.oap.cache.offheap.memory.size 50g # equal to the value of memoryOverhead
spark.executor.memoryOverhead 50g                                  # According to the resource of cluster
spark.executor.extraClassPath ./oap-<oap.version>-with-spark-<spark.verison>.jar
spark.driver.extraClassPath /path/oap-<oap.version>-with-spark-<spark.verison>.jar
spark.files /path/oap-<oap.version>-with-spark-<spark.verison>.jar
```

If the version of oap before 0.7, please update ```$BEAVER_HOME/repo/oaptestconf/spark/spark-defaults.conf``` as followed:

```
spark.executor.cores 45                                            # half of total cores in one node
spark.executor.instances 2                                         # 2x number of your worker nodes
spark.executor.memory 90g                                          # 2x value of spark.executor.cores
spark.driver.memory 10g
spark.sql.oap.cache.memory.manager offheap
spark.memory.offHeap.size 50g                                      # equal to the value of memoryOverhead
spark.executor.memoryOverhead 50g                                  # According to the resource of cluster
spark.executor.extraClassPath ./oap-<oap.version>-with-spark-<spark.verison>.jar
spark.driver.extraClassPath /path/oap-<oap.version>-with-spark-<spark.verison>.jar
spark.files /path/oap-<oap.version>-with-spark-<spark.verison>.jar
```

Note: these are the minimum configurations you must added if you use DRAM for cache and the cache strategy is Guava (Please choose parent repo in ```$BEAVER_HOME/repo/oap_release_function_test/oap-cache```  like ```DailyTest_200GB_DRAM```).

Deploy the cluster will install all the components needed for running Spark including HDFS and Hive on the worker nodes and master node and start the services for use. And meanwhile the configurations of OAP will be added to ```$SPARK_HOME/conf/spark-defaults.conf```.

```
sh scripts/daily_test.sh deploy $BEAVER_HOME/repo/oaptestconf oap
```


## Update SparkSQL with OAP for oap-perf-test ##

If you have made some changes in the parameters, and needs to reapply the parameters to the cluster configuration and restart the cluster, you can execute update action:

```
sh scripts/daily_test.sh update $BEAVER_HOME/repo/oaptestconf oap
```

## Undeploy SparkSQL with OAP for oap-perf-test ##

If you have finished using the cluster and no need any longer, you can execute deploy to stop the cluster services and uninstall the component from the worker and master nodes.

```
sh scripts/daily_test.sh undeploy $BEAVER_HOME/repo/oaptestconf oap
```

# Run oap-perf-test with SparkSQL and OAP #

## Generate data for oap-perf-test ##

The first step to run oap-perf-test is to gen data in the cluster, if you have deployed the cluster as above, you can generate data. To update the data scale, data partition and data location you want, in the oap-perf folder of your conf folder, create a file named ```oap-benchmark-default.conf``` and add the parameters, for example:

```
oap.benchmark.hdfs.file.root.dir     /dailytest
oap.benchmark.tpcds.data.scale       1
```

config to generate 1GB scale. Refer to ```$BEAVER_HOME/conf``` if you want to change other aspects. And then execute the below command to gen data.

```
sh scripts/daily_test.sh gen_data $BEAVER_HOME/repo/testconf oap
```

## Run oap-perf-test ##

Once the data is generated, you can execute the following command to run oap-perf-test:

```
sh scripts/daily_test.sh run $BEAVER_HOME/repo/testconf oap
```

## Deploy and Run oap-perf-test ##

If you want to do everything in a single call, you can execute deploy_and_run command:

```
sh scripts/daily_test.sh deploy_and_run $BEAVER_HOME/repo/oaptestconf oap
```

which will compile (if not exists) and deploy the cluster, generate data and run the oap-perf-test.

# OAP release workflow #

## Prerequisite  ##

The step of generating data has been completed(Such as 1TB, 3TB parquet or orc).

## Prepare repo  ##

There are two repos in ```$BEAVER_HOME/repo``` named ```oap_release_function_test``` and ```oap_release_performance_test``` which provide default configuration for different cases. Please create a repo  with the same structure and update the values you need.

For example: we create the repo named as ```OAP_0.8_function_test``` and update ```$BEAVER_HOME/repo/OAP_0.8_function_test/oap-cache/.base``` to inherit ```$BEAVER_HOME/repo/oap_release_function_test/oap-cache```

## Trick release test ##

### OAP release function test ###

Update the scripts ```$BEAVER_HOME/scripts/oap_release_function_test.sh```, and exec:

```sh $BEAVER_HOME/scripts/oap_release_function_test.sh $BEAVER_HOME/repo/OAP_0.8_function_test;```

### OAP release performance test ###

Update the scripts ```$BEAVER_HOME/scripts/oap_release_performance_test.sh```, and exec:

```sh $BEAVER_HOME/scripts/oap_release_performance_test.sh $BEAVER_HOME/repo/OAP_0.8_performance_test;```



# Deploy & Run Beaver workflow

In general, a workflow folder is a combination of different configuration folders, you can use it to test a series of OAP features. To run a Beaver workflow, you should create a new directory with a meaningful name which will act as your configuration root for your workflow in $BEAVER_HOME/repo/workflows. As an example, we create a configuration folder with named test_workflow, this directory structure of this folder is as follows.
```
test_workflow
├── common
│   ├── env.conf
│   ├── hadoop
│   │   ├── hdfs-site.xml
│   │   └── yarn-site.xml
│   ├── nodes.conf
│   └──.base
├── components
│   ├── oap-dataGen
│   │   ├── oap_cache_200GB
│   │   │   ├── env.conf
│   │   │   └── oap-perf
│   │   │       └── oap-benchmark-default.conf
│   │   ├── oap_shuffle_TPCDS_parquet_0.5TB
│   │   │   ├── env.conf
│   │   │   └── TPC-DS
│   │   │       └── config
│   │   └── oap_spark_kmeans_200GB
│   │       ├── env.conf
│   │       └── hibench
│   │           ├── hibench.conf
│   │           └── kmeans.conf
│   ├── oap-shuffle
│   │   ├── remote-shuffle
│   │   │   ├── TPCDS_0.5TB_parquet_AE_and_INDEX_REMOTE_SHUFFLE
│   │   │   │   ├── env.conf
│   │   │   │   ├── spark
│   │   │   │   │   └── spark-defaults.conf
│   │   │   │   └── TPC-DS
│   │   │   │       └── config
│   │   └── RPMem-shuffle
│   │       └── TPCDS_0.5TB_parquet_RPMEM_SHUFFLE
│   │           ├── env.conf
│   │           ├── spark
│   │           │   └── spark-defaults.conf
│   │           └── TPC-DS
│   │               └── config
│   └── oap-spark
│       └── KMEANS_200GB_DCPMM_RDD_Cache
│           ├── env.conf
│           ├── hibench
│           │   ├── hibench.conf
│           │   ├── kmeans.conf
│           │   └── spark.conf
│           └── spark
│               └── spark-env.sh
└── .base
```

### Some concepts

#### 1. **common** and **components**
 All configuration folders in **components**  will inherit from **common**, only the configuration folder under **components** will run. You can define these configuration folders exactly as described above. 

***Notice:*** There is no need to add a .base file to configuration folders under **components**.

 

#### 2. About .base 
In the root directory of the workflow folder, you can create a file named **.base**, its concept is basically the same as mentioned above in the configuration folder. 
- It will make you workflow inherit from other workflows in repo. Put the relative path to the workflow you want to inherit in the first line, for example: ../oap_release_function_test. 
- The inherited workflow will contain many configuration folders, we only need a small part of it. So you can define the configurations which you want to run as follows
  ``` 
  ../oap_release_function_test
  REMOVE:all
  ADD:oap-dataGen/oap_cache_200GB, oap-dataGen/oap_shuffle_TPCDS_parquet_0.5TB,oap-dataGen/oap_spark_kmeans_200GB,oap-shuffle/remote-shuffle/TPCDS_0.5TB_parquet_AE_and_INDEX_REMOTE_SHUFFLE
  ```


#### 3. About **components/oap-dataGen**
We can see that there is a folder named **oap-dataGen** under the **components** directory, it contains some configuration folders which defined to generate test data. All **env.conf** under the floder **oap-dataGen**  will add the same configuration:
```
GENERATE_DATA=TRUE
```
This configuration make Beaver to only use it to generate data without running test.


#### Some configurations in **common/env.conf** 
```
# the email address which receive the results
OAP_EMAIL_RECEIVER=***.**@intel.com 
# a flag for compare feature result with baseline result
BASELINE_COMP=TRUE
# the version of OAP conda package which you want to deploy
CONDA_OAP_VERSION=1.0.0
```


## Trigger a Beaver workflow

You can use the blow command to trigger your Beaver workflow.
```
python $BEAVER_HOME/bin/run_workflows.py --plugins  oap  --workflows repo/workflows/test_workflow1,repo/workflows/test_workflow2/
```
- ***--plugins***  This parameter is used for OAP, it will decide how Beaver deploy OAP. The option **oap** means Beaver will build OAP from source code, **conda_oap** means Beaver will deploy OAP from Conda.
- ***--workflows***  This parameter can receive multiple workflows, split by ",". Beaver will run workflows one by one.

If you specify that  ***plugins*** is **conda_oap**, please ensure that you have added a configuration **CONDA_OAP_VERSION** in the **env.conf** under the floder **common**.

After execute the command, Beaver will automatically deploys the environment and generate test data which you defined in the folder **components/oap-dataGen**. Don't worry about time-consuming  of data generation, Beaver can 

## Run Gold-deck workflow
Gold-deck workflow is not much different from normal workflow, there are two things to note.

1. Add a configurations in **common/env.conf**
```
BASELINE_COMP=TRUE
```
2. Add baseline configuration folders  
For every feature configuration folder you want to test, you must create a corresponding baseline configuration folder. Note that there are strict naming rules for a baseline configuration folder. It should be named as the name of feature configuration folder plus suffix **"_Baseline"**, you can refer to the following example.
```
│   ├── oap-shuffle
│   │   └── remote-shuffle
│   │       ├── TPCDS_0.5TB_parquet_AE_and_INDEX_REMOTE_SHUFFLE
│   │       │   ├── env.conf
│   │       │   ├── spark
│   │       │   │   └── spark-defaults.conf
│   │       │   └── TPC-DS
│   │       │       └── config
│   │       └── TPCDS_0.5TB_parquet_AE_and_INDEX_REMOTE_SHUFFLE_Baseline
│   │           ├── env.conf
│   │           ├── spark
│   │           │   └── spark-defaults.conf
│   │           └── TPC-DS
│   │               └── config
```

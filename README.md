# Spark Project submission

## Introduction

This repository is of Spark Project submissions, which were requirements of EPFL's [Spark course](https://www.coursera.org/learn/scala-spark-big-data/home/welcome) on coursera.

The individual submissions have been packaged into a multi-module Scala project, and deployed and run on an [AWS EMR](https://aws.amazon.com/emr/) cluster, which were running Spark, Hive and Zepellin.

The `spark-submit` and `sbt` instructions below will work on a Spark YARN cluster as well as Spark Local.

## Week2 - StackOverflow forum posts analysis

#### Context
The context is analyzing a csv dump of StackOverflow posts and look at the ratings of user-provided answers (based on user votes).  
The motivation is to answer questions such as -
* Which programming languages on StackOverflow are commonly referenced by the community?
* Which programming languages typically have the highest and lowest rated answers to questions?  

The answers to the above questions indicate respectively -
* The demand for those programming languages in the industry
* The quality of documentation / community support for those programming languages

#### Data
The data source is this [dataset](http://alaska.epfl.ch/~dockermoocs/bigdata/stackoverflow.csv).

#### Implementation details
The project implements a distributed k-mean clustering computation to determine the highest rated answers for different programming languages.

The intention is to determine which programming languages have more supportive communities of users and documentation. 

#### Build Instructions

Use the following commands to build the Assembly/Fat/Uber Jar.
```
git clone https://github.com/kevvo83/scala-spark-ln.git
sbt clean compile week2/assembly
```

#### Execution Instructions

The Assembly Jar should be downloaded to local location (I've used the HDFS location `/home/ec2-user/` on EMR).

To submit directly on the Master server, SSH to the Master server and run this command (tested on AWS EMR) -

```
sudo spark-submit --class stackoverflow.StackOverflow \
--deploy-mode cluster --master yarn \
--num-executors 3 --conf spark.executor.cores=3 \
--conf spark.executor.memory=8g --conf spark.driver.memory=1g \
--conf spark.driver.cores=1 --conf spark.logConf=true \
--conf spark.yarn.appMasterEnv.SPARKMASTER=yarn \
--conf spark.yarn.appMasterEnv.WAREHOUSEDIR=s3a://***S3 OUTPUT BUCKET***/spark-warehouse \
--conf spark.yarn.jars=/usr/lib/spark/jars/*.jar \
--conf spark.yarn.preserve.staging.files=true \
--conf spark.executorEnv.SPARK_HOME=/usr/lib/spark/ \
--conf spark.network.timeout=600000 \
--conf spark.default.parallelism=20 \
--files /usr/lib/spark/conf/spark-defaults.conf \
--jars /home/ec2-user/week2-assembly-0.2.0-SNAPSHOT.jar \
/home/ec2-user/week2-assembly-0.2.0-SNAPSHOT.jar \
/user/spark/stackoverflow.csv
```

#### Results, Reporting and Interpretation

The clustering results are plotted in the scatterplot below. The Zepellin notebook used to generate this plot is here and can be imported if needed.

For clusters that have a large number of Questions, the point's size will be larger.  
For clusters that have a high Score, the point's size will have a darker colour.

 

## TimeUsage analysis

The [dataset](http://alaska.epfl.ch/~dockermoocs/bigdata/atussum.csv) used is the Activity Summary file 
of the [American Time Use Survey](https://www.kaggle.com/bls/american-time-use-survey) on Kaggle.

### Dashboard

The TimeUsage Zeppelin Dashboard is checked in [here](notebooks/TimeUsage.json). It can be imported and viewed in your own Zepellin instance.

The Dashboard does a comparison of time spending characteristics between -

1. Employed Men vs. Women of working age
2. Employed Men vs. Unemployed Men of working age

![TimeUsage Dashboard](images/TimeUsageDashboard.png).


### Build & Assemble Instructions

`git clone https://github.com/kevvo83/scala-spark-ln.git`

`sbt clean update week3/compile week3/assembly`

(Note: You may need to download the [SBT utility](https://www.scala-sbt.org/download.html))

### Spark Job Submit Instructions

The Assembly Jar should be downloaded to local location (I've used `/home/ec2-user/` on EMR).

The Access Key and Secret Key are the IAM Credentials of an IAM User that can access the S3 Bucket where the datafile is stored.

To submit directly on the Master server, SSH to the Master server and run this command (tested on AWS EMR) -
```
sudo spark-submit --class timeusage.TimeUsage \
--deploy-mode cluster --master yarn \
--num-executors 2 --conf spark.executor.cores=2 \
--conf spark.executor.memory=2g --conf spark.driver.memory=1g \
--conf spark.driver.cores=1 --conf spark.logConf=true \
--conf spark.yarn.appMasterEnv.SPARKMASTER=yarn \
--conf spark.yarn.appMasterEnv.WAREHOUSEDIR=s3a://***S3OUTPUTBUCKET***/spark-warehouse \
--conf spark.yarn.appMasterEnv.S3AACCESSKEY=***S3ACCESSKEY*** \
--conf spark.yarn.appMasterEnv.S3ASECRETKEY=***S3SECRETKEY*** \
--conf yarn.log-aggregation-enable=true \
--conf spark.yarn.jars=/usr/lib/spark/jars/*.jar \
--conf spark.yarn.preserve.staging.files=true \
--conf spark.executorEnv.SPARK_HOME=/usr/lib/spark/ \
--conf yarn.nodemanager.delete.debug-delay-sec=36000 \
--conf spark.network.timeout=600000 \
--files /usr/lib/spark/conf/spark-defaults.conf \
--jars /home/ec2-user/week3-assembly-0.1.0-SNAPSHOT.jar \
/home/ec2-user/week3-assembly-0.1.0-SNAPSHOT.jar \
s3a://***S3BUCKET***/atussum.csv
```

## Present in this Repository

RDDs, DataSets, DataFrames.

KMeans Clustering.

Unit Tests using FunSuite.

Compare efficienty of Operations such as groupByKey vs. reduceByKey.

DataSets Typed Operations.

DataFrames Untyped Operations.

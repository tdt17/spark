# Difference with upstream
* [SPARK-15777](https://issues.apache.org/jira/browse/SPARK-15777) (Partial fix) - Catalog federation
  * make ExternalCatalog configurable beyond in memory and hive
  * FileIndex for catalog tables is provided by external catalog instead of using default impl
* [SPARK-33089](https://issues.apache.org/jira/browse/SPARK-33088) Enhance ExecutorPlugin API to include callbacks on task start and end events
  * Merged upstream, remove when we migrate to 3.1  
* [SPARK-18079](https://issues.apache.org/jira/browse/SPARK-18079) - CollectLimitExec.executeToIterator should perform per-partition limits
* [SPARK-20952](https://issues.apache.org/jira/browse/SPARK-20952) - ParquetFileFormat should forward TaskContext to its forkjoinpool
* [SPARK-26626](https://issues.apache.org/jira/browse/SPARK-26626) - Limited the maximum size of repeatedly substituted aliases
* [SPARK-25200](https://issues.apache.org/jira/browse/SPARK-25200) - Allow setting HADOOP_CONF_DIR as a spark config





# Added
* [palantir/spark#381](https://github.com/palantir/spark/pull/381) Gradle plugin to easily create custom docker images for use with k8s


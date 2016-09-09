# Top Ten Task

```
javac -classpath $HADOOP_HOME/share/hadoop/common/hadoop-common-2.7.2.jar:$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.7.2.jar:$HADOOP_HOME/share/hadoop/common/lib/commons-cli-1.2.jar -d topten_classes sics/TopTen.java
jar -cvf topten.jar -C topten_classes/ .
$HADOOP_HOME/bin/hdfs dfs -rm -r output
$HADOOP_HOME/bin/hadoop jar topten.jar sics.TopTen input output
```

```
$HADOOP_HOME/bin/hdfs dfs -ls output
$HADOOP_HOME/bin/hdfs dfs -cat output/part-r-00000
```

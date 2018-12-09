# Clickstream-Hadoop

Predicting Website Navigation with MapReduce in Hadoop

## Statistics Generated

* users browsing behavior - top 10 bread crumb trails that most users follow
(Ryan)
* most popular - top 10 most visited wikipedia pages (Qizhou)
* external, link, and other - what wikipedia page is most often linked to
externally, what page is most often linked to from within Wikipedia, and what
page is most often linked to from other (referrer missing) (Parth)


## NextClick

### Description

This MapReduce module creates a TSV file with the most likely next click for
any link on Wikipedia. This file can be used in other scripts to predict user
browsing behavior.

### Compiling / Running
 
Make sure your required files are in HDFS and the output dir is empty
NextClick writes to output/next_click by default and takes the input TSV file
path as the first argument.
```bash
hadoop fs -mkdir -p src/main/resources/
hadoop fs -copyFromLocal src/main/resources/
hadoop fs -rm -r output/next_click
hadoop jar target/click-stream-0.0.1.jar hadoop.NextClick src/main/resources/clickstream-enwiki-2018-10-abridged.tsv
mvn clean install
```

### Script Analysis (WIP)

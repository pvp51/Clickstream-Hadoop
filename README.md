# Clickstream-Hadoop

Predicting Website Navigation with MapReduce in Hadoop

Presentation can be found
[here](https://docs.google.com/presentation/d/1XSUvSYeoF0-qAdLf9mmImw0EtSuDcG2uYQrUlsExKPQ/edit?usp=sharing).

## Dataset

This project uses the English
[Wikipedia clickstream](https://meta.wikimedia.org/wiki/Research:Wikipedia_clickstream)
which can be found [here](https://dumps.wikimedia.org/other/clickstream).

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
hadoop fs -mkdir -p src/main/resources
hadoop fs -copyFromLocal src/main/resources
hadoop fs -rm -r output/next_click
mvn clean install
hadoop jar target/click-stream-0.0.1.jar hadoop.NextClick src/main/resources/clickstream-enwiki-2018-10-abridged.tsv
```

## TypeCount

### Description

This MapReduce module determines what the most popular pages are based on referrer type: external, link within Wikipedia, or other (unknown).

### Compiling / Running
 
Make sure your required files are in HDFS and the output dir is empty
TypeCount writes to output/type_count by default and takes the input TSV file
path as the first argument.
```bash
hadoop fs -mkdir -p src/main/resources
hadoop fs -copyFromLocal src/main/resources
hadoop fs -rm -r output/type_count
mvn clean install
hadoop jar target/click-stream-0.0.1.jar hadoop.TypeCount src/main/resources/clickstream-enwiki-2018-10-abridged.tsv
```

### Script Analysis

A python script will generate breadcrumb trails for all Wikipedia pages in
compressed TSV format. It will also create a histogram of breadcrumb trail
length. It reads results/next_click.tsv.gz (copy from output/next_click/part*)
and writes to results/breadcrumbs_histogram.png and results/breacrumbs.tsv.gz.
Numpy and matplotlib are requried for the analysis.
```bash
python3 scripts/breadcrumbs.py
```

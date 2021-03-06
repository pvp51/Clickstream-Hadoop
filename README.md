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
hadoop jar target/click-stream-0.0.1.jar hadoop.NextClick src/main/resources/clickstream-enwiki-2018-10.tsv.gz
```

## TypeCount

### Description

This MapReduce module determines how many pages are landed on based on referrer type: external, link within Wikipedia, or other (unknown).

### Compiling / Running
 
Make sure your required files are in HDFS and the output dir is empty
TypeCount writes to output/type_count by default and takes the input TSV file
path as the first argument.
```bash
hadoop fs -mkdir -p src/main/resources
hadoop fs -copyFromLocal src/main/resources
hadoop fs -rm -r output/type_count
mvn clean install
hadoop jar target/click-stream-0.0.1.jar hadoop.TypeCount src/main/resources/clickstream-enwiki-2018-10.tsv.gz
```

## TopTen

### Description

This MapReduce module computes the top ten most popular links for each of the
three types: link (within Wikipedia), other (unknown referrer), and external
(outside Wikipedia). It runs in two parts, the first part summing all of the
referred to links aggregated by type and name and the second part using that
data to produce the top ten lists.

### Compiling / Running
 
Make sure your required files are in HDFS and the output dir is empty
TopTen writes to output/top_ten_part1 and output/top_ten_part2 by default and
takes the input TSV file path as the first argument.
```bash
hadoop fs -mkdir -p src/main/resources
hadoop fs -copyFromLocal src/main/resources
hadoop fs -rm -r output/top_ten*
mvn clean install
hadoop jar target/click-stream-0.0.1.jar hadoop.TopTen src/main/resources/clickstream-enwiki-2018-10.tsv.gz
```

# Clickstream-Hadoop

Predicting Website Navigation with MapReduce in Hadoop

## Statistics Generated

* users browsing behavior - top 10 bread crumb trails that most users follow
(Ryan)
* most popular - top 10 most visited wikipedia pages (Qizhou)
* external, link, and other - what wikipedia page is most often linked to
externally, what page is most often linked to from within Wikipedia, and what
page is most often linked to from other (referrer missing) (Parth)

## Dev Notes

* Please copy dataset under src/main/resources (Ryan - just saw this now, moved and
done)
* If you can figure out, for some reason the code is maven building at JRE 1.5
instead 1.8. Not a blocker, but good if we can resolve it. (Ryan - changed the
pox file for newer version)

## NextClick

### Description

This MapReduce module creates a TSV file with the most likely next click for
any link on Wikipedia. This file can be used in other scripts to predict user
browsing behavior.

### Compiling / Running
 
Make sure your the required files are in HDFS and the output dir is empty
NextClick writes to output/next_click by default and takes the input TSV file
path as the first argument.
```bash
hadoop fs -mkdir -p src/main/resources/
hadoop fs -copyFromLocal src/main/resources/
hadoop fs -rm -r output/next_click
mvn clean install
hadoop jar target/click-stream-0.0.1.jar hadoop.NextClick src/main/resources/clickstream-enwiki-2018-10-abridged.tsv
```

### Script Analysis (WIP)

# Clickstream-Hadoop

Predicting Website Navigation with MapReduce in Hadoop

## Statistics Generated

* users browsing behavior - top 10 bread crumb trails that most users follow (Ryan)
* most popular - top 10 most visited wikipedia pages (Qizhou)
* external, link, and other - what wikipedia page is most often linked to externally, what page is most often linked to from within Wikipedia, and what page is most often linked to from other (referrer missing) (Parth)

## Dev Notes

* Please copy dataset under src/main/resources (Ryan - just saw this now, moved and done)
* If you can figure out, for some reason the code is maven building at JRE 1.5 instead 1.8. Not a blocker, but good if we can resolve it. (Ryan - changed the pox file for newer version)
* Make sure your the required files are in HDFS. BreadCrumbs writes to output/ by default.

#!/usr/bin/python3
"""
Outputs a file with the most common breadcrumb trails for all wikipedia pages
Output TSV Format: length_of_trail page trail
Outputs a histogram of breadcrumb trail length in PNG format
"""
import csv, gzip
import numpy as np
import matplotlib.mlab as mlab
import matplotlib.pyplot as plt

next_click = {}
lengths = []

def follow(link, breadcrumbs):
    if link in breadcrumbs or link not in next_click:
        return breadcrumbs;
    else:
        breadcrumbs.append(link);
        return follow(next_click[link], breadcrumbs);

with gzip.open('results/next_click.tsv.gz', mode='rt', newline='') as csvfile:
    reader = csv.reader(csvfile, delimiter='\t')
    for row in reader:
        next_click[row[0]] = row[1]

with gzip.open('results/breadcrumbs.tsv.gz', mode='wt', newline='') as csvfile:
    writer = csv.writer(csvfile, delimiter='\t')
    for page in next_click:
        breadcrumbs = follow(page, [])
        length = len(breadcrumbs)
        lengths.append(length)
        trail = "->".join(breadcrumbs)
        writer.writerow([length, page, trail])

plt.hist(lengths, bins=range(300), alpha=0.75)
plt.xlabel('Length of Breadcrumb Trail')
plt.ylabel('Frequency')
plt.title('Histogram of Wikipedia Breadcrumbs')
plt.axis([0, 15, 0, 4e5])
plt.tight_layout()
plt.savefig('results/breadcrumbs_histogram.png')

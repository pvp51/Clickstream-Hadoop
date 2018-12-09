#!/usr/bin/python3

import csv

next_click = {}

def follow(link, breadcrumbs):
    if link in breadcrumbs or link not in next_click:
        return breadcrumbs;
    else:
        breadcrumbs.append(link);
        return follow(next_click[link], breadcrumbs);

with open('results/next_click.tsv', newline='') as csvfile:
    reader = csv.reader(csvfile, delimiter='\t')
    for row in reader:
        next_click[row[0]] = row[1]

for link in next_click:
    breadcrumbs = follow(link, [])
    path = "->".join(breadcrumbs)
    print("{}: {}".format(len(breadcrumbs), path))

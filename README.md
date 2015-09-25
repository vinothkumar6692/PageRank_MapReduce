# MapReduce Implementation of PageRank

A simple Mapreduce implementation of the PageRank algorithm using Java.

Overview
=======

[PageRank](https://en.wikipedia.org/wiki/PageRank) is an algorithm used by Google Search to rank websites in their search engine results. PageRank was named after Larry Page,[1] one of the founders of Google. PageRank is a way of measuring the importance of website pages. According to Google:

PageRank works by counting the number and quality of links to a page to determine a rough estimate of how important the website is. The underlying assumption is that more important websites are likely to receive more links from other websites.

Consider the following input file:

```bash
A C F 0.166667
B D E F 0.166667
C A B 0.166667
D A B C E F 0.166667
E F 0.166667
F B C 0.166667
```

The first line, for example, is interpreted as follows:

```bash
“A” means "Page A“.
"C F" means "Page A" has outlinks to "Page C" and "Page F“.
"0.166667" is the initial PR value of Page A. 
```

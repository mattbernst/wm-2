# Usage

## Extracting data from English Wikipedia

### Software requirements

This software has only been tested under Linux and macOS, though there is no conscious inclusion of platform-specific
code.

You will need a Java runtime environment and [sbt](https://www.scala-sbt.org/1.x/docs/Setup.html), the Scala build tool.
The code has been tested on JREs 8 through 21.

The Makefile is written for GNU Make.

### Input data

Download [the latest pages and articles](https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2)
for English Wikipedia. As of 2024-09-03 the file is about 21 GB compressed, 97 GB decompressed.

It is possible to run the extraction process directly from the downloaded .bz2 file but this will be slow. The
code will spend of its time decompressing the data and the worker threads will be If you are going to run the extraction
process more than once, decompress the file with the command line utility `bunzip2` or a similar tool.

### Running extraction
Run the Makefile `extract` target with `dumpfile` (pointing to your input data) set appropriately, like so:

```
make extract dumpfile=/Users/mernst/git/wm-data/enwiki-latest-pages-articles.xml
```

The equivalent sbt command is
```
sbt "runMain wiki.extractor.WikipediaExtractor /Users/mernst/git/wm-data/enwiki-latest-pages-articles.xml"
```

With default settings on a 2019 Macbook Pro (2.6 GHz 6-Core Intel Core i7, 64 GB RAM) this takes about XX minutes to
complete. TODO: update timing after DB logic merged in.

### Caveats

The software is not deliberately intended to work only for English-language Wikipedia, but there has been very little
testing with other languages.

# Background

See [this Google tech talk](https://www.youtube.com/watch?v=NFCZuzA4cFc) "Knowledge-based Information Retrieval with
Wikipedia" by David Milne from 2008.

Milne's [publications page](https://wikipedia-miner.sourceforge.net/publications.htm) on the old Sourceforge
repository for his original wikipediaminer is informative. In particular:
- [An Effective, Low-Cost Measure of Semantic Relatedness Obtained from Wikipedia Links](https://citeseerx.ist.psu.edu/document?repid=rep1&type=pdf&doi=8d1eda296fcb4ecb4835248e3ab987b453bb7979)
- [Learning to link with wikipedia](https://dl.acm.org/doi/10.1145/1458082.1458150)
- [Topic Indexing with Wikipedia](https://cdn.aaai.org/Workshops/2008/WS-08-15/WS08-15-004.pdf)
- [An open-source toolkit for mining Wikipedia](https://www.sciencedirect.com/science/article/pii/S000437021200077X)
- [Mining Meaning from Wikipedia](https://arxiv.org/abs/0809.4530).

The original [wikipediaminer](https://github.com/dnmilne/wikipediaminer) code can be found on his GitHub. Event older
releases (1.0, 1.1, 1.2) can still be downloaded
[from SourceForge](https://sourceforge.net/projects/wikipedia-miner/files/wikipedia-miner/). These oldest versions may
not be buildable, but they contain pre-built JAR files. 1.0 and 1.1 used Perl for the dump extraction process instead
of Java, and MySQL for the database instead of the [Berkeley DB, Java Edition](https://github.com/berkeleydb/je)
used by subsequent versions.

The GitHub [wiki for wikipediaminer](https://github.com/dnmilne/wikipediaminer/wiki) is also a good related source.

# Motivation
The original wikipediaminer is written in an older dialect of Java (started on 1.5, compiles with warnings on
1.8) and is tied to Hadoop which is cumbersome to set up. Further, computer power has increased enough since the
beginnings of Milne's efforts (circa 2007?) that a single workstation can replace a Hadoop cluster for Wikipedia
processing. To quote "An open-source toolkit for mining Wikipedia",

*This Hadoop-powered extraction process is extremely scalable. Given a cluster of 30 machines, each with two 2.66 GHz
processors and 4 GB of RAM, it processes the latest versions of the full English Wikipedia—3.3 million articles, 27 GB of
uncompressed markup—in a little over 2.5 hours. The process scales roughly linearly with the size of Wikipedia and the
number of machines available.*

Now that this much computing power is easily purchased or rented as a single node, Hadoop isn't necessary. 
Removing the overhead of Hadoop means that even a single contemporary laptop can outdo this old Hadoop cluster. 32 GB
of heap space on one machine is faster than 120 GB spread across 30 machines.

Reimplementing wikipediaminer in Scala probably limits outside contributions, but I am far more fluent in Scala than
in Java.

## Licensing

This is under the GPL v2 because the wikipediaminer code is too. Although I've directly copied only a little from the
old code I have read it a lot, to the extent that this would probably count as a derivative work.

# Usage

## Extracting data from English Wikipedia

### Software requirements

This software has only been tested under Linux and macOS, though there is no conscious inclusion of platform-specific
code.

You will need a Java runtime environment and [sbt](https://www.scala-sbt.org/1.x/docs/Setup.html), the Scala build tool.
The code has been tested on JREs 11 through 23.

The Makefile is written for GNU Make.

### Input data

Download [the latest pages and articles](https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2)
for English Wikipedia. As of 2024-09-03 the file is about 21 GB originally compressed as .bz2, 25 GB compressed as .zst,
and 97 GB decompressed.

It is possible to run the extraction process directly from the downloaded .bz2 file but this will be slow. The
code will spend much of its time decompressing the data and the worker threads may be starved. If you are going to run
the extraction process more than once, decompress the file with the command line utility `bunzip2` or a similar tool.

Running extraction from a .zst compressed input is somewhat slower than from uncompressed input but saves a great deal
of disk space on storage-constrained systems like entry level laptops. It is much faster than reading the input
from .bz2.

### Running extraction
Run the Makefile `extract` target with `dumpfile` (pointing to your input data) set appropriately, like so:

```
make extract dumpfile=/Users/mernst/git/wm-data/enwiki-latest-pages-articles.xml
```

The equivalent sbt command is
```
sbt "runMain wiki.extractor.WikipediaExtractor /Users/mernst/git/wm-data/enwiki-latest-pages-articles.xml"
```

With default settings on a 2019 Macbook Pro (2.6 GHz 6-Core Intel Core i7, 64 GB RAM) and the dump file from September
2024 this takes about 2.5 hours to complete. The output SQLite database is about 72 GB on disk (much larger if running
without page markup compression), with most of the space consumed by `markup_z` or `markup` (table used
depends on "COMPRESS_MARKUP" environment variable).

### Caveats

The software is not deliberately intended to work only for English-language Wikipedia, but there has been very little
testing with other languages.

# Background

## Wikipedia miner
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

## Sweble

The [wikitext](https://en.wikipedia.org/wiki/Help:Wikitext) format used to write Wikipedia entries is complicated and
difficult to parse correctly. Milne had some ad-hoc approaches to the parsing in the original wikipediaminer code,
marked by some frustrated comments. It is very difficult for any software outside the Wikimedia ecosystem to parse
wikitext with full fidelity, but the [Sweble Wikitext Components](https://github.com/sweble/sweble-wikitext) project
comes close. I chose this parser to enable the extraction of cross-references and the rendering of wikitext into
simplified plain text. Its increased fidelity comes at a cost: about 2 hours of the initial 2.5 hour runtime to extract
English Wikipedia is spent in the Sweble parser.

The original [sweble.org](http://sweble.org) domain is defunct now, as is its
[replacement site](https://osr.cs.fau.de/software/sweble-wikitext/) at the Friedrich-Alexander-Universität Professorship
for Open-Source Software. Some useful information is still at those domains in the Wayback Machine, including these
references to these publications:

- [Design and implementation of the sweble wikitext parser: unlocking the structured data of wikipedia](https://opensym.org/ws2011/_media/proceedings%253Ap72-dohrn.pdf)
- [WOM: An object model for Wikitext](https://dirkriehle.com/wp-content/uploads/2011/07/wom-tr.pdf)

N.B. The [DBPedia parser](https://web.archive.org/web/20160424045430/http://oldwiki.dbpedia.org/DeveloperDocumentation/WikiParser)
may be worth investigating too (uses Sweble components internally).

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

Reinterpreting wikipediaminer as Scala probably limits outside contributions, but I am far more comfortable in Scala
than in Java.

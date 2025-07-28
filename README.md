# Usage

## Preparing data, models, and web service for English Wikipedia

### Software requirements

This software has only been tested under Linux and macOS, though there is no conscious inclusion of platform-specific
code.

You will need [a Java runtime environment](https://sdkman.io/), [curl](https://curl.se/), and [sbt](https://www.scala-sbt.org/1.x/docs/Setup.html),
the Scala build tool. The code has been tested on JREs 17 through 23.

The Makefile is written for GNU Make.

The CatBoost model training relies on Python 3.8+ and several libraries, which will be automatically installed via
[uv](https://github.com/astral-sh/uv) when invoking `make train_disambiguation`.

### Quick start

To quickly generate a small, complete database with trained models, run
```
WP_LANG=en_simple make all_in_one
```

If nothing fails, you can then run
```
make run_web_service
```

to activate the live service.

To generate a complete database and trained models for the full English wikipedia, run
```
WP_LANG=en make all_in_one
```

or just
```
make all_in_one
```

### Input data

Download [the latest pages and articles](https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2)
for English Wikipedia by running `make fetch_english_wikipedia`. As of 2025-07-25 the file is about 23 GB originally
compressed as .bz2 and 103 GB decompressed. To run a faster end-to-end test you may first want to use the Simple English
Wikipedia, obtained with `make fetch_simple_english_wikipedia`.

It is possible to run the extraction process directly from the downloaded .bz2 file but this will be slow. The
code will spend much of its time decompressing the data and the worker threads may be starved. If you are going to run
the extraction process more than once, decompress the file with the command line utility `bunzip2` or a similar tool.

Running extraction from a .zst compressed input saves a great deal of disk space on storage-constrained systems like
entry level laptops. It is much faster than reading the input from .bz2.

### Running extraction
Run the Makefile `extract` target with `input` (pointing to your input data) set appropriately, like so:

```
make extract input=enwiki-latest-pages-articles.xml.bz2
```

The equivalent sbt command is
```
sbt "runMain wiki.extractor.WikipediaExtractor enwiki-latest-pages-articles.xml.bz2"
```

With default settings on a 2024 Macbook Pro (M4 Pro, 64 GB RAM) and the dump file from July
2025 this takes about 3 hours to complete. The output SQLite database is about 80 GB on disk (much larger if running
without page markup compression), with most of the space consumed by `markup_z` or `markup` (table used
depends on "COMPRESS_MARKUP" environment variable).

Faster test with Simple English Wikipedia:

```
WP_LANG=en_simple make extract input=simplewiki-latest-pages-articles.xml.bz2
```

Running with French Wikipedia:

```
WP_LANG=fr make extract input=frwiki-20240901-pages-articles.xml.bz2
```

### Model training
The last extraction phase will write CSV files for training word a sense disambiguation model to the current
working directory. After that, you can run

```
make train_disambiguation
```

to train a CatBoost model for word sense disambiguation.

Then run

```
make load_disambiguation
make prepare_link_training
make train_link_detector
make load_linking
```

to train a CatBoost model for linkability detection.

These extraction and model training steps must be run before starting the service for the first time.

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
simplified plain text. Its increased fidelity comes at a cost: about 2 hours of the initial 3 hour runtime to extract
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

Now that this much computing power is commonly available as a single node, Hadoop isn't necessary.

Reinterpreting wikipediaminer as Scala probably limits outside contributions, but I am far more comfortable in Scala
than in Java.

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
old code I have read a lot, to the extent that this would probably be a derivative work if anyone cared to argue about
it in court.
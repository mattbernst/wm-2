.PHONY: build clean extract extract-graal extract-with-profiling format test
JAR := target/scala-2.13/wm-2-assembly-1.0.jar
EXTRACTOR_MAIN := wiki.extractor.WikipediaExtractor
# N.B. the Sweble wikitext parser needs a large Xss to run quickly and without
# encountering StackOverflowErrors
JAVA_OPTS := -Xmx14G -Xss16m -agentlib:jdwp=transport=dt_socket,server=y,address=5000,suspend=n

clean:
	sbt clean

build:
	sbt assembly

extract: build
	java $(JAVA_OPTS) -cp $(JAR) $(EXTRACTOR_MAIN) $(input)

# This only works with Oracle Java 21 or later. On my machine it reduces the
# 2 hour and 25 minute extraction time to 2 hours and 10 minutes.
GRAAL_JAVA_OPTS := $(JAVA_OPTS) -XX:+UnlockExperimentalVMOptions -XX:+UseGraalJIT
extract-graal: build
	java $(GRAAL_JAVA_OPTS) -cp $(JAR) $(EXTRACTOR_MAIN) $(input)

P_JAVA_OPTS := $(JAVA_OPTS) -XX:FlightRecorderOptions=stackdepth=1024 -XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints -XX:StartFlightRecording:maxsize=10000MB,filename=extraction.jfr
# Profile extraction with Flight Recorder for analysis with JDK
# Mission Control
# https://www.oracle.com/java/technologies/jdk-mission-control.html
# https://github.com/openjdk/jmc
extract-with-profiling: build
	java $(P_JAVA_OPTS) -cp $(JAR) $(EXTRACTOR_MAIN) $(input)

format:
	sbt scalafmtAll

test:
	sbt test

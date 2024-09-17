.PHONY: clean build extract extract-with-profiling format
JAR := target/scala-2.13/wm-2-assembly-1.0.jar
EXTRACTOR_MAIN := wiki.extractor.WikipediaExtractor
JAVA_OPTS := -Xmx16G -agentlib:jdwp=transport=dt_socket,server=y,address=5000,suspend=n

clean:
	sbt clean

build:
	sbt assembly

extract: build
	java $(JAVA_OPTS) -cp $(JAR) $(EXTRACTOR_MAIN) $(dumpfile)

format:
	sbt scalafmtAll

P_JAVA_OPTS := $(JAVA_OPTS) -XX:FlightRecorderOptions=stackdepth=1024 -XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints -XX:StartFlightRecording:maxsize=1000MB,filename=extraction.jfr
# Profile extraction with Flight Recorder for analysis with Mission Control
# https://www.oracle.com/java/technologies/jdk-mission-control.html
# https://github.com/openjdk/jmc
extract-with-profiling: build
	java $(P_JAVA_OPTS) -cp $(JAR) $(EXTRACTOR_MAIN) $(dumpfile)

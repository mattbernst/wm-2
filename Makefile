.PHONY: clean build extract extract-with-profiling
JAR := target/scala-2.13/wm-2-assembly-1.0.jar
EXTRACTOR_MAIN := wiki.extractor.WikipediaExtractor
JAVA_OPTS := -Xmx12G

clean:
	sbt clean

build:
	sbt assembly

extract: build
	java -cp $(JAR) $(EXTRACTOR_MAIN) $(dumpfile)

P_JAVA_OPTS := $(JAVA_OPTS) -XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints -XX:StartFlightRecording=filename=extraction.jfr
extract-with-profiling:
	java $(P_JAVA_OPTS) -cp $(JAR) $(EXTRACTOR_MAIN) $(dumpfile)

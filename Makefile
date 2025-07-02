.PHONY: build clean extract extract-graal extract-with-profiling fetch-english-wikipedia fetch-french-wikipedia fetch-simple-english-wikipedia format test train-disambiguation
JAR := target/scala-2.13/wm-2-assembly-1.0.jar
EXTRACTOR_MAIN := wiki.extractor.WikipediaExtractor
PREPARE_DISAMBIGUATION_MAIN := wiki.service.PrepareDisambiguation
PREPARE_LINK_TRAINING_MAIN := wiki.extractor.ExtractLinkTrainingData
WEB_SERVICE_MAIN := wiki.service.WebService
# N.B. the Sweble wikitext parser needs a large Xss to run quickly and without
# encountering StackOverflowErrors
JAVA_OPTS := -Xmx14G -Xss16m -agentlib:jdwp=transport=dt_socket,server=y,address=5000,suspend=n

clean:
	sbt clean

build:
	sbt assembly

extract: build
	java $(JAVA_OPTS) -cp $(JAR) $(EXTRACTOR_MAIN) $(input)

fetch-english-wikipedia:
	curl -L -o enwiki-latest-pages-articles.xml.bz2 https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2

fetch-french-wikipedia:
	curl -L -o frwiki-latest-pages-articles.xml.bz2 https://dumps.wikimedia.org/frwiki/latest/frwiki-latest-pages-articles.xml.bz2

fetch-simple-english-wikipedia:
	curl -L -o simplewiki-latest-pages-articles.xml.bz2 https://dumps.wikimedia.org/simplewiki/latest/simplewiki-latest-pages-articles.xml.bz2

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

prepare-disambiguation: build
	java $(JAVA_OPTS) -cp $(JAR) $(PREPARE_DISAMBIGUATION_MAIN) $(input)

prepare-link-training: build
	java $(P_JAVA_OPTS) -cp $(JAR) $(PREPARE_LINK_TRAINING_MAIN) $(input)

run-web-service: build
	java $(JAVA_OPTS) -cp $(JAR) $(WEB_SERVICE_MAIN) $(input)

train-disambiguation:
	@echo "Setting up disambiguation training..."
	@# Check for CSV files and determine language
	@if [ -n "$(WP_LANG)" ]; then \
		LANG_CODE="$(WP_LANG)"; \
	else \
		AVAILABLE_LANGS=$$(ls wiki_*_disambiguation-train.csv 2>/dev/null | sed 's/wiki_\(.*\)_disambiguation-train\.csv/\1/' | sort -u); \
		LANG_COUNT=$$(echo "$$AVAILABLE_LANGS" | wc -w); \
		if [ $$LANG_COUNT -eq 0 ]; then \
			echo "Error: No training CSV files found (wiki_*_disambiguation-train.csv)"; \
			echo "Please run 'make extract' first to generate the required CSV files."; \
			exit 1; \
		elif [ $$LANG_COUNT -gt 1 ]; then \
			echo "Error: Multiple language CSV files found: $$AVAILABLE_LANGS"; \
			echo "Please set WP_LANG environment variable to specify which language to use."; \
			echo "Example: WP_LANG=en make train-disambiguation"; \
			exit 1; \
		else \
			LANG_CODE="$$AVAILABLE_LANGS"; \
		fi; \
	fi; \
	\
	echo "Using language code: $$LANG_CODE"; \
	\
	if [ ! -f "wiki_$${LANG_CODE}_disambiguation-train.csv" ]; then \
		echo "Error: Training file wiki_$${LANG_CODE}_disambiguation-train.csv not found"; \
		echo "Please run 'make extract' to generate the required CSV files."; \
		exit 1; \
	fi; \
	\
	if [ ! -f "wiki_$${LANG_CODE}_disambiguation-test.csv" ]; then \
		echo "Error: Test file wiki_$${LANG_CODE}_disambiguation-test.csv not found"; \
		echo "Please run 'make extract' to generate the required CSV files."; \
		exit 1; \
	fi; \
	\
	echo "Found required CSV files for language: $$LANG_CODE"; \
	\
	if ! command -v uv >/dev/null 2>&1; then \
		echo "Installing uv package manager..."; \
		curl -LsSf https://astral.sh/uv/install.sh | sh; \
		export PATH="$$HOME/.local/bin:$$PATH"; \
		if ! command -v uv >/dev/null 2>&1; then \
			echo "Error: Failed to install uv. Please install it manually."; \
			exit 1; \
		fi; \
	fi; \
	\
	echo "Setting up Python virtual environment..."; \
	if [ ! -d "pysrc/.venv" ]; then \
		echo "Creating virtual environment and installing requirements..."; \
		cd pysrc && uv venv && uv pip install -r requirements.txt && cd ..; \
	else \
		echo "Virtual environment already exists"; \
	fi; \
	\
	echo "Running disambiguation training..."; \
	cd pysrc && \
	uv run python train_word_sense_disambiguation.py \
		--train-file ../wiki_$${LANG_CODE}_disambiguation-train.csv \
		--val-file ../wiki_$${LANG_CODE}_disambiguation-test.csv

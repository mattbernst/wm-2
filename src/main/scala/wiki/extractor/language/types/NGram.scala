package wiki.extractor.language.types

import opennlp.tools.util.Span

case class NGram(
  start: Int,
  end: Int,
  tokenSpans: Array[Span],
  caseContext: CaseContext,
  isSentenceStart: Boolean)
    extends Span(start, end) {

  def getNgramAsString(sourceText: String): String =
    sourceText.substring(start, end)
}

object NGram {

  /**
    * Generate the strings enclosed by a series of NGrams. Each NGram only
    * contains indices of positions in the source string. This generates
    * the sequence of substrings so that string fragments can be directly
    * inspected and compared.
    *
    * @param sourceString The original string that ngrams came from
    * @param ngrams       A sequence of ngrams extracted from the source string
    * @return             Pieces of the original source string
    */
  def generateStrings(sourceString: String, ngrams: Seq[NGram]): Seq[String] = {
    ngrams.map(ngram => generateString(sourceString, ngram))
  }

  def generateString(sourceString: String, ngram: NGram): String =
    sourceString.slice(ngram.start, ngram.end)
}

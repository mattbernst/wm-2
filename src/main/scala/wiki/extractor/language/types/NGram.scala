package wiki.extractor.language.types

import opennlp.tools.util.Span

import java.util.Locale

case class NGram(
  start: Int,
  end: Int,
  tokenSpans: Array[Span],
  caseContext: CaseContext,
  isSentenceStart: Boolean)
    extends Span(start, end) {

  def getNgram(sourceText: String): String =
    sourceText.substring(start, end)

  def getNgramUpperFirst(sourceText: String, locale: Locale): String = {
    val ngram = getNgram(sourceText).toLowerCase(locale).toCharArray
    tokenSpans.foreach { span =>
      ngram(span.getStart) = ngram(span.getStart).toUpper
    }
    new String(ngram)
  }
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
  def sliceString(sourceString: String, ngrams: Seq[NGram]): Seq[String] = {
    ngrams.map(ngram => sliceString(sourceString, ngram))
  }

  def sliceString(sourceString: String, ngram: NGram): String =
    sourceString.slice(ngram.start, ngram.end)
}

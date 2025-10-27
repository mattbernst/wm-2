package wiki.extractor.language.types

import opennlp.tools.util.Span

case class NGram(
  start: Int,
  end: Int,
  tokenSpans: Array[Span],
  caseContext: CaseContext,
  stringContent: String,
  isSentenceStart: Boolean,
  isDowncased: Boolean)
    extends Span(start, end) {

  override def equals(other: Any): Boolean = other match {
    case that: NGram =>
      start == that.start &&
        end == that.end &&
        caseContext == that.caseContext &&
        stringContent == that.stringContent &&
        isSentenceStart == that.isSentenceStart &&
        isDowncased == that.isDowncased
    case _ => false
  }

  override def hashCode(): Int = {
    val prime  = 31
    var result = 1
    result = prime * result + start
    result = prime * result + end
    result = prime * result + caseContext.hashCode
    result = prime * result + stringContent.hashCode
    result = prime * result + (if (isSentenceStart) 1 else 0)
    result = prime * result + (if (isDowncased) 1 else 0)
    result
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
  def generateStrings(sourceString: String, ngrams: Seq[NGram]): Seq[String] = {
    ngrams.map(ngram => generateString(sourceString, ngram))
  }

  def generateString(sourceString: String, ngram: NGram): String =
    sourceString.substring(ngram.start, ngram.end)
}

package wiki.extractor.language

import opennlp.tools.sentdetect.SentenceDetector
import opennlp.tools.tokenize.Tokenizer
import opennlp.tools.util.Span
import wiki.extractor.language.types.{CaseContext, NGram}

class NGramGenerator(sentenceDetector: SentenceDetector, tokenizer: Tokenizer, maxTokens: Int = 10) {

  /**
    * Generate token-based ngrams of up to maxTokens tokens.
    *
    * @param text Text of a document to convert to ngrams
    * @return     All ngrams generated from the input text
    */
  def generate(text: String): Array[NGram] = {
    val ngramSpans = for {
      (line, lineStart) <- text.split("\n").zipWithIndex
      sentenceSpan      <- sentenceDetector.sentPosDetect(line)
      sentence    = line.substring(sentenceSpan.getStart, sentenceSpan.getEnd)
      tokenSpans  = tokenizer.tokenizePos(sentence)
      caseContext = identifyCaseContext(sentence, tokenSpans)
      i <- tokenSpans.indices
      if !(tokenSpans(i).length == 1 &&
        !sentence.charAt(tokenSpans(i).getStart).isLetterOrDigit)
      j <- i to math.min(i + n, tokenSpans.length - 1)
      if !(tokenSpans(j).length == 1 &&
        !sentence.charAt(tokenSpans(j).getStart).isLetterOrDigit)
    } yield {
      val globalStart = lineStart + sentenceSpan.getStart + tokenSpans(i).getStart
      val globalEnd   = lineStart + sentenceSpan.getStart + tokenSpans(j).getEnd
      val ngramStart  = tokenSpans(i).getStart

      val tokenSpansLocalToNgram = (i to j).map { k =>
        val tokenSpan = tokenSpans(k)
        new Span(
          tokenSpan.getStart - ngramStart,
          tokenSpan.getEnd - ngramStart
        )
      }.toArray

      NGram(
        start = globalStart,
        end = globalEnd,
        tokenSpans = tokenSpansLocalToNgram,
        caseContext = caseContext,
        isSentenceStart = i == 0
      )
    }

    ngramSpans
  }

  private def identifyCaseContext(text: String, tokenSpans: Array[Span]): CaseContext = {
    val contexts = tokenSpans.map { span =>
      val token = text.substring(span.getStart, span.getEnd)
      identifyCaseContext(token)
    }

    if (contexts.forall(_ == CaseContext.Upper)) CaseContext.Upper
    else if (contexts.forall(_ == CaseContext.Lower)) CaseContext.Lower
    else if (contexts.forall(c => c == CaseContext.Upper || c == CaseContext.UpperFirst)) CaseContext.UpperFirst
    else CaseContext.Mixed
  }

  private def identifyCaseContext(token: String): CaseContext = {
    val (allUpper, allLower, upperFirst) = token.foldLeft((true, true, true)) {
      case ((isAllUpper, isAllLower, isUpperFirst), c) =>
        val newAllUpper = isAllUpper && c.isUpper
        val newAllLower = isAllLower && c.isLower
        val newUpperFirst =
          if (token.indexOf(c.toInt) == 0) isUpperFirst && c.isUpper
          else isUpperFirst && c.isLower
        (newAllUpper, newAllLower, newUpperFirst)
    }

    if (allUpper) CaseContext.Upper
    else if (allLower) CaseContext.Lower
    else if (upperFirst) CaseContext.UpperFirst
    else CaseContext.Mixed
  }

  require(maxTokens >= 1, "Cannot generate NGrams with less than 1 token")
  private val n = maxTokens - 1
}

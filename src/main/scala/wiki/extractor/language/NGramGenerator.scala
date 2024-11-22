package wiki.extractor.language

import opennlp.tools.sentdetect.SentenceDetector
import opennlp.tools.tokenize.Tokenizer
import opennlp.tools.util.Span
import wiki.extractor.language.types.{CaseContext, NGram}

class NGramGenerator(sentenceDetector: SentenceDetector, tokenizer: Tokenizer, maxTokens: Int = 8) {

  /**
    * Generate token-based ngrams of up to maxTokens tokens per ngram. The
    * ngrams do not cross sentence boundaries.
    *
    * @param text Text of a document to convert to ngrams
    * @return     All ngrams generated from the input text
    */
  def generate(text: String): Array[NGram] = {
    val ngramSpans = for {
      (line, lineStart) <- text.split("\n").zipWithIndex
      sentenceSpan      <- sentenceDetector.sentPosDetect(line)
      sentence                = line.substring(sentenceSpan.getStart, sentenceSpan.getEnd)
      tokenSpans: Array[Span] = tokenizer.tokenizePos(sentence)
      caseContext             = identifyCaseContext(sentence, tokenSpans)
      left <- tokenSpans.indices
      //  An ngram cannot start with a punctuation token
      if !(tokenSpans(left).length == 1 &&
        !sentence.charAt(tokenSpans(left).getStart).isLetterOrDigit)
      right <- left.to(math.min(left + n, tokenSpans.length - 1))
      //  An ngram cannot end with a punctuation token
      if !(tokenSpans(right).length == 1 &&
        !sentence.charAt(tokenSpans(right).getStart).isLetterOrDigit)
    } yield {
      val globalStart = lineStart + sentenceSpan.getStart + tokenSpans(left).getStart
      val globalEnd   = lineStart + sentenceSpan.getStart + tokenSpans(right).getEnd
      val ngramStart  = tokenSpans(left).getStart

      val tokenSpansLocalToNgram = (left.to(right)).map { k =>
        val tokenSpan = tokenSpans(k)
        new Span(
          tokenSpan.getStart - ngramStart,
          tokenSpan.getEnd - ngramStart
        )
      }

      NGram(
        start = globalStart,
        end = globalEnd,
        tokenSpans = tokenSpansLocalToNgram.toArray,
        caseContext = caseContext,
        isSentenceStart = left == 0
      )
    }

    ngramSpans
  }

  private def identifyCaseContext(text: String, tokenSpans: Array[Span]): CaseContext = {
    val contexts = tokenSpans.map { span =>
      val token = text.substring(span.getStart, span.getEnd)
      identifyCaseContext(token)
    }

    if (contexts.forall(_ == CaseContext.UPPER)) CaseContext.UPPER
    else if (contexts.forall(_ == CaseContext.LOWER)) CaseContext.LOWER
    else if (contexts.forall(c => c == CaseContext.UPPER || c == CaseContext.UPPER_FIRST)) CaseContext.UPPER_FIRST
    else CaseContext.MIXED
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

    if (allUpper) CaseContext.UPPER
    else if (allLower) CaseContext.LOWER
    else if (upperFirst) CaseContext.UPPER_FIRST
    else CaseContext.MIXED
  }

  require(maxTokens >= 1, "Cannot generate NGrams with less than 1 token")
  private val n = maxTokens - 1
}

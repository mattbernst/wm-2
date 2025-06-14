package wiki.extractor.language

import opennlp.tools.sentdetect.SentenceDetector
import opennlp.tools.tokenize.Tokenizer
import opennlp.tools.util.Span
import wiki.extractor.language.types.{CaseContext, NGram}

import scala.collection.mutable

class NGramGenerator(
  sentenceDetector: SentenceDetector,
  tokenizer: Tokenizer,
  maxTokens: Int = 10,
  allowedStrings: mutable.Set[String] = mutable.Set()) {

  /**
    * Generate token-based ngrams of up to maxTokens tokens per ngram. The
    * ngrams do not cross sentence boundaries. Errors from the OpenNLP
    * sentence detection model can prevent some valid ngrams from being
    * generated (see docstring on generateFast).
    *
    * @param text Text of a document to convert to ngrams
    * @return     All ngrams generated from the input text
    */
  def generate(text: String): Array[NGram] = {
    // Calculate the starting position of each line in the original text
    val lines = text.split("\n")
    val lineStarts = lines
      .foldLeft(List(0)) {
        case (acc, line) =>
          // Next line starts after current line plus newline character
          acc :+ (acc.last + line.length + 1)
      }
      .dropRight(1) // Drop the last element as it's the position after the last line

    val ngramSpans = for {
      (line, lineStart) <- lines.zip(lineStarts)
      sentenceSpan      <- sentenceDetector.sentPosDetect(line)
      sentence                = line.substring(sentenceSpan.getStart, sentenceSpan.getEnd)
      tokenSpans: Array[Span] = tokenizer.tokenizePos(sentence)
      caseContext             = identifyCaseContext(sentence, tokenSpans)
      left <- tokenSpans.indices
      //  NGram cannot start with a punctuation token
      if isAlphanumericToken(sentence, tokenSpans(left))
      right <- left.to(math.min(left + n, tokenSpans.length - 1))
      //  NGram cannot end with a punctuation token
      if isAlphanumericToken(sentence, tokenSpans(right))
    } yield {
      val globalStart = lineStart + sentenceSpan.getStart + tokenSpans(left).getStart
      val globalEnd   = lineStart + sentenceSpan.getStart + tokenSpans(right).getEnd
      val ngramStart  = tokenSpans(left).getStart

      val tokenSpansLocalToNgram = left.to(right).map { k =>
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

  /**
    * Helper method to check if a token span represents a single
    * alphanumeric character. Properly handles Unicode supplementary
    * characters.
    *
    * @param sentence  The originating sentence for the tokenSpan
    * @param tokenSpan An OpenNLP Span representing part of the sentence
    * @return          True if the span under inspection is alphanumeric,
    *                  false otherwise
    */
  private def isAlphanumericToken(sentence: String, tokenSpan: Span): Boolean = {
    // Check if it's a single character token (accounting for surrogate pairs)
    val start          = tokenSpan.getStart
    val end            = tokenSpan.getEnd
    val codePointCount = sentence.codePointCount(start, end)
    if (codePointCount != 1) {
      true
    } else {
      // Get the single code point and check if it's a letter or digit
      val codePoint = sentence.codePointAt(start)
      Character.isLetterOrDigit(codePoint)
    }
  }

  /**
    * Generate strings composed of token-based ngrams of up to maxTokens tokens
    * per ngram. The ngrams do not cross line boundaries. This fast alternative
    * to "generate" is only used for bulk processing the pages of a Wikipedia
    * dump.
    *
    * The original ngram generation logic in Milne's code split the code into
    * sentences before splitting them into tokens. This probably reduced the
    * computational load and also reduced the false positives by not allowing
    * a sentence-ending punctuation mark to be confused with punctuation
    * internal to a named entity.
    *
    * However, the OpenNLP sentence detection model frequently mis-detects the
    * end of a sentence when it encounters a period in a named entity as in
    * "James T. Kirk is the captain of the fictional starship Enterprise."
    * (where it mistakenly finds two sentences, "James T." and
    * "Kirk is the captain of the fictional starship Enterprise.)
    *
    * These extra sentence splits can prevent detection of named entities
    * containing punctuation and have therefore been removed from this
    * implementation.
    *
    * @param text Text of a document to convert to ngrams
    * @return All valid ngram-strings generated from the input text
    */
  def generateFast(text: String): Array[String] = {
    var j      = 0
    val result = mutable.ListBuffer[String]()
    val lines  = text.split('\n')
    while (j < lines.length) {
      val line   = lines(j)
      val tokens = tokenizer.tokenizePos(line)

      var left = 0
      while (left <= tokens.length) {
        var right = math.min(left + maxTokens, tokens.length)
        while (right >= left) {
          val slice = tokens.slice(left, right)

          if (slice.nonEmpty) {
            val combined = line.substring(slice.head.getStart, slice.last.getEnd)

            // Check if combined string starts and ends with letter or digit
            // Handle potential Unicode surrogate pairs properly
            val isValidString = combined.nonEmpty &&
              Character.isLetterOrDigit(combined.codePointAt(0)) &&
              Character.isLetterOrDigit(combined.codePointBefore(combined.length))

            if (isValidString && (allowedStrings.isEmpty || allowedStrings.contains(combined))) {
              result.append(combined)
            }
          }
          right -= 1
        }
        left += 1
      }

      j += 1
    }

    result.toArray
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

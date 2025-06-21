package wiki.extractor.language

import opennlp.tools.sentdetect.SentenceDetectorME
import opennlp.tools.tokenize.TokenizerME
import wiki.extractor.language.types.Snippet
import wiki.extractor.types.Language
import wiki.util.Logging

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

trait LanguageLogic {

  /**
    * Use heuristics to get the first paragraph and first sentence of content
    * from a potentially large input string representing a plain-text version
    * of a Wikipedia page.
    *
    * Heuristic: Get groups of sentences separated by newlines. The first
    * multi-sentence group is the first paragraph. The first sentence is taken
    * from the first paragraph. If there are no multi-sentence groups, but there
    * are single-sentence groups, then the first paragraph is None and the first
    * sentence is the first sentence from the input. If there are no sentences
    * at all, then both firstParagraph and firstSentence are None.
    *
    * @param input Text to process for extraction
    * @return      A snippet of extracted text content
    */
  def getSnippet(input: String): Snippet = {
    val lines = input.split('\n')

    val firstParaGroup = lines.iterator
      .map(chunk => sentenceDetector.get().sentDetect(chunk))
      .find(_.length > 1)

    val firstParagraph = firstParaGroup.map(_.mkString(" "))
    val firstSentence = firstParaGroup
      .flatMap(_.headOption)
      .orElse {
        lines.iterator
          .map(chunk => sentenceDetector.get().sentDetect(chunk))
          .find(_.nonEmpty)
          .map(_.head)
      }

    Snippet(firstParagraph = firstParagraph, firstSentence = firstSentence)
  }

  /**
    * Generate word NGrams and filter them against the valid set of NGrams.
    * This is intended for use during Wikipedia document label counting, where
    * the only labels we want to count are those that have been previously
    * identified as anchor text from links.
    *
    * @param wikiPagePlainText A plain-text rendition of a Wikipedia entry
    * @param valid             The set of valid strings to retain
    * @return                  An array of word-level NGrams as strings
    */
  def wikiWordNGrams(wikiPagePlainText: String, valid: mutable.Set[String]): Array[String] =
    fastNGrams(wikiPagePlainText, valid)

  /**
    * Generate word NGrams from a text document. This is a more general
    * function that can handle any kind of document. The main different
    * from wikiWordNGrams is that it will generate lower-cased variants of
    * NGrams for beginning-of-sentence NGrams. This requires a slower code path
    * than wikiWordNGrams.
    *
    * @param language     The language to use for processing the document
    * @param documentText The plain text of a document
    * @return             An array of word-level NGrams as strings
    */
  def wordNGrams(language: Language, documentText: String): Array[String] = {
    val buffer = ListBuffer[String]()
    val ngg    = new NGramGenerator(sentenceDetector.get(), tokenizer.get())

    ngg.generate(documentText).foreach { ng =>
      buffer.append(ng.getNgramAsString(documentText))
      if (ng.isSentenceStart) {
        buffer.append(language.unCapitalizeFirst(ng.getNgramAsString(documentText)))
      }
    }

    buffer.toArray
  }

  private[language] def fastNGrams(input: String, valid: mutable.Set[String]): Array[String] = {
    val ngg = new NGramGenerator(sentenceDetector.get(), tokenizer.get(), allowedStrings = valid)
    ngg.generateFiltered(input)
  }

  protected def tokenizer: ThreadLocal[TokenizerME]

  protected def sentenceDetector: ThreadLocal[SentenceDetectorME]
}

object LanguageLogic extends Logging {

  /**
    * Get the language-specific LanguageLogic implementation from its
    * corresponding ISO 639-1 language code. Throws if no implementation
    * yet defined and mapped for the given language code.
    *
    * @param languageCode An ISO 639-1 language code e.g. "en"
    * @return             Language-specific NLP logic
    */
  def getLanguageLogic(languageCode: String): LanguageLogic = {
    Try(logicForLanguage(languageCode)) match {
      case Success(res) =>
        res
      case Failure(ex: NoSuchElementException) =>
        logger.error(s"No LanguageLogic defined for language code '$languageCode'")
        throw ex
      case Failure(ex) =>
        throw ex
    }
  }

  private val logicForLanguage: Map[String, LanguageLogic] = Map(
    "en"        -> EnglishLanguageLogic,
    "en_simple" -> EnglishLanguageLogic,
    "fr"        -> FrenchLanguageLogic
  )
}

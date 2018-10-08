package com.databricks.spark.corenlp

import java.util.Properties

import scala.collection.JavaConverters._

import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, CleanXmlAnnotator, StanfordCoreNLP, TokenizerAnnotator}
import edu.stanford.nlp.pipeline.CoreNLPProtos.Sentiment
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import edu.stanford.nlp.simple.{Document, Sentence}
import edu.stanford.nlp.util.Quadruple

import org.apache.spark.sql.functions.udf

/**
  * A collection of Spark SQL UDFs that wrap CoreNLP annotators and simple functions.
  *
  * @see [[edu.stanford.nlp.simple]]
  */
object functions {

  @transient private var sentimentPipeline: StanfordCoreNLP = _

  private def getOrCreateSentimentPipeline(): StanfordCoreNLP = {
    if (sentimentPipeline == null) {
      val props = new Properties()
      props.setProperty("annotators", "tokenize, ssplit, parse, sentiment")
      sentimentPipeline = new StanfordCoreNLP(props)
    }
    sentimentPipeline
  }

  private case class OpenIE(subject: String, relation: String, target: String, confidence: Double) {
    def this(quadruple: Quadruple[String, String, String, java.lang.Double]) =
      this(quadruple.first, quadruple.second, quadruple.third, quadruple.fourth)
  }

  private case class CorefMention(sentNum: Int, startIndex: Int, mention: String)

  private case class CorefChain(representative: String, mentions: Seq[CorefMention])

  private case class SemanticGraphEdge(source: String,
                                        sourceIndex: Int,
                                        relation: String,
                                        target: String,
                                        targetIndex: Int,
                                        weight: Double)

  /**
    * Cleans XML tags in a document.
    */
  def cleanxml = udf { document: String =>
    val annotation = new Annotation(document)
    val tokenizerAnnotator = new TokenizerAnnotator()
    tokenizerAnnotator.annotate(annotation)
    val cleanXmlAnnotator = new CleanXmlAnnotator()
    cleanXmlAnnotator.annotate(annotation)
    val tokens = annotation.get(classOf[CoreAnnotations.TokensAnnotation])
    tokens.asScala.map(_.word()).mkString(" ")
  }

  /**
    * Tokenizes a sentence into words.
    * @see [[Sentence#words]]
    */
  def tokenize = udf { sentence: String =>
    new Sentence(sentence).words().asScala
  }

  /**
    * Tokenizes a sentence into words, with custom config.
    * @see [[Sentence#words]]
    */
  def tokenize(properties: Properties) = udf { sentence: String =>
    new Sentence(sentence, properties).words().asScala
  }

  /**
   * Splits a document into sentences.
   * @see [[Document#sentences]]
   */
  def ssplit = udf { document: String =>
    new Document(document).sentences().asScala.map(_.text())
  }

  /**
    * Splits a document into sentences, with custom config.
    * @see [[Document#sentences]]
    */
  def ssplit(properties: Properties) = udf { document: String =>
    new Document(document).sentences(properties).asScala.map(_.text())
  }

  /**
   * Generates the part of speech tags of the sentence.
   * @see [[Sentence#posTags]]
   */
  def pos = udf { sentence: String =>
    new Sentence(sentence).posTags().asScala
  }

  /**
   * Generates the part of speech tags of the sentence, with custom config.
   * @see [[Sentence#posTags]]
   */
    def pos(properties: Properties) = udf { sentence: String =>
    new Sentence(sentence, properties).posTags(properties).asScala
  }

  /**
    * Generates the word lemmas of the sentence.
    * @see [[Sentence#lemmas]]
    */
  def lemma = udf { sentence: String =>
    new Sentence(sentence).lemmas().asScala
  }

  /**
    * Generates the word lemmas of the sentence, with custom config.
    * @see [[Sentence#lemmas]]
    */
  def lemma(properties: Properties) = udf { sentence: String =>
    new Sentence(sentence, properties).lemmas(properties).asScala
  }

  /**
    * Generates the named entity tags of the sentence.
    * @see [[Sentence#nerTags]]
    */
  def ner = udf { sentence: String =>
    new Sentence(sentence).nerTags().asScala
  }

  /**
    * Generates the named entity tags of the sentence, with custom config.
    * @see [[Sentence#nerTags]]
    */
  def ner(properties: Properties) = udf { sentence: String =>
    new Sentence(sentence, properties).nerTags(properties).asScala
  }

  /**
    * Generates the constituency parsing of the sentence.
    * @see [[Sentence#parse]]
    */
  def parse = udf { sentence: String =>
    new Sentence(sentence).parse()
      .pennString()
      .replaceAll("\n", " ")
      .replaceAll("\\s{2,}", " ")
      .trim
  }

  /**
    * Generates the constituency parsing of the sentence, with custom config.
    * @see [[Sentence#parse]]
    */
  def parse(properties: Properties) = udf { sentence: String =>
    new Sentence(sentence, properties).parse(properties)
      .pennString()
      .replaceAll("\n", " ")
      .replaceAll("\\s{2,}", " ")
      .trim
  }

  /**
   * Generates the semantic dependencies of the sentence.
   * @see [[Sentence#dependencyGraph]]
   */
  def depparse = udf { sentence: String =>
    new Sentence(sentence).dependencyGraph().edgeListSorted().asScala.map { edge =>
      SemanticGraphEdge(
        edge.getSource.word(),
        edge.getSource.index(),
        edge.getRelation.toString,
        edge.getTarget.word(),
        edge.getTarget.index(),
        edge.getWeight)
    }
  }

  /**
    * Generates the semantic dependencies of the sentence, with custom config.
    * @see [[Sentence#dependencyGraph]]
    */
  def depparse(properties: Properties) = udf { sentence: String =>
    new Sentence(sentence, properties).dependencyGraph(properties).edgeListSorted().asScala.map { edge =>
      SemanticGraphEdge(
        edge.getSource.word(),
        edge.getSource.index(),
        edge.getRelation.toString,
        edge.getTarget.word(),
        edge.getTarget.index(),
        edge.getWeight)
    }
  }

  /**
   * Generates the coref chains of the document.
   */
  def coref = udf { document: String =>
    new Document(document).coref().asScala.values.map { chain =>
      val rep = chain.getRepresentativeMention.mentionSpan
      val mentions = chain.getMentionsInTextualOrder.asScala.map { m =>
        CorefMention(m.sentNum, m.startIndex, m.mentionSpan)
      }
      CorefChain(rep, mentions)
    }.toSeq
  }

  /**
   * Generates the Natural Logic notion of polarity for each token in a sentence,
   * returned as "up", "down", or "flat".
   * @see [[Sentence#natlogPolarities]]
   */
  def natlog = udf { sentence: String =>
    new Sentence(sentence).natlogPolarities().asScala
        .map(_.toString)
  }

  /**
   * Generates a list of Open IE triples as flat (subject, relation, target, confidence) quadruples.
   * @see [[Sentence#openie]]
   */
  def openie = udf { sentence: String =>
    new Sentence(sentence).openie().asScala.map(q => new OpenIE(q)).toSeq
  }

  /**
   * Measures the sentiment of an input sentence on a scale of 0 (strong negative) to 4 (strong
   * positive).
   * If the input contains multiple sentences, only the first one is used.
   * @see [[Sentiment]]
   */
  def sentiment = udf { sentence: String =>
    val pipeline = getOrCreateSentimentPipeline()
    val annotation = pipeline.process(sentence)
    val tree = annotation.get(classOf[CoreAnnotations.SentencesAnnotation])
      .asScala
      .head
      .get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])
    RNNCoreAnnotations.getPredictedClass(tree)
  }
}

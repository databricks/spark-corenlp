package com.databricks.spark.corenlp

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.NamedExpression

import scala.reflect.runtime.universe.TypeTag

import scala.collection.JavaConverters._

import edu.stanford.nlp.ling.CoreLabel
import edu.stanford.nlp.pipeline.CleanXmlAnnotator
import edu.stanford.nlp.simple.{Document, Sentence}
import edu.stanford.nlp.util.Quadruple

import org.apache.spark.sql.functions.udf

/**
 * A collection of Spark SQL UDFs that wrap CoreNLP annotators and simple functions.
 * @see [[edu.stanford.nlp.simple]]
 */
object functions {

  private case class OpenIE(subject: String, relation: String, target: String, confidence: Double) {
    def this(quadruple: Quadruple[String, String, String, java.lang.Double]) =
      this(quadruple.first, quadruple.second, quadruple.third, quadruple.fourth)
  }

  private case class CorefMention(sentNum: Int, startIndex: Int, mention: String)

  private case class CorefChain(representative: String, mentions: Seq[CorefMention])

  private case class SemanticGraphEdge(
    source: String,
    sourceIndex: Int,
    relation: String,
    target: String,
    targetIndex: Int,
    weight: Double)

  /**
   * Cleans XML tags in a document.
   */
  def cleanxml = udf { document: String =>
    val words = new Sentence(document).words().asScala
    val labels = words.map { w =>
      val label = new CoreLabel()
      label.setWord(w)
      label
    }
    val annotator = new CleanXmlAnnotator()
    annotator.process(labels.asJava).asScala.map(_.word()).mkString(" ")
  }

  /**
   * Tokenizes a sentence into words.
   * @see [[Sentence#words]]
   */
  def tokenize = udf { sentence: String =>
    new Sentence(sentence).words().asScala
  }

  /**
   * Splits a document into sentences.
   * @see [[Document#sentences]]
   */
  def ssplit = udf { document: String =>
    new Document(document).sentences().asScala.map(_.text())
  }

  /**
   * Generates the part of speech tags of the sentence.
   * @see [[Sentence#posTags]]
   */
  def pos = udf { sentence: String =>
    new Sentence(sentence).posTags().asScala
  }

  /**
   * Generates the word lemmas of the sentence.
   * @see [[Sentence#lemmas]]
   */
  def lemma = udf { sentence: String =>
    new Sentence(sentence).lemmas().asScala
  }

  /**
   * Generates the named entity tags of the sentences.
   * @see [[Sentence#nerTags]]
   */
  def ner = udf { sentence: String =>
    new Sentence(sentence).nerTags().asScala
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
   * Generates a list of Open IE triples as flat (subject, relation, object, confidence) quadruples.
   * @see [[Sentence#openie]]
   */
  def openie = udf { sentence: String =>
    new Sentence(sentence).openie().asScala.map(q => new OpenIE(q)).toSeq
  }
}

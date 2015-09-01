package com.databricks.spark.corenlp

import java.util.Properties

import edu.stanford.nlp.pipeline.StanfordCoreNLP

/**
 * A serializable wrapper of [[StanfordCoreNLP]].
 * @param props properties used to construct [[StanfordCoreNLP]]
 */
class StanfordCoreNLPWrapper(private val props: Properties) extends Serializable {
  @transient private var coreNLP: StanfordCoreNLP = _

  /** Returns the contained [[StanfordCoreNLP]] instance. */
  def get: StanfordCoreNLP = {
    if (coreNLP == null) {
      coreNLP = new StanfordCoreNLP(props)
    }
    coreNLP
  }
}

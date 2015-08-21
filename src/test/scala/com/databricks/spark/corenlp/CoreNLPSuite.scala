package com.databricks.spark.corenlp

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SQLContext}

class CoreNLPSuite extends FunSuite with BeforeAndAfterAll {

  @transient var sc: SparkContext = _
  @transient var sqlContext: SQLContext = _

  override def beforeAll(): Unit = {
    sc = new SparkContext("local[2]", "CoreNLPSuite")
    sqlContext = new SQLContext(sc)
  }

  override def afterAll(): Unit = {
    sc.stop()
    sc = null
    sqlContext = null
  }

  test("CoreNLP") {
    // This example is taken from Stanford CoreNLP website.
    val input = sqlContext.createDataFrame(Seq(
      (1, "<xml>Stanford University is located in California. It is a great university.</xml>")
    )).toDF("id", "text")
    val coreNLP = new CoreNLP()
      .setInputCol("text")
      .setAnnotators(Array("tokenize", "cleanxml", "ssplit"))
      .setFlattenNestedFields(Array("sentence_token_word", "sentence_characterOffsetBegin"))
      .setOutputCol("parsed")
    val parsed = coreNLP.transform(input)
    val first = parsed.first().getAs[Row]("parsed")

    val words = first.getAs[Seq[Row]]("sentence").map(
      _.getAs[Seq[Row]]("token").map(
        _.getAs[String]("word")))
    val expected = Seq(
      Seq("Stanford", "University", "is", "located", "in", "California", "."),
      Seq("It", "is", "a", "great", "university", "."))
    assert(words === expected)

    val flattenedWords = first.getAs[Seq[String]]("sentence_token_word")
    assert(flattenedWords === expected.flatten)

    val flattenedSentenceOffsets = first.getAs[Seq[Int]]("sentence_characterOffsetBegin")
    assert(flattenedSentenceOffsets === Seq(5, 51))
  }
}

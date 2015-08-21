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
    val input = sqlContext.createDataFrame(Seq(
      (1, "<xml>Stanford University is located in California. It is a great university.</xml>")
    )).toDF("id", "text")
    val coreNLP = new CoreNLP()
      .setInputCol("text")
      .setAnnotators(Array("tokenize", "cleanxml", "ssplit"))
      .setOutputCol("parsed")
    val parsed = coreNLP.transform(input)
    parsed.printSchema()
    val first = parsed.first()
    val words = first.getAs[Row]("parsed")
      .getAs[Seq[Row]]("sentence").map(
        _.getAs[Seq[Row]]("token").map(
          _.getAs[String]("word")))
    val expected = Seq(
      Seq("Stanford", "University", "is", "located", "in", "California", "."),
      Seq("It", "is", "a", "great", "university", "."))
    assert(words === expected)
  }
}

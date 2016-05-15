package com.databricks.spark.corenlp

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

trait SparkFunSuite extends FunSuite with BeforeAndAfterAll {
  @transient var sc: SparkContext = _
  @transient var sqlContext: SQLContext = _

  override def beforeAll(): Unit = {
    sc = SparkContext.getOrCreate(
      new SparkConf()
        .setMaster("local[2]")
        .setAppName(this.getClass.getSimpleName)
    )
    sqlContext = SQLContext.getOrCreate(sc)
  }

  override def afterAll(): Unit = {
    sc.stop()
    sc = null
    sqlContext = null
  }
}

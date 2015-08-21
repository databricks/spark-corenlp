package com.databricks.spark.corenlp

import java.{lang => jl, util => ju}
import java.util.{Properties, UUID}

import scala.collection.JavaConverters._

import com.google.protobuf.{ByteString, MessageOrBuilder}
import com.google.protobuf.Descriptors.{Descriptor, EnumValueDescriptor, FieldDescriptor}
import edu.stanford.nlp.pipeline._

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{callUDF, col}
import org.apache.spark.sql.types._

class CoreNLP(override val uid: String) extends Transformer {

  def this() = this("corenlp_" + UUID.randomUUID().toString.takeRight(12))

  val inputCol: Param[String] = new Param(this, "inputCol", "input column name")

  def getInputCol: String = $(inputCol)

  def setInputCol(value: String): this.type = set(inputCol, value)

  val outputCol: Param[String] = new Param(this, "outputCol", "output column name")

  def getOutputCol: String = $(outputCol)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  val annotators: Param[Array[String]] =
    new Param(this, "annotators", "a list of CoreNLP annotators")

  def getAnnotators: Array[String] = $(annotators)

  def setAnnotators(value: Array[String]): this.type = set(annotators, value)

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transform(dataset: DataFrame): DataFrame = {
    val props = new Properties()
    props.setProperty(annotators.name, $(annotators).mkString(","))
    val f = { text: String =>
      val coreNLP = new StanfordCoreNLP(props)
      val doc = new Annotation(text)
      coreNLP.annotate(doc)
      val serializer = new ProtobufAnnotationSerializer()
      CoreNLP.convertMessage(serializer.toProto(doc), CoreNLP.docSchema)
    }
    dataset.withColumn($(outputCol), callUDF(f, CoreNLP.docSchema, col($(inputCol))))
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    new StructType(schema.fields :+ new StructField($(outputCol), CoreNLP.docSchema))
  }
}

object CoreNLP {

  lazy val docSchema: StructType = {
    val schema = parseDescriptor(CoreNLPProtos.Document.getDescriptor, DEFAULT_DEPTH)
    schema.printTreeString()
    schema
  }

  private val DEFAULT_DEPTH = 2

  private def parseDescriptor(desc: Descriptor, depth: Int): StructType = {
    StructType(desc.getFields.asScala.map(parseFieldDescriptor(_, depth)))
  }

  private def parseFieldDescriptor(desc: FieldDescriptor, depth: Int): StructField = {
    import com.google.protobuf.Descriptors.FieldDescriptor.JavaType._
    val dataType = desc.getJavaType match {
      case INT => IntegerType
      case LONG => LongType
      case FLOAT => FloatType
      case DOUBLE => DoubleType
      case BOOLEAN => BooleanType
      case STRING => StringType
      case BYTE_STRING => BinaryType
      case ENUM => StringType
      case MESSAGE =>
        if (depth == 0) {
          NullType
        } else {
          parseDescriptor(desc.getMessageType, depth - 1)
        }
    }
    StructField(desc.getName, if (desc.isRepeated) ArrayType(dataType) else dataType)
  }

  def convertMessage(msg: MessageOrBuilder, schema: StructType): Row = {
    val values = msg.getDescriptorForType.getFields.asScala.view.zip(schema.fields).map {
      case (desc, field) =>
        assert(desc.getName == field.name)
        convert(msg.getField(desc), field.dataType)
    }
    Row(values: _*)
  }

  def convert(any: Any, dataType: DataType): Any = {
    (any, dataType) match {
      case (_, NullType) => null
      case (x: jl.Integer, IntegerType) => x
      case (x: jl.Float, FloatType) => x
      case (x: jl.Double, DoubleType) => x
      case (x: jl.Boolean, BooleanType) => x
      case (x: String, StringType) => x
      case (x: ByteString, BinaryType) => x.toByteArray
      case (x: EnumValueDescriptor, StringType) => x.getName
      case (x: ju.List[_], ArrayType(elementType, _)) =>
        x.asScala.map(convert(_, elementType)).toSeq
      case (x: MessageOrBuilder, schema: StructType) =>
        convertMessage(x, schema)
    }
  }
}

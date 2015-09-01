package com.databricks.spark.corenlp

import java.{lang => jl, util => ju}
import java.util.{Properties, UUID}

import scala.annotation.tailrec
import scala.collection.JavaConverters._

import com.google.protobuf.{ByteString, MessageOrBuilder}
import com.google.protobuf.Descriptors.{Descriptor, EnumValueDescriptor, FieldDescriptor}
import edu.stanford.nlp.pipeline._

import org.apache.spark.Logging
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{callUDF, col}
import org.apache.spark.sql.types._

/**
 * A Stanford CoreNLP wrapper for Spark ML pipeline API.
 * It reads a string column representing documents, and applies CoreNLP annotators to each document.
 * The output column is a nested column with schema mapped from the CoreNLP Document proto
 * ([[com.databricks.spark.corenlp.CoreNLP$.docSchema]]).
 * Further pruning and filtering could be done via SQL statements.
 * This requires Java 8 and CoreNLP version > 3.5.2 (not yet released) to run.
 * Users must include CoreNLP model jars as dependencies to use language models.
 * @see [[http://nlp.stanford.edu/software/corenlp.shtml Stanford CoreNLP]]
 */
class CoreNLP(override val uid: String) extends Transformer {

  import CoreNLP._

  def this() = this("corenlp_" + UUID.randomUUID().toString.takeRight(12))

  val inputCol: Param[String] = new Param(this, "inputCol", "input column name")

  def getInputCol: String = $(inputCol)

  def setInputCol(value: String): this.type = set(inputCol, value)

  val outputCol: Param[String] = new Param(this, "outputCol", "output column name")

  def getOutputCol: String = $(outputCol)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  val annotators: Param[Array[String]] =
    new Param(this, "annotators", "a list of Stanford CoreNLP annotators")

  def getAnnotators: Array[String] = $(annotators)

  def setAnnotators(value: Array[String]): this.type = set(annotators, value)

  def flattenNestedFields: Param[Array[String]] =
    new Param(this, "flattenNestedFields",
      "a list of nested fields delimited by `_` to be flattened under the output column, e.g., " +
        "`sentence_token_word` extracts all token words as an array field under the output column")

  def getFlattenNestedFields: Array[String] = $(flattenNestedFields)

  def setFlattenNestedFields(value: Array[String]): this.type = set(flattenNestedFields, value)

  setDefault(flattenNestedFields -> Array.empty)

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transform(dataset: DataFrame): DataFrame = {
    val props = new Properties()
    props.setProperty(annotators.name, $(annotators).mkString(","))
    val wrapper = new StanfordCoreNLPWrapper(props)
    val f = { text: String =>
      val doc = new Annotation(text)
      wrapper.get.annotate(doc)
      val serializer = new ProtobufAnnotationSerializer()
      val row = CoreNLP.convertMessage(serializer.toProto(doc), docSchema)
      Row(row.toSeq ++ $(flattenNestedFields).map(flatten(row, docSchema, _)): _*)
    }
    dataset.withColumn($(outputCol), callUDF(f, outputSchema, col($(inputCol))))
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    new StructType(schema.fields :+ new StructField($(outputCol), outputSchema))
  }

  private def outputSchema: StructType = {
    val flattenedStructFields = $(flattenNestedFields).map { f =>
      CoreNLP.flattenStructField(docSchema, f)
    }
    StructType(docSchema.fields ++ flattenedStructFields)
  }
}

object CoreNLP extends Logging {

  /**
   * Spark SQL schema mapped from Stanford CoreNLP Document proto with depth limited to 2.
   */
  lazy val docSchema: StructType = {
    val schema = parseDescriptor(CoreNLPProtos.Document.getDescriptor, DEFAULT_DEPTH)
    logInfo(s"CoreNLP Document schema:\ns ${schema.treeString}")
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

  private def convertMessage(msg: MessageOrBuilder, schema: StructType): Row = {
    val values = msg.getDescriptorForType.getFields.asScala.view.zip(schema.fields).map {
      case (desc, field) =>
        assert(desc.getName == field.name)
        convert(msg.getField(desc), field.dataType)
    }
    Row(values: _*)
  }

  private def convert(any: Any, dataType: DataType): Any = {
    (any, dataType) match {
      case (null, _) => null
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

  private def flattenStructField(dataType: DataType, fields: String): StructField = {
    val elementType = extractElementType(dataType, fields.split("_").toList)
    StructField(fields, ArrayType(elementType))
  }

  @tailrec
  private def extractElementType(dataType: DataType, fields: List[String]): DataType = {
    (dataType, fields) match {
      case (_, Nil) => dataType
      case (structType: StructType, field :: tail) =>
        extractElementType(structType(field).dataType, tail)
      case (arrayType: ArrayType, _) =>
        extractElementType(arrayType.elementType, fields)
    }
  }

  private def flatten(any: Any, dataType: DataType, fields: String): Seq[Any] = {
    flatten(any, dataType, fields.split("_").toList)
  }

  private def flatten(any: Any, dataType: DataType, fields: List[String]): Seq[Any] = {
    (any, dataType, fields) match {
      case (seq: Seq[_], arrayType: ArrayType, _) =>
        seq.flatMap(flatten(_, arrayType.elementType, fields))
      case (_, _, Nil) =>
        Seq(any)
      case (row: Row, structType: StructType, field :: tail) =>
        flatten(row.getAs(structType.fieldIndex(field)), structType(field).dataType, tail)
    }
  }
}

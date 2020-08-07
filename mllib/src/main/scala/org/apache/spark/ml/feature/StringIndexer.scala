/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.ml.feature

import org.apache.hadoop.fs.Path

import org.apache.spark.SparkException
import org.apache.spark.annotation.Since
import org.apache.spark.ml.{Estimator, Model, Transformer}
import org.apache.spark.ml.attribute.{Attribute, NominalAttribute}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasHandleInvalid, HasInputCol, HasOutputCol}
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.util.ThreadUtils
import org.apache.spark.util.VersionUtils.majorMinorVersion
import org.apache.spark.util.collection.OpenHashMap

/**
 * Base trait for [[StringIndexer]] and [[StringIndexerModel]].
 */
private[feature] trait StringIndexerBase extends Params with HasHandleInvalid with HasInputCol
  with HasOutputCol {

  /**
   * Param for how to handle invalid data (unseen labels or NULL values).
   * Options are 'skip' (filter out rows with invalid data),
   * 'error' (throw an error), or 'keep' (put invalid data in a special additional
   * bucket, at index numLabels).
   * Default: "error"
   * @group param
   */
  @Since("1.6.0")
  override val handleInvalid: Param[String] = new Param[String](this, "handleInvalid",
    "How to handle invalid data (unseen labels or NULL values). " +
    "Options are 'skip' (filter out rows with invalid data), error (throw an error), " +
    "or 'keep' (put invalid data in a special additional bucket, at index numLabels).",
    ParamValidators.inArray(StringIndexer.supportedHandleInvalids))

  setDefault(handleInvalid, StringIndexer.ERROR_INVALID)

  /**
   * Param for how to order labels of string column. The first label after ordering is assigned
   * an index of 0.
   * Options are:
   *   - 'frequencyDesc': descending order by label frequency (most frequent label assigned 0)
   *   - 'frequencyAsc': ascending order by label frequency (least frequent label assigned 0)
   *   - 'alphabetDesc': descending alphabetical order
   *   - 'alphabetAsc': ascending alphabetical order
   * Default is 'frequencyDesc'.
   *
   * @group param
   */
  @Since("2.3.0")
  final val stringOrderType: Param[String] = new Param(this, "stringOrderType",
    "How to order labels of string column. " +
    "The first label after ordering is assigned an index of 0. " +
    s"Supported options: ${StringIndexer.supportedStringOrderType.mkString(", ")}.",
    ParamValidators.inArray(StringIndexer.supportedStringOrderType))

  /** @group getParam */
  @Since("2.3.0")
  def getStringOrderType: String = $(stringOrderType)

  /** Validates and transforms the input schema. */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    val inputColName = $(inputCol)
    val inputDataType = schema(inputColName).dataType
    require(inputDataType == StringType || inputDataType.isInstanceOf[NumericType],
      s"The input column $inputColName must be either string type or numeric type, " +
        s"but got $inputDataType.")
    val inputFields = schema.fields
    val outputColName = $(outputCol)
    require(inputFields.forall(_.name != outputColName),
      s"Output column $outputColName already exists.")
    NominalAttribute.defaultAttr.withName(outputColName).toStructField()
  }

  /** Validates and transforms the input schema. */
  protected def validateAndTransformSchema(
      schema: StructType,
      skipNonExistsCol: Boolean = false): StructType = {
    val (inputColNames, outputColNames) = getInOutCols()

    require(outputColNames.distinct.length == outputColNames.length,
      s"Output columns should not be duplicate.")

    val outputFields = inputColNames.zip(outputColNames).flatMap {
      case (inputColName, outputColName) =>
        schema.fieldNames.contains(inputColName) match {
          case true => Some(validateAndTransformField(schema, inputColName, outputColName))
          case false if skipNonExistsCol => None
          case _ => throw new SparkException(s"Input column $inputColName does not exist.")
        }
    }
    StructType(schema.fields ++ outputFields)
  }
}

/**
 * A label indexer that maps a string column of labels to an ML column of label indices.
 * If the input column is numeric, we cast it to string and index the string values.
 * The indices are in [0, numLabels). By default, this is ordered by label frequencies
 * so the most frequent label gets index 0. The ordering behavior is controlled by
 * setting `stringOrderType`.
 *
 * @see `IndexToString` for the inverse transformation
 */
@Since("1.4.0")
class StringIndexer @Since("1.4.0") (
    @Since("1.4.0") override val uid: String) extends Estimator[StringIndexerModel]
  with StringIndexerBase with DefaultParamsWritable {

  @Since("1.4.0")
  def this() = this(Identifiable.randomUID("strIdx"))

  /** @group setParam */
  @Since("1.6.0")
  def setHandleInvalid(value: String): this.type = set(handleInvalid, value)

  /** @group setParam */
  @Since("2.3.0")
  def setStringOrderType(value: String): this.type = set(stringOrderType, value)
  setDefault(stringOrderType, StringIndexer.frequencyDesc)

  /** @group setParam */
  @Since("1.4.0")
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  @Since("1.4.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  @Since("3.0.0")
  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  /** @group setParam */
  @Since("3.0.0")
  def setOutputCols(value: Array[String]): this.type = set(outputCols, value)

  /**
   * Gets columns from dataset. If a column is not string type, we replace NaN values
   * with null. Columns are casted to string type.
   */
  private def getSelectedCols(dataset: Dataset[_], inputCols: Seq[String]): Seq[Column] = {
    inputCols.map { colName =>
      val col = dataset.col(colName)
      if (col.expr.dataType == StringType) {
        col
      } else {
        // We don't count for NaN values. Because `StringIndexerAggregator` only processes strings,
        // we replace NaNs with null in advance.
        new Column(If(col.isNaN.expr, Literal(null), col.expr)).cast(StringType)
      }
    }
  }

  private def countByValue(
      dataset: Dataset[_],
      inputCols: Array[String]): Array[OpenHashMap[String, Long]] = {

    val aggregator = new StringIndexerAggregator(inputCols.length)
    implicit val encoder = Encoders.kryo[Array[OpenHashMap[String, Long]]]

    val selectedCols = getSelectedCols(dataset, inputCols)
    dataset.select(selectedCols: _*)
      .toDF
      .groupBy().agg(aggregator.toColumn)
      .as[Array[OpenHashMap[String, Long]]]
      .collect()(0)
  }

  private def sortByFreq(dataset: Dataset[_], ascending: Boolean): Array[Array[String]] = {
    val (inputCols, _) = getInOutCols()

    val sortFunc = StringIndexer.getSortFunc(ascending = ascending)
    val orgStrings = countByValue(dataset, inputCols).toSeq
    ThreadUtils.parmap(orgStrings, "sortingStringLabels", 8) { counts =>
      counts.toSeq.sortWith(sortFunc).map(_._1).toArray
    }.toArray
  }

  private def sortByAlphabet(dataset: Dataset[_], ascending: Boolean): Array[Array[String]] = {
    val (inputCols, _) = getInOutCols()

    val selectedCols = getSelectedCols(dataset, inputCols).map(collect_set(_))
    val allLabels = dataset.select(selectedCols: _*)
      .collect().toSeq.flatMap(_.toSeq).asInstanceOf[Seq[Seq[String]]]
    ThreadUtils.parmap(allLabels, "sortingStringLabels", 8) { labels =>
      val sorted = labels.filter(_ != null).sorted
      if (ascending) {
        sorted.toArray
      } else {
        sorted.reverse.toArray
      }
    }.toArray
  }

  @Since("2.0.0")
  override def fit(dataset: Dataset[_]): StringIndexerModel = {
    transformSchema(dataset.schema, logging = true)

    // In case of equal frequency when frequencyDesc/Asc, the strings are further sorted
    // alphabetically.
    val labelsArray = $(stringOrderType) match {
      case StringIndexer.frequencyDesc => sortByFreq(dataset, ascending = false)
      case StringIndexer.frequencyAsc => sortByFreq(dataset, ascending = true)
      case StringIndexer.alphabetDesc => sortByAlphabet(dataset, ascending = false)
      case StringIndexer.alphabetAsc => sortByAlphabet(dataset, ascending = true)
     }
    copyValues(new StringIndexerModel(uid, labelsArray).setParent(this))
  }

  @Since("1.4.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  @Since("1.4.1")
  override def copy(extra: ParamMap): StringIndexer = defaultCopy(extra)
}

@Since("1.6.0")
object StringIndexer extends DefaultParamsReadable[StringIndexer] {
  private[feature] val SKIP_INVALID: String = "skip"
  private[feature] val ERROR_INVALID: String = "error"
  private[feature] val KEEP_INVALID: String = "keep"
  private[feature] val supportedHandleInvalids: Array[String] =
    Array(SKIP_INVALID, ERROR_INVALID, KEEP_INVALID)
  private[feature] val frequencyDesc: String = "frequencyDesc"
  private[feature] val frequencyAsc: String = "frequencyAsc"
  private[feature] val alphabetDesc: String = "alphabetDesc"
  private[feature] val alphabetAsc: String = "alphabetAsc"
  private[feature] val supportedStringOrderType: Array[String] =
    Array(frequencyDesc, frequencyAsc, alphabetDesc, alphabetAsc)

  @Since("1.6.0")
  override def load(path: String): StringIndexer = super.load(path)
}

/**
 * Model fitted by [[StringIndexer]].
 *
 * @param labels  Ordered list of labels, corresponding to indices to be assigned.
 *
 * @note During transformation, if the input column does not exist,
 * `StringIndexerModel.transform` would return the input dataset unmodified.
 * This is a temporary fix for the case when target labels do not exist during prediction.
 */
@Since("1.4.0")
class StringIndexerModel (
    @Since("1.4.0") override val uid: String,
    @Since("1.5.0") val labels: Array[String])
  extends Model[StringIndexerModel] with StringIndexerBase with MLWritable {

  import StringIndexerModel._

  @Since("1.5.0")
  def this(labels: Array[String]) = this(Identifiable.randomUID("strIdx"), labels)

  private val labelToIndex: OpenHashMap[String, Double] = {
    val n = labels.length
    val map = new OpenHashMap[String, Double](n)
    var i = 0
    while (i < n) {
      map.update(labels(i), i)
      i += 1
    }
    map
  }

  /** @group setParam */
  @Since("1.6.0")
  def setHandleInvalid(value: String): this.type = set(handleInvalid, value)

  /** @group setParam */
  @Since("1.4.0")
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  @Since("1.4.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  @Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    if (!dataset.schema.fieldNames.contains($(inputCol))) {
      logInfo(s"Input column ${$(inputCol)} does not exist during transformation. " +
        "Skip StringIndexerModel.")
      return dataset.toDF
    }
    transformSchema(dataset.schema, logging = true)

    val filteredLabels = getHandleInvalid match {
      case StringIndexer.KEEP_INVALID => labels :+ "__unknown"
      case _ => labels
    }

    val metadata = NominalAttribute.defaultAttr
      .withName($(outputCol)).withValues(filteredLabels).toMetadata()
    // If we are skipping invalid records, filter them out.
    val (filteredDataset, keepInvalid) = $(handleInvalid) match {
      case StringIndexer.SKIP_INVALID =>
        val filterer = udf { label: String =>
          labelToIndex.contains(label)
        }
        (dataset.na.drop(Array($(inputCol))).where(filterer(dataset($(inputCol)))), false)
      case _ => (dataset, getHandleInvalid == StringIndexer.KEEP_INVALID)
    }

    val indexer = udf { label: String =>
      if (label == null) {
        if (keepInvalid) {
          labels.length
        } else {
          throw new SparkException("StringIndexer encountered NULL value. To handle or skip " +
            "NULLS, try setting StringIndexer.handleInvalid.")
        }
      } else {
        if (labelToIndex.contains(label)) {
          labelToIndex(label)
        } else if (keepInvalid) {
          labels.length
        } else {
          throw new SparkException(s"Unseen label: $label.  To handle unseen labels, " +
            s"set Param handleInvalid to ${StringIndexer.KEEP_INVALID}.")
        }
      }
    }.asNondeterministic()
  }

  @Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)

    val (inputColNames, outputColNames) = getInOutCols()
    val outputColumns = new Array[Column](outputColNames.length)

    // Skips invalid rows if `handleInvalid` is set to `StringIndexer.SKIP_INVALID`.
    val filteredDataset = if (getHandleInvalid == StringIndexer.SKIP_INVALID) {
      filterInvalidData(dataset, inputColNames)
    } else {
      dataset
    }

    for (i <- 0 until outputColNames.length) {
      val inputColName = inputColNames(i)
      val outputColName = outputColNames(i)
      val labelToIndex = labelsToIndexArray(i)
      val labels = labelsArray(i)

      if (!dataset.schema.fieldNames.contains(inputColName)) {
        logWarning(s"Input column ${inputColName} does not exist during transformation. " +
          "Skip StringIndexerModel for this column.")
        outputColNames(i) = null
      } else {
        val filteredLabels = getHandleInvalid match {
          case StringIndexer.KEEP_INVALID => labels :+ "__unknown"
          case _ => labels
        }
        val metadata = NominalAttribute.defaultAttr
          .withName(outputColName)
          .withValues(filteredLabels)
          .toMetadata()

        val indexer = getIndexer(labels, labelToIndex)

        outputColumns(i) = indexer(dataset(inputColName).cast(StringType))
          .as(outputColName, metadata)
      }
    }

    filteredDataset.select(col("*"),
      indexer(dataset($(inputCol)).cast(StringType)).as($(outputCol), metadata))
  }

  @Since("1.4.0")
  override def transformSchema(schema: StructType): StructType = {
    if (schema.fieldNames.contains($(inputCol))) {
      validateAndTransformSchema(schema)
    } else {
      // If the input column does not exist during transformation, we skip StringIndexerModel.
      schema
    }
  }

  @Since("1.4.1")
  override def copy(extra: ParamMap): StringIndexerModel = {
    val copied = new StringIndexerModel(uid, labels)
    copyValues(copied, extra).setParent(parent)
  }

  @Since("1.6.0")
  override def write: StringIndexModelWriter = new StringIndexModelWriter(this)

  @Since("3.0.0")
  override def toString: String = {
    s"StringIndexerModel: uid=$uid, handleInvalid=${$(handleInvalid)}" +
      get(stringOrderType).map(t => s", stringOrderType=$t").getOrElse("") +
      get(inputCols).map(c => s", numInputCols=${c.length}").getOrElse("") +
      get(outputCols).map(c => s", numOutputCols=${c.length}").getOrElse("")
  }
}

@Since("1.6.0")
object StringIndexerModel extends MLReadable[StringIndexerModel] {

  private[StringIndexerModel]
  class StringIndexModelWriter(instance: StringIndexerModel) extends MLWriter {

    private case class Data(labels: Array[String])

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.labels)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class StringIndexerModelReader extends MLReader[StringIndexerModel] {

    private val className = classOf[StringIndexerModel].getName

    override def load(path: String): StringIndexerModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath)
        .select("labels")
        .head()
      val labels = data.getAs[Seq[String]](0).toArray
      val model = new StringIndexerModel(metadata.uid, labels)
      metadata.getAndSetParams(model)
      model
    }
  }

  @Since("1.6.0")
  override def read: MLReader[StringIndexerModel] = new StringIndexerModelReader

  @Since("1.6.0")
  override def load(path: String): StringIndexerModel = super.load(path)
}

/**
 * A `Transformer` that maps a column of indices back to a new column of corresponding
 * string values.
 * The index-string mapping is either from the ML attributes of the input column,
 * or from user-supplied labels (which take precedence over ML attributes).
 *
 * @see `StringIndexer` for converting strings into indices
 */
@Since("1.5.0")
class IndexToString @Since("2.2.0") (@Since("1.5.0") override val uid: String)
  extends Transformer with HasInputCol with HasOutputCol with DefaultParamsWritable {

  @Since("1.5.0")
  def this() =
    this(Identifiable.randomUID("idxToStr"))

  /** @group setParam */
  @Since("1.5.0")
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setLabels(value: Array[String]): this.type = set(labels, value)

  /**
   * Optional param for array of labels specifying index-string mapping.
   *
   * Default: Not specified, in which case [[inputCol]] metadata is used for labels.
   * @group param
   */
  @Since("1.5.0")
  final val labels: StringArrayParam = new StringArrayParam(this, "labels",
    "Optional array of labels specifying index-string mapping." +
      " If not provided or if empty, then metadata from inputCol is used instead.")

  /** @group getParam */
  @Since("1.5.0")
  final def getLabels: Array[String] = $(labels)

  @Since("1.5.0")
  override def transformSchema(schema: StructType): StructType = {
    val inputColName = $(inputCol)
    val inputDataType = schema(inputColName).dataType
    require(inputDataType.isInstanceOf[NumericType],
      s"The input column $inputColName must be a numeric type, " +
        s"but got $inputDataType.")
    val inputFields = schema.fields
    val outputColName = $(outputCol)
    require(inputFields.forall(_.name != outputColName),
      s"Output column $outputColName already exists.")
    val outputFields = inputFields :+ StructField($(outputCol), StringType)
    StructType(outputFields)
  }

  @Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    val inputColSchema = dataset.schema($(inputCol))
    // If the labels array is empty use column metadata
    val values = if (!isDefined(labels) || $(labels).isEmpty) {
      Attribute.fromStructField(inputColSchema)
        .asInstanceOf[NominalAttribute].values.get
    } else {
      $(labels)
    }
    val indexer = udf { index: Double =>
      val idx = index.toInt
      if (0 <= idx && idx < values.length) {
        values(idx)
      } else {
        throw new SparkException(s"Unseen index: $index ??")
      }
    }
    val outputColName = $(outputCol)
    dataset.select(col("*"),
      indexer(dataset($(inputCol)).cast(DoubleType)).as(outputColName))
  }

  @Since("1.5.0")
  override def copy(extra: ParamMap): IndexToString = {
    defaultCopy(extra)
  }
}

@Since("1.6.0")
object IndexToString extends DefaultParamsReadable[IndexToString] {

  @Since("1.6.0")
  override def load(path: String): IndexToString = super.load(path)
}

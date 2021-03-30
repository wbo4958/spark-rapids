package com.nvidia.spark.rapids

import java.io.{DataOutputStream, File, IOException}
import java.net.{URI, URISyntaxException}
import java.nio.ByteBuffer
import java.nio.channels.{Channels, WritableByteChannel}
import java.util
import java.util.concurrent.Callable
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, Queue}
import ai.rapids.cudf.{HostMemoryBuffer, NvtxColor, NvtxRange}
import com.google.protobuf.CodedOutputStream
import com.nvidia.spark.rapids.OrcUtilsNew.{OrcOutputStripeNew, OrcPartitionReaderContextNew}
import com.nvidia.spark.rapids.RapidsPluginImplicits.AutoCloseableColumn
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.common.io.DiskRangeList
import org.apache.orc.{DataReader, OrcConf, OrcFile, OrcProto, PhysicalWriter, Reader, StripeInformation, TypeDescription}
import org.apache.orc.impl.{BufferChunk, DataReaderProperties, OrcCodecPool, OutStream, RecordReaderImpl, RecordReaderUtils, SchemaEvolution}
import org.apache.orc.impl.RecordReaderImpl.SargApplier
import org.apache.orc.mapred.OrcInputFormat
import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.execution.datasources.v2.EmptyPartitionReader
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.OrcFilters
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

import scala.collection.immutable.HashSet




object OrcUtilsNew {
  case class OrcOutputStripeNew(
    infoBuilder: OrcProto.StripeInformation.Builder,
    footer: OrcProto.StripeFooter,
    inputDataRanges: DiskRangeList)

  case class OrcPartitionReaderContextNew(
    updatedReadSchema: TypeDescription,
    evolution: SchemaEvolution,
    dataReader: DataReader,
    orcReader: Reader,
    blockIterator: BufferedIterator[OrcOutputStripeNew],
    requestedMapping: Option[Array[Int]])
}

class GpuOrcPartitionReaderUtils(
    conf: Configuration,
    partFile: PartitionedFile,
    orcFileReaderOpts: OrcFile.ReaderOptions,
    orcReader: Reader,
    readerOpts: Reader.Options,
    dataReader: DataReader,
    requestedMapping: Option[Array[Int]],
  ) extends Arm {

  // These streams are not copied to the GPU since they are only used for filtering.
  // Filtering is already being performed as the ORC memory file is built.
  private val ORC_STREAM_KINDS_IGNORED = util.EnumSet.of(
    OrcProto.Stream.Kind.BLOOM_FILTER,
    OrcProto.Stream.Kind.BLOOM_FILTER_UTF8,
    OrcProto.Stream.Kind.ROW_INDEX)

  def getOrcPartitionReaderContext(): OrcPartitionReaderContextNew = {

    closeOnExcept(orcReader) { _ =>
      val updatedReadSchema = checkSchemaCompatibility(orcReader.getSchema, readerOpts.getSchema,
        readerOpts.getIsSchemaEvolutionCaseAware)
      val evolution = new SchemaEvolution(orcReader.getSchema, readerOpts.getSchema, readerOpts)
      val (sargApp, sargColumns) = getSearchApplier(evolution, orcFileReaderOpts.getUseUTCTimestamp)
      val splitStripes = orcReader.getStripes.asScala.filter(s =>
        s.getOffset >= partFile.start && s.getOffset < partFile.start + partFile.length)
      val stripes = buildOutputStripes(splitStripes, evolution,
        sargApp, sargColumns, OrcConf.IGNORE_NON_UTF8_BLOOM_FILTERS.getBoolean(conf),
        orcReader.getWriterVersion)
      OrcPartitionReaderContextNew(updatedReadSchema, evolution, dataReader, orcReader,
        stripes.iterator.buffered, requestedMapping)
    }
  }

  /**
   * Check if the read schema is compatible with the file schema.
   *
   * @param fileSchema input file's ORC schema
   * @param readSchema ORC schema for what will be read
   * @param isCaseAware true if field names are case-sensitive
   * @return read schema mapped to the file's field names
   */
  private def checkSchemaCompatibility(
    fileSchema: TypeDescription,
    readSchema: TypeDescription,
    isCaseAware: Boolean): TypeDescription = {
    val fileFieldNames = fileSchema.getFieldNames.asScala
    val fileChildren = fileSchema.getChildren.asScala
    val caseSensitiveFileTypes = fileFieldNames.zip(fileChildren.zip(fileFieldNames)).toMap
    val fileTypesMap = if (isCaseAware) {
      caseSensitiveFileTypes
    } else {
      CaseInsensitiveMap[(TypeDescription, String)](caseSensitiveFileTypes)
    }

    val readerFieldNames = readSchema.getFieldNames.asScala
    val readerChildren = readSchema.getChildren.asScala
    val newReadSchema = TypeDescription.createStruct()
    readerFieldNames.zip(readerChildren).foreach { case (readField, readType) =>
      val (fileType, fileFieldName) = fileTypesMap.getOrElse(readField, (null, null))
      if (readType != fileType) {
        throw new QueryExecutionException("Incompatible schemas for ORC file" +
          s" at ${partFile.filePath}\n" +
          s" file schema: $fileSchema\n" +
          s" read schema: $readSchema")
      }
      newReadSchema.addField(fileFieldName, fileType)
    }

    newReadSchema
  }

  /**
   * Build an ORC search argument applier that can filter input file splits
   * when predicate push-down filters have been specified.
   *
   * @param evolution ORC SchemaEvolution
   * @param useUTCTimestamp true if timestamps are UTC
   * @return the search argument applier and search argument column mapping
   */
  private def getSearchApplier(
    evolution: SchemaEvolution,
    useUTCTimestamp: Boolean): (SargApplier, Array[Boolean]) = {
    val searchArg = readerOpts.getSearchArgument
    if (searchArg != null && orcReader.getRowIndexStride != 0) {
      val sa = new SargApplier(searchArg, orcReader.getRowIndexStride, evolution,
        orcReader.getWriterVersion, useUTCTimestamp)
      // SargApplier.sargColumns is unfortunately not visible so we redundantly compute it here.
      val filterCols = RecordReaderImpl.mapSargColumnsToOrcInternalColIdx(searchArg.getLeaves,
        evolution)
      val saCols = new Array[Boolean](evolution.getFileIncluded.length)
      filterCols.foreach { i =>
        if (i > 0) {
          saCols(i) = true
        }
      }
      (sa, saCols)
    } else {
      (null, null)
    }
  }

  /**
   * Build the output stripe descriptors for what will appear in the ORC memory file.
   *
   * @param stripes descriptors for the ORC input stripes, filtered to what is in the split
   * @param evolution ORC SchemaEvolution
   * @param sargApp ORC search argument applier
   * @param sargColumns mapping of ORC search argument columns
   * @param ignoreNonUtf8BloomFilter true if bloom filters other than UTF8 should be ignored
   * @param writerVersion writer version from the original ORC input file
   * @return output stripes descriptors
   */
  private def buildOutputStripes(
    stripes: Seq[StripeInformation],
    evolution: SchemaEvolution,
    sargApp: SargApplier,
    sargColumns: Array[Boolean],
    ignoreNonUtf8BloomFilter: Boolean,
    writerVersion: OrcFile.WriterVersion): Seq[OrcOutputStripeNew] = {
    val fileIncluded = calcOrcFileIncluded(evolution)
    val columnMapping = columnRemap(fileIncluded)
    val result = new ArrayBuffer[OrcOutputStripeNew](stripes.length)
    stripes.foreach { stripe =>
      val stripeFooter = dataReader.readStripeFooter(stripe)
      val needStripe = if (sargApp != null) {
        // An ORC schema is a single struct type describing the schema fields
        val orcFileSchema = evolution.getFileType(0)
        val orcIndex = dataReader.readRowIndex(stripe, orcFileSchema, stripeFooter,
          ignoreNonUtf8BloomFilter, fileIncluded, null, sargColumns,
          writerVersion, null, null)
        val rowGroups = sargApp.pickRowGroups(stripe, orcIndex.getRowGroupIndex,
          orcIndex.getBloomFilterKinds, stripeFooter.getColumnsList, orcIndex.getBloomFilterIndex,
          true)
        rowGroups != SargApplier.READ_NO_RGS
      } else {
        true
      }

      if (needStripe) {
        result.append(buildOutputStripe(stripe, stripeFooter, columnMapping))
      }
    }

    result
  }

  /**
   * Build the output stripe descriptor for a corresponding input stripe
   * that should be copied to the ORC memory file.
   *
   * @param inputStripe input stripe descriptor
   * @param inputFooter input stripe footer
   * @param columnMapping mapping of input column IDs to output column IDs
   * @return output stripe descriptor
   */
  private def buildOutputStripe(
    inputStripe: StripeInformation,
    inputFooter: OrcProto.StripeFooter,
    columnMapping: Array[Int]): OrcOutputStripeNew = {
    val rangeCreator = new DiskRangeList.CreateHelper
    val footerBuilder = OrcProto.StripeFooter.newBuilder()
    var inputFileOffset = inputStripe.getOffset
    var outputStripeDataLength = 0L

    // copy stream descriptors for columns that are requested
    inputFooter.getStreamsList.asScala.foreach { stream =>
      val streamEndOffset = inputFileOffset + stream.getLength

      if (stream.hasKind && stream.hasColumn) {
        val outputColumn = columnMapping(stream.getColumn)
        val wantKind = !ORC_STREAM_KINDS_IGNORED.contains(stream.getKind)
        if (outputColumn >= 0 && wantKind) {
          // remap the column ID when copying the stream descriptor
          footerBuilder.addStreams(OrcProto.Stream.newBuilder(stream).setColumn(outputColumn).build)
          outputStripeDataLength += stream.getLength
          rangeCreator.addOrMerge(inputFileOffset, streamEndOffset, true, true)
        }
      }

      inputFileOffset = streamEndOffset
    }

    // add the column encodings that are relevant
    for (i <- 0 until inputFooter.getColumnsCount) {
      if (columnMapping(i) >= 0) {
        footerBuilder.addColumns(inputFooter.getColumns(i))
      }
    }

    // copy over the timezone
    if (inputFooter.hasWriterTimezone) {
      footerBuilder.setWriterTimezoneBytes(inputFooter.getWriterTimezoneBytes)
    }

    val outputStripeFooter = footerBuilder.build()

    // Fill out everything for StripeInformation except the file offset and footer length
    // which will be calculated when the stripe data is finally written.
    val infoBuilder = OrcProto.StripeInformation.newBuilder()
      .setIndexLength(0)
      .setDataLength(outputStripeDataLength)
      .setNumberOfRows(inputStripe.getNumberOfRows)

    OrcOutputStripeNew(infoBuilder, outputStripeFooter, rangeCreator.get)
  }

  /**
   * Compute an array of booleans, one for each column in the ORC file, indicating whether the
   * corresponding ORC column ID should be included in the file to be loaded by the GPU.
   *
   * @param evolution ORC schema evolution instance
   * @return per-column inclusion flags
   */
  private def calcOrcFileIncluded(evolution: SchemaEvolution): Array[Boolean] = {
    if (requestedMapping.isDefined) {
      // ORC schema has no column names, so need to filter based on index
      val orcSchema = orcReader.getSchema
      val topFields = orcSchema.getChildren
      val numFlattenedCols = orcSchema.getMaximumId
      val included = new Array[Boolean](numFlattenedCols + 1)
      util.Arrays.fill(included, false)
      // first column is the top-level schema struct, always add it
      included(0) = true
      // find each top-level column requested by top-level index and add it and all child columns
      requestedMapping.get.foreach { colIdx =>
        val field = topFields.get(colIdx)
        (field.getId to field.getMaximumId).foreach { i =>
          included(i) = true
        }
      }
      included
    } else {
      evolution.getFileIncluded
    }
  }

  /**
   * Build an integer array that maps the original ORC file's column IDs
   * to column IDs in the memory file. Columns that are not present in
   * the memory file will have a mapping of -1.
   *
   * @param fileIncluded indicator per column in the ORC file whether it should be included
   * @return column mapping array
   */
  private def columnRemap(fileIncluded: Array[Boolean]): Array[Int] = {
    var nextOutputColumnId = 0
    val result = new Array[Int](fileIncluded.length)
    fileIncluded.indices.foreach { i =>
      if (fileIncluded(i)) {
        result(i) = nextOutputColumnId
        nextOutputColumnId += 1
      } else {
        result(i) = -1
      }
    }
    result
  }
}

private case class GpuOrcFileFilterHandler(
    @transient sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    pushedFilters: Array[Filter]) extends Arm {

  val isCaseSensitive = sqlConf.caseSensitiveAnalysis

  def filterStripes(
      partFile: PartitionedFile,
      dataSchema: StructType,
      readDataSchema: StructType,
      partitionSchema: StructType): OrcPartitionReaderContextNew = {

    val conf = broadcastedConf.value.value
    OrcConf.IS_SCHEMA_EVOLUTION_CASE_SENSITIVE.setBoolean(conf, isCaseSensitive)

    val filePath = new Path(new URI(partFile.filePath))
    val fs = filePath.getFileSystem(conf)

    val orcFileReaderOpts = OrcFile.readerOptions(conf).filesystem(fs)

    closeOnExcept(OrcFile.createReader(filePath, orcFileReaderOpts)) { orcReader =>
      val resultedColPruneInfo = GpuOrcPartitionReaderFactory.requestedColumnIds(
        isCaseSensitive, dataSchema, readDataSchema, orcReader)
      if (resultedColPruneInfo.isEmpty) {
        orcReader.close()
        throw new IOException(s"${partFile.filePath} is empty")
      } else {
        val (requestedColIds, canPruneCols) = resultedColPruneInfo.get
        GpuOrcPartitionReaderFactory.orcResultSchemaString(canPruneCols, dataSchema, readDataSchema,
          partitionSchema, conf)
        assert(requestedColIds.length == readDataSchema.length,
          "[BUG] requested column IDs do not match required schema")
        // Only need to filter ORC's schema evolution if it cannot prune directly
        val requestedMapping = if (canPruneCols) {
          None
        } else {
          Some(requestedColIds)
        }
        val fullSchema = StructType(dataSchema ++ partitionSchema)
        val readerOpts = buildOrcReaderOpts(conf, orcReader, partFile, fullSchema)
        val dataReader = buildDataReader(orcReader, readerOpts, filePath, fs, conf)

        new GpuOrcPartitionReaderUtils(conf, partFile,
          orcFileReaderOpts, orcReader, readerOpts, dataReader, requestedMapping)
          .getOrcPartitionReaderContext()
      }
    }
  }

  private def buildOrcReaderOpts(
    conf: Configuration,
    orcReader: Reader,
    partFile: PartitionedFile,
    fullSchema: StructType): Reader.Options = {
    val readerOpts = OrcInputFormat.buildOptions(
      conf, orcReader, partFile.start, partFile.length)
    // create the search argument if we have pushed filters
    OrcFilters.createFilter(fullSchema, pushedFilters).foreach { f =>
      readerOpts.searchArgument(f, fullSchema.fieldNames)
    }
    readerOpts
  }

  private def buildDataReader(
    orcReader: Reader,
    readerOpts: Reader.Options,
    filePath: Path,
    fs: FileSystem,
    conf: Configuration): DataReader = {
    if (readerOpts.getDataReader != null) {
      readerOpts.getDataReader
    } else {
      val zeroCopy: Boolean = if (readerOpts.getUseZeroCopy != null) {
        readerOpts.getUseZeroCopy
      } else {
        OrcConf.USE_ZEROCOPY.getBoolean(conf)
      }
      val maxDiskRangeChunkLimit = OrcConf.ORC_MAX_DISK_RANGE_CHUNK_LIMIT.getInt(conf)
      //noinspection ScalaDeprecation
      RecordReaderUtils.createDefaultDataReader(DataReaderProperties.builder()
        .withBufferSize(orcReader.getCompressionSize)
        .withCompression(orcReader.getCompressionKind)
        .withFileSystem(fs)
        .withPath(filePath)
        .withTypeCount(org.apache.orc.OrcUtils.getOrcTypes(orcReader.getSchema).size)
        .withZeroCopy(zeroCopy)
        .withMaxDiskRangeChunkLimit(maxDiskRangeChunkLimit)
        .build())
    }
  }

}

class MultiFileCloudOrcPartitionReader(
    @transient sqlConf: SQLConf,
    conf: Configuration,
    files: Array[PartitionedFile],
    broadcastedConf: Broadcast[SerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    debugDumpPrefix: String,
    maxReadBatchSizeRows: Integer,
    maxReadBatchSizeBytes: Long,
    execMetrics: Map[String, GpuMetric],
    partitionSchema: StructType,
    numThreads: Int,
    maxNumFileProcessed: Int,
    filters: Array[Filter],
    fileHandler: GpuOrcFileFilterHandler)
  extends MultiFileCloudPartitionReaderBase(
    conf, files, numThreads, maxNumFileProcessed, filters, execMetrics) {

  private class OrcReadBatchRunner(
      partFile: PartitionedFile,
      conf: Configuration,
      filters: Array[Filter]) extends Callable[HostMemoryBuffersWithMetaDataBase] with Logging {

    private var blockChunkIter: BufferedIterator[OrcOutputStripeNew] = null

    /**
     * Returns the host memory buffers and file meta data for the file processed.
     * If there was an error then the error field is set. If there were no blocks the buffer
     * is returned as null.  If there were no columns but rows (count() operation) then the
     * buffer is null and the size is the number of rows.
     *
     * Note that the TaskContext is not set in these threads and should not be used.
     */
    override def call(): HostMemoryBuffersWithMetaDataBase = {

      val startingBytesRead = fileSystemBytesRead()

      val hostBuffers = new ArrayBuffer[(HostMemoryBuffer, Long)]
      try {
        val ctx = fileHandler.filterStripes(partFile, dataSchema, readDataSchema, partitionSchema)

        // TODO need to fix this
//        if (ctx.blockIterator.size == 0) {
//          val bytesRead = fileSystemBytesRead() - startingBytesRead
//          // no blocks so return null buffer and size 0
//          return HostMemoryBuffersForOrc(partFile.partitionValues, Array((null, 0)),
//            partFile.filePath, partFile.start, partFile.length, bytesRead)
//        }

        blockChunkIter = ctx.blockIterator.buffered

        if (isDone) {
          val bytesRead = fileSystemBytesRead() - startingBytesRead
          // got close before finishing
          HostMemoryBuffersForOrc(partFile.partitionValues, Array((null, 0)),
            partFile.filePath, partFile.start, partFile.length, bytesRead)
        } else {
          if (readDataSchema.isEmpty) {
            val bytesRead = fileSystemBytesRead() - startingBytesRead
            val numRows = ctx.blockIterator.map(_.infoBuilder.getNumberOfRows).sum.toInt
            // overload size to be number of rows with null buffer
            HostMemoryBuffersForOrc(partFile.partitionValues, Array((null, numRows)),
              partFile.filePath, partFile.start, partFile.length, bytesRead)
          } else {
            val filePath = new Path(new URI(partFile.filePath))
            while (blockChunkIter.hasNext) {
              val blocksToRead = populateCurrentBlockChunk(blockChunkIter)
              hostBuffers += readPartFile(ctx, blocksToRead)
            }
            val bytesRead = fileSystemBytesRead() - startingBytesRead
            if (isDone) {
              // got close before finishing
              hostBuffers.foreach(_._1.safeClose())
              HostMemoryBuffersForOrc(partFile.partitionValues, Array((null, 0)),
                partFile.filePath, partFile.start, partFile.length, bytesRead)
            } else {
              HostMemoryBuffersForOrc(partFile.partitionValues, hostBuffers.toArray,
                partFile.filePath, partFile.start, partFile.length, bytesRead)
            }
          }
        }
      } catch {
        case e: Throwable =>
          hostBuffers.foreach(_._1.safeClose())
          throw e
      }
    }
  }

  override def getBatchRunner(
    file: PartitionedFile,
    conf: Configuration,
    filters: Array[Filter]): Callable[HostMemoryBuffersWithMetaDataBase] = {
    new OrcReadBatchRunner(file, conf, filters)
  }

  override def readBatch(
    fileBufsAndMeta: HostMemoryBuffersWithMetaDataBase): Option[ColumnarBatch] = {
    fileBufsAndMeta match {
      case buffer: HostMemoryBuffersForOrc => null
      case _ => throw new Exception("never reach here")
    }
  }


  private def readPartFile(
      ctx: OrcPartitionReaderContextNew,
      stripes: Seq[OrcOutputStripeNew]): (HostMemoryBuffer, Long) = {
    withResource(new NvtxWithMetrics("Buffer file split", NvtxColor.YELLOW,
      metrics("bufferTime"))) { _ =>
      if (stripes.isEmpty) {
        return (null, 0L)
      }

      val hostBufferSize = estimateOutputSize(ctx, stripes)
      var succeeded = false
      val hmb = HostMemoryBuffer.allocate(hostBufferSize)
      try {
        val out = new HostMemoryOutputStream(hmb)
        writeOrcOutputFile(ctx, out, stripes)
        succeeded = true
        (hmb, out.getPos)
      } finally {
        if (!succeeded) {
          hmb.close()
        }
      }
    }
  }

  private def estimateOutputSize(ctx: OrcPartitionReaderContextNew,
      stripes: Seq[OrcOutputStripeNew]): Long = {
    // start with header magic
    var size: Long = OrcFile.MAGIC.length

    // account for the size of every stripe
    stripes.foreach { stripe =>
      size += stripe.infoBuilder.getIndexLength + stripe.infoBuilder.getDataLength
      // The true footer length is unknown since it may be compressed.
      // Use the uncompressed size as an upper bound.
      size += stripe.footer.getSerializedSize
    }

    // the original file's footer and postscript should be worst-case
    size += ctx.orcReader.getFileTail.getPostscript.getFooterLength
    size += ctx.orcReader.getFileTail.getPostscriptLength

    // and finally the single-byte postscript length at the end of the file
    size += 1

    // Add in a bit of fudging in case the whole file is being consumed and
    // our codec version isn't as efficient as the original writer's codec.
    size + 128 * 1024
  }

  private def writeOrcOutputFile(
    ctx: OrcPartitionReaderContextNew,
    rawOut: HostMemoryOutputStream,
    stripes: Seq[OrcOutputStripeNew]): Unit = {
    val outChannel = Channels.newChannel(rawOut)
    val outReceiver = new PhysicalWriter.OutputReceiver {
      override def output(buffer: ByteBuffer): Unit = outChannel.write(buffer)
      override def suppress(): Unit = throw new UnsupportedOperationException(
        "suppress should not be called")
    }

    // write ORC header
    val dataOut = new DataOutputStream(rawOut)
    dataOut.writeBytes(OrcFile.MAGIC)
    dataOut.flush()

    val codec = OrcCodecPool.getCodec(ctx.orcReader.getCompressionKind)
    try {

      // buffer size must be greater than zero or writes hang (ORC-381)
      val orcBufferSize = if (ctx.orcReader.getCompressionSize > 0) {
        ctx.orcReader.getCompressionSize
      } else {
        // note that this buffer is just for writing meta-data
        OrcConf.BUFFER_SIZE.getDefaultValue.asInstanceOf[Int]
      }

      val codecStream = new OutStream(getClass.getSimpleName, orcBufferSize,
        codec, outReceiver)
      val protoWriter = CodedOutputStream.newInstance(codecStream)
      var numRows = 0L
      val fileFooterBuilder = OrcProto.Footer.newBuilder

      // write the stripes
      stripes.foreach { stripe =>
        stripe.infoBuilder.setOffset(rawOut.getPos)
        copyStripeData(ctx, outChannel, stripe.inputDataRanges)
        val stripeFooterStartOffset = rawOut.getPos
        stripe.footer.writeTo(protoWriter)
        protoWriter.flush()
        codecStream.flush()
        stripe.infoBuilder.setFooterLength(rawOut.getPos - stripeFooterStartOffset)
        fileFooterBuilder.addStripes(stripe.infoBuilder.build())
        numRows += stripe.infoBuilder.getNumberOfRows
      }

      // write the footer
      val footer = fileFooterBuilder.setHeaderLength(OrcFile.MAGIC.length)
        .setContentLength(rawOut.getPos)
        .addAllTypes(org.apache.orc.OrcUtils.getOrcTypes(buildReaderSchema(ctx)))
        .setNumberOfRows(numRows)
        .build()
      val footerStartOffset = rawOut.getPos
      footer.writeTo(protoWriter)
      protoWriter.flush()
      codecStream.flush()
      val postScriptStartOffset = rawOut.getPos

      // write the postscript (uncompressed)
      val postscript = OrcProto.PostScript.newBuilder(ctx.orcReader.getFileTail.getPostscript)
        .setFooterLength(postScriptStartOffset - footerStartOffset)
        .setMetadataLength(0)
        .build()
      postscript.writeTo(rawOut)
      val postScriptLength = rawOut.getPos - postScriptStartOffset
      if (postScriptLength > 255) {
        throw new IllegalArgumentException(s"PostScript length is too large at $postScriptLength")
      }
      rawOut.write(postScriptLength.toInt)
    } finally {
      OrcCodecPool.returnCodec(ctx.orcReader.getCompressionKind, codec)
    }
  }

  /** Get the ORC schema corresponding to the file being constructed for the GPU */
  private def buildReaderSchema(ctx: OrcPartitionReaderContextNew): TypeDescription = {
    if (ctx.requestedMapping.isDefined) {
      // filter top-level schema based on requested mapping
      val orcSchema = ctx.orcReader.getSchema
      val orcSchemaNames = orcSchema.getFieldNames
      val orcSchemaChildren = orcSchema.getChildren
      val readerSchema = TypeDescription.createStruct()
      ctx.requestedMapping.get.foreach { orcColIdx =>
        val fieldName = orcSchemaNames.get(orcColIdx)
        val fieldType = orcSchemaChildren.get(orcColIdx)
        readerSchema.addField(fieldName, fieldType.clone())
      }
      readerSchema
    } else {
      ctx.evolution.getReaderSchema
    }
  }

  private def copyStripeData(
    ctx: OrcPartitionReaderContextNew,
    out: WritableByteChannel,
    inputDataRanges: DiskRangeList): Unit = {
    val bufferChunks = ctx.dataReader.readFileData(inputDataRanges, 0, false)
    var current = bufferChunks
    while (current != null) {
      out.write(current.getData)
      if (ctx.dataReader.isTrackingDiskRanges && current.isInstanceOf[BufferChunk]) {
        ctx.dataReader.releaseBuffer(current.asInstanceOf[BufferChunk].getChunk)
      }
      current = current.next
    }
  }

  private def populateCurrentBlockChunk(
    blockIter: BufferedIterator[OrcOutputStripeNew]): Seq[OrcOutputStripeNew] = {
    val currentChunk = new ArrayBuffer[OrcOutputStripeNew]

    var numRows: Long = 0
    var numBytes: Long = 0
    var numOrcBytes: Long = 0

    @tailrec
    def readNextBatch(): Unit = {
      if (blockIter.hasNext) {
        val peekedStripe = blockIter.head
        if (peekedStripe.infoBuilder.getNumberOfRows > Integer.MAX_VALUE) {
          throw new UnsupportedOperationException("Too many rows in split")
        }
        if (numRows == 0 ||
          numRows + peekedStripe.infoBuilder.getNumberOfRows <= maxReadBatchSizeRows) {
          val estimatedBytes = GpuBatchUtils.estimateGpuMemory(readDataSchema,
            peekedStripe.infoBuilder.getNumberOfRows)
          if (numBytes == 0 || numBytes + estimatedBytes <= maxReadBatchSizeBytes) {
            currentChunk += blockIter.next()
            numRows += currentChunk.last.infoBuilder.getNumberOfRows
            numOrcBytes += currentChunk.last.infoBuilder.getDataLength
            numBytes += estimatedBytes
            readNextBatch()
          }
        }
      }
    }

    readNextBatch()

    logDebug(s"Loaded $numRows rows from Orc. Orc bytes read: $numOrcBytes. " +
      s"Estimated GPU bytes: $numBytes")

    currentChunk
  }
}

case class GpuOrcMultiFilePartitionReaderFactory(
  @transient sqlConf: SQLConf,
  broadcastedConf: Broadcast[SerializableConfiguration],
  dataSchema: StructType,
  readDataSchema: StructType,
  partitionSchema: StructType,
  filters: Array[Filter],
  @transient rapidsConf: RapidsConf,
  metrics: Map[String, GpuMetric],
  queryUsesInputFile: Boolean) extends PartitionReaderFactory with Arm with Logging {
  private val isCaseSensitive = sqlConf.caseSensitiveAnalysis
  private val debugDumpPrefix = rapidsConf.parquetDebugDumpPrefix
  private val maxReadBatchSizeRows = rapidsConf.maxReadBatchSizeRows
  private val maxReadBatchSizeBytes = rapidsConf.maxReadBatchSizeBytes
  private val numThreads = rapidsConf.parquetMultiThreadReadNumThreads
  private val maxNumFileProcessed = rapidsConf.maxNumParquetFilesParallel
  private val canUseMultiThreadReader = rapidsConf.isParquetMultiThreadReadEnabled
  // we can't use the coalescing files reader when InputFileName, InputFileBlockStart,
  // or InputFileBlockLength because we are combining all the files into a single buffer
  // and we don't know which file is associated with each row.
  private val canUseCoalesceFilesReader =
  rapidsConf.isParquetCoalesceFileReadEnabled && !queryUsesInputFile

  private val configCloudSchemes = rapidsConf.getCloudSchemes
  private val CLOUD_SCHEMES = HashSet("dbfs", "s3", "s3a", "s3n", "wasbs", "gs")
  private val allCloudSchemes = CLOUD_SCHEMES ++ configCloudSchemes.getOrElse(Seq.empty)

  private val filterHandler = GpuOrcFileFilterHandler(sqlConf, broadcastedConf, filters)

  override def supportColumnarReads(partition: InputPartition): Boolean = true

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    throw new IllegalStateException("GPU column parser called to read rows")
  }

  private def resolveURI(path: String): URI = {
    try {
      val uri = new URI(path)
      if (uri.getScheme() != null) {
        return uri
      }
    } catch {
      case e: URISyntaxException =>
    }
    new File(path).getAbsoluteFile().toURI()
  }

  // We expect the filePath here to always have a scheme on it,
  // if it doesn't we try using the local filesystem. If that
  // doesn't work for some reason user would need to configure
  // it directly.
  private def isCloudFileSystem(filePath: String): Boolean = {
    val uri = resolveURI(filePath)
    val scheme = uri.getScheme
    if (allCloudSchemes.contains(scheme)) {
      true
    } else {
      false
    }
  }

  private def arePathsInCloud(filePaths: Array[String]): Boolean = {
    filePaths.exists(isCloudFileSystem)
  }

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    assert(partition.isInstanceOf[FilePartition])
    val filePartition = partition.asInstanceOf[FilePartition]
    val files = filePartition.files
    val filePaths = files.map(_.filePath)
    val conf = broadcastedConf.value.value
    buildBaseColumnarParquetReaderForCloud(files, conf)
  }

  private def buildBaseColumnarParquetReaderForCloud(
    files: Array[PartitionedFile],
    conf: Configuration): PartitionReader[ColumnarBatch] = {

    val conf = broadcastedConf.value.value
    OrcConf.IS_SCHEMA_EVOLUTION_CASE_SENSITIVE.setBoolean(conf, isCaseSensitive)

    new MultiFileCloudOrcPartitionReader(sqlConf, conf, files, broadcastedConf,
      dataSchema, readDataSchema, debugDumpPrefix, maxReadBatchSizeRows, maxReadBatchSizeBytes,
      metrics, partitionSchema, numThreads, maxNumFileProcessed, filters, filterHandler)
  }
//
//  private def buildBaseColumnarParquetReader(
////    files: Array[PartitionedFile]): PartitionReader[ColumnarBatch] = {
////    val conf = broadcastedConf.value.value
////    val clippedBlocks = ArrayBuffer[ParquetFileInfoWithSingleBlockMeta]()
////    files.map { file =>
////      val singleFileInfo = filterHandler.filterBlocks(file, conf, filters, readDataSchema)
////      clippedBlocks ++= singleFileInfo.blocks.map(
////        ParquetFileInfoWithSingleBlockMeta(singleFileInfo.filePath, _, file.partitionValues,
////          singleFileInfo.schema, singleFileInfo.isCorrectedRebaseMode))
////    }
//
//    new MultiFileParquetPartitionReader(conf, files, clippedBlocks,
//      isCaseSensitive, readDataSchema, debugDumpPrefix,
//      maxReadBatchSizeRows, maxReadBatchSizeBytes, metrics,
//      partitionSchema, numThreads)
//  }
}

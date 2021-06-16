/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nvidia.spark.rapids

import java.util.concurrent.{Callable, ConcurrentLinkedQueue, Future, LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, LinkedHashMap, Queue}
import scala.math.max

import ai.rapids.cudf.{ColumnVector, HostMemoryBuffer, NvtxColor, NvtxRange, Table}
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.nvidia.spark.rapids.GpuMetric.{NUM_OUTPUT_BATCHES, PEAK_DEVICE_MEMORY}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.hadoop.metadata.BlockMetaData

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.rapids.InputFileUtils
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * The base HostMemoryBuffer information read from a single file.
 */
trait HostMemoryBuffersWithMetaDataBase {
  // PartitionedFile to be read
  def partitionedFile: PartitionedFile
  // An array of BlockChunk(HostMemoryBuffer and its data size) read from PartitionedFile
  def memBuffersAndSizes: Array[(HostMemoryBuffer, Long)]
  // Total bytes read
  def bytesRead: Long
}

// This is a common trait for all kind of file formats
trait MultiFileReaderFunctions extends Arm {

  // Add partitioned columns into the batch
  protected def addPartitionValues(
      batch: Option[ColumnarBatch],
      inPartitionValues: InternalRow,
      partitionSchema: StructType): Option[ColumnarBatch] = {
    if (partitionSchema.nonEmpty) {
      batch.map { cb =>
        val partitionValues = inPartitionValues.toSeq(partitionSchema)
        val partitionScalars = ColumnarPartitionReaderWithPartitionValues
          .createPartitionValues(partitionValues, partitionSchema)
        withResource(partitionScalars) { scalars =>
          ColumnarPartitionReaderWithPartitionValues.addPartitionValues(cb, scalars,
            GpuColumnVector.extractTypes(partitionSchema))
        }
      }
    } else {
      batch
    }
  }

  protected def fileSystemBytesRead(): Long = {
    FileSystem.getAllStatistics.asScala.map(_.getThreadStatistics.getBytesRead).sum
  }
}

// A tool to create a ThreadPoolExecutor
// Please note that the TaskContext is not set in these threads and should not be used.
object MultiFileThreadPoolUtil {

  def createThreadPool(
      threadTag: String,
      maxThreads: Int = 20,
      keepAliveSeconds: Long = 60): ThreadPoolExecutor = {
    val threadFactory = new ThreadFactoryBuilder()
      .setNameFormat(threadTag + " reader worker-%d")
      .setDaemon(true)
      .build()

    val threadPoolExecutor = new ThreadPoolExecutor(
      maxThreads, // corePoolSize: max number of threads to create before queuing the tasks
      maxThreads, // maximumPoolSize: because we use LinkedBlockingDeque, this is not used
      keepAliveSeconds,
      TimeUnit.SECONDS,
      new LinkedBlockingQueue[Runnable],
      threadFactory)
    threadPoolExecutor.allowCoreThreadTimeOut(true)
    threadPoolExecutor
  }
}

/**
 * The Abstract multi-file cloud reading framework
 *
 * The data driven:
 * next() -> if (first time) initAndStartReaders -> submit tasks (getBatchRunner)
 *        -> wait tasks done sequentially -> decode in GPU (readBatch)
 *
 * @param conf Configuration parameters
 * @param files PartitionFiles to be read
 * @param numThreads the number of threads to read files parallelly.
 * @param maxNumFileProcessed threshold to control the maximum file number to be
 *                            submitted to threadpool
 * @param filters push down filters
 * @param execMetrics the metrics
 */
abstract class MultiFileCloudPartitionReaderBase(
    conf: Configuration,
    files: Array[PartitionedFile],
    numThreads: Int,
    maxNumFileProcessed: Int,
    filters: Array[Filter],
    execMetrics: Map[String, GpuMetric]) extends PartitionReader[ColumnarBatch] with Logging
  with ScanWithMetrics with Arm {

  metrics = execMetrics

  protected var maxDeviceMemory: Long = 0

  protected var batch: Option[ColumnarBatch] = None
  protected var isDone: Boolean = false

  private var filesToRead = 0
  protected var currentFileHostBuffers: Option[HostMemoryBuffersWithMetaDataBase] = None
  private var isInitted = false
  private val tasks = new ConcurrentLinkedQueue[Future[HostMemoryBuffersWithMetaDataBase]]()
  private val tasksToRun = new Queue[Callable[HostMemoryBuffersWithMetaDataBase]]()
  private[this] val inputMetrics = TaskContext.get.taskMetrics().inputMetrics

  private def initAndStartReaders(): Unit = {
    // limit the number we submit at once according to the config if set
    val limit = math.min(maxNumFileProcessed, files.length)
    for (i <- 0 until limit) {
      val file = files(i)
      // Add these in the order as we got them so that we can make sure
      // we process them in the same order as CPU would.
      tasks.add(getThreadPool(numThreads).submit(getBatchRunner(file, conf, filters)))
    }
    // queue up any left to add once others finish
    for (i <- limit until files.length) {
      val file = files(i)
      tasksToRun.enqueue(getBatchRunner(file, conf, filters))
    }
    isInitted = true
    filesToRead = files.length
  }

  /**
   * The sub-class must implement the real file reading logic in a Callable
   * which will be running in a thread pool
   *
   * @param file file to be read
   * @param conf the Configuration parameters
   * @param filters push down filters
   * @return Callable[HostMemoryBuffersWithMetaDataBase]
   */
  def getBatchRunner(
    file: PartitionedFile,
    conf: Configuration,
    filters: Array[Filter]): Callable[HostMemoryBuffersWithMetaDataBase]

  /**
   * Get ThreadPoolExecutor to run the Callable.
   *
   * The requirements:
   * 1. Same ThreadPoolExecutor for cloud and coalescing for the same file format
   * 2. Different file formats have different ThreadPoolExecutors
   *
   * @param numThreads  max number of threads to create
   * @return ThreadPoolExecutors
   */
  def getThreadPool(numThreads: Int): ThreadPoolExecutor

  /**
   * Decode HostMemoryBuffers in GPU
   * @param fileBufsAndMeta the file HostMemoryBuffer read from a PartitionedFile
   * @return Option[ColumnarBatch] which has been decoded by GPU
   */
  def readBatch(
    fileBufsAndMeta: HostMemoryBuffersWithMetaDataBase): Option[ColumnarBatch]

  /**
   * File format short name used for logging and other things to uniquely identity
   * which file format is being used.
   *
   * @return the file format short name
   */
  def getFileFormatShortName: String

  override def next(): Boolean = {
    withResource(new NvtxRange(getFileFormatShortName + " readBatch", NvtxColor.GREEN)) { _ =>
      if (isInitted == false) {
        initAndStartReaders()
      }
      batch.foreach(_.close())
      batch = None
      // if we have batch left from the last file read return it
      if (currentFileHostBuffers.isDefined) {
        if (getSizeOfHostBuffers(currentFileHostBuffers.get) == 0) {
          next()
        }
        batch = readBatch(currentFileHostBuffers.get)
      } else {
        currentFileHostBuffers = None
        if (filesToRead > 0 && !isDone) {
          val fileBufsAndMeta = tasks.poll.get()
          filesToRead -= 1
          TrampolineUtil.incBytesRead(inputMetrics, fileBufsAndMeta.bytesRead)
          InputFileUtils.setInputFileBlock(
            fileBufsAndMeta.partitionedFile.filePath,
            fileBufsAndMeta.partitionedFile.start,
            fileBufsAndMeta.partitionedFile.length)

          if (getSizeOfHostBuffers(fileBufsAndMeta) == 0) {
            // if sizes are 0 means no rows and no data so skip to next file
            // file data was empty so submit another task if any were waiting
            addNextTaskIfNeeded()
            next()
          } else {
            batch = readBatch(fileBufsAndMeta)
            // the data is copied to GPU so submit another task if we were limited
            addNextTaskIfNeeded()
          }
        } else {
          isDone = true
          metrics(PEAK_DEVICE_MEMORY) += maxDeviceMemory
        }
      }
    }

    // this shouldn't happen but if somehow the batch is None and we still
    // have work left skip to the next file
    if (batch.isEmpty && filesToRead > 0 && !isDone) {
      next()
    }

    // This is odd, but some operators return data even when there is no input so we need to
    // be sure that we grab the GPU
    GpuSemaphore.acquireIfNecessary(TaskContext.get())
    batch.isDefined
  }

  override def get(): ColumnarBatch = {
    val ret = batch.getOrElse(throw new NoSuchElementException)
    batch = None
    ret
  }

  private def getSizeOfHostBuffers(fileInfo: HostMemoryBuffersWithMetaDataBase): Long = {
    fileInfo.memBuffersAndSizes.map(_._2).sum
  }

  private def addNextTaskIfNeeded(): Unit = {
    if (tasksToRun.nonEmpty && !isDone) {
      val runner = tasksToRun.dequeue()
      tasks.add(getThreadPool(numThreads).submit(runner))
    }
  }

  override def close(): Unit = {
    // this is more complicated because threads might still be processing files
    // in cases close got called early for like limit() calls
    isDone = true
    currentFileHostBuffers.foreach { current =>
      current.memBuffersAndSizes.foreach { case (buf, _) =>
        if (buf != null) {
          buf.close()
        }
      }
    }
    currentFileHostBuffers = None
    batch.foreach(_.close())
    batch = None
    tasks.asScala.foreach { task =>
      if (task.isDone()) {
        task.get.memBuffersAndSizes.foreach { case (buf, _) =>
          if (buf != null) {
            buf.close()
          }
        }
      } else {
        // Note we are not interrupting thread here so it
        // will finish reading and then just discard. If we
        // interrupt HDFS logs warnings about being interrupted.
        task.cancel(false)
      }
    }
  }

}

// A trait to describe a data block info
trait DataBlockBase {
  // get the row number of this data block
  def getRowCount: Long
  // uncompressed bytes
  def getTotalUnCompressedByteSize: Long
  // TODO
  def getFileBlockSize: Long
}

// A trait for schema
trait SchemaBase

// A single block info of a file
// eg. a parquet file has 3 RowGroup, then it will produce 3 SingleBlockInfoWithMeta
trait SingleDataBlockInfo {
  def filePath: Path // file path info
  def partitionValues: InternalRow // partition value
  def dataBlock: DataBlockBase // a single block info of a single file
  def isCorrectedRebaseMode: Boolean // TODO is this needed for ORC
  def schema: SchemaBase // schema info
}

abstract class MultiFileCoalescingPartitionReaderBase(
    clippedBlocks: Seq[SingleDataBlockInfo],
    readDataSchema: StructType,
    partitionSchema: StructType,
    maxReadBatchSizeRows: Integer,
    maxReadBatchSizeBytes: Long,
    numThreads: Int,
    execMetrics: Map[String, GpuMetric]) extends PartitionReader[ColumnarBatch] with Logging
  with ScanWithMetrics with Arm with MultiFileReaderFunctions {

  protected var isDone: Boolean = false
  protected var maxDeviceMemory: Long = 0
  protected var batch: Option[ColumnarBatch] = None
  private val blockIterator: BufferedIterator[SingleDataBlockInfo] =
    clippedBlocks.iterator.buffered
  private[this] val inputMetrics = TaskContext.get.taskMetrics().inputMetrics

  metrics = execMetrics

  private case class CurrentChunkMeta(
    isCorrectRebaseMode: Boolean,
    clippedSchema: SchemaBase,
    currentChunk: Seq[(Path, DataBlockBase)],
    numTotalRows: Long,
    rowsPerPartition: Array[Long],
    allPartValues: Array[InternalRow])

  /**
   * To check if the next block will be split into another ColumnarBatch
   * @param currentBlockInfo current SingleDataBlockInfo
   * @param nextBlockInfo    next SingleDataBlockInfo
   * @return Boolean
   */
  def checkIfNeededToSplitDataBlock(
    currentBlockInfo: SingleDataBlockInfo,
    nextBlockInfo: SingleDataBlockInfo): Boolean

  // This method only calculate the total size according to the DataBlockBase not including
  // any magic and footer.
  // then allocate the corresponding host buffer to hold all the blocks

  /**
   * Calculate the output size according to the block chunks and the schema
   * @param currentChunkedBlocks a sequence of data block to be evaluated
   * @param schema Schema info
   * @return Long, the estimated output size
   */
  def calculateEstimatedBlocksOutputSize(
    currentChunkedBlocks: Seq[DataBlockBase],
    schema: SchemaBase): Long

  /**
   * Get ThreadPoolExecutor to run the Callable.
   *
   * The rules:
   * 1. same ThreadPoolExecutor for cloud and coalescing for the same file format
   * 2. different file formats have different ThreadPoolExecutors
   *
   * @return ThreadPoolExecutor
   */
  def getThreadPool(numThreads: Int): ThreadPoolExecutor

  /**
   * The sub-class must implement the real file reading logic in a Callable
   * which will be running in a thread pool
   *
   * @param file   file to be read
   * @param outhmb
   * @param blocks
   * @param offset
   * @return Callable[(Seq[DataBlockBase], Long)]
   */
  def getBatchRunner(
    file: Path,
    outhmb: HostMemoryBuffer,
    blocks: ArrayBuffer[DataBlockBase],
    offset: Long): Callable[(Seq[DataBlockBase], Long)]

  /**
   * File format short name used for logging and other things to uniquely identity
   * which file format is being used.
   *
   * @return the file format short name
   */
  def getFileFormatShortName: String

  /**
   * Sent host memory to GPU to decode
   *
   * Please be note, the dataBuffer only contains the data blocks, not including magic and footer
   *
   * @param blocksBuffer  data block in Host Memory
   * @param blockDataSize    data size
   * @param blocks      all output data blocks
   * @param clippedSchema the clipped schema
   * @param isCorrectRebaseMode specify if need to rebase
   * @return Table
   */
  def readBufferToTable(
    blocksBuffer: HostMemoryBuffer,
    blockDataSize: Long,
    blocks: Seq[DataBlockBase],
    clippedSchema: SchemaBase,
    isCorrectRebaseMode: Boolean): Table

  override def get(): ColumnarBatch = {
    val ret = batch.getOrElse(throw new NoSuchElementException)
    batch = None
    ret
  }

  override def close(): Unit = {
    batch.foreach(_.close())
    batch = None
    isDone = true
  }

  override def next(): Boolean = {
    batch.foreach(_.close())
    batch = None
    if (!isDone) {
      if (!blockIterator.hasNext) {
        isDone = true
        metrics(PEAK_DEVICE_MEMORY) += maxDeviceMemory
      } else {
        batch = readBatch()
      }
    }
    // This is odd, but some operators return data even when there is no input so we need to
    // be sure that we grab the GPU
    GpuSemaphore.acquireIfNecessary(TaskContext.get())
    batch.isDefined
  }

  private def readBatch(): Option[ColumnarBatch] = {
    withResource(new NvtxRange("Parquet readBatch", NvtxColor.GREEN)) { _ =>
      val currentChunkMeta = populateCurrentBlockChunk()
      if (readDataSchema.isEmpty) {
        // not reading any data, so return a degenerate ColumnarBatch with the row count
        if (currentChunkMeta.numTotalRows == 0) {
          None
        } else {
          // Someone is going to process this data, even if it is just a row count
          GpuSemaphore.acquireIfNecessary(TaskContext.get())
          val emptyBatch = new ColumnarBatch(Array.empty, currentChunkMeta.numTotalRows.toInt)
          addAllPartitionValues(Some(emptyBatch), currentChunkMeta.allPartValues,
            currentChunkMeta.rowsPerPartition, partitionSchema)
        }
      } else {
        val table = readToTable(currentChunkMeta.currentChunk, currentChunkMeta.clippedSchema,
          currentChunkMeta.isCorrectRebaseMode)
        try {
          val colTypes = readDataSchema.fields.map(f => f.dataType)
          val maybeBatch = table.map(t => GpuColumnVector.from(t, colTypes))
          maybeBatch.foreach { batch =>
            logDebug(s"GPU batch size: ${GpuColumnVector.getTotalDeviceMemoryUsed(batch)} bytes")
          }
          // we have to add partition values here for this batch, we already verified that
          // its not different for all the blocks in this batch
          addAllPartitionValues(maybeBatch, currentChunkMeta.allPartValues,
            currentChunkMeta.rowsPerPartition, partitionSchema)
        } finally {
          table.foreach(_.close())
        }
      }
    }
  }

  private def readToTable(
      currentChunkedBlocks: Seq[(Path, DataBlockBase)],
      clippedSchema: SchemaBase,
      isCorrectRebaseMode: Boolean = false): Option[Table] = {
    if (currentChunkedBlocks.isEmpty) {
      return None
    }
    val (dataBuffer, dataSize, blocks) = readPartFiles(currentChunkedBlocks, clippedSchema)
    try {
      if (dataSize == 0) {
        None
      } else {
        val table = readBufferToTable(dataBuffer, dataSize, blocks, clippedSchema,
          isCorrectRebaseMode)
        closeOnExcept(table) { _ =>
          maxDeviceMemory = max(GpuColumnVector.getTotalDeviceMemoryUsed(table), maxDeviceMemory)
          if (readDataSchema.length < table.getNumberOfColumns) {
            throw new QueryExecutionException(s"Expected ${readDataSchema.length} columns " +
              s"but read ${table.getNumberOfColumns} from $currentChunkedBlocks")
          }
        }
        metrics(NUM_OUTPUT_BATCHES) += 1
        Some(table)
      }
    } finally {
      dataBuffer.close()
    }
  }

  /**
   * Read all data blocks into HostMemoryBuffer
   * @param blocks a sequence of data blocks to be read
   * @param clippedSchema the clipped schema is used to calculate the estimated output size
   * @return (HostMemoryBuffer, Long, Seq[DataBlockBase])
   *         the HostMemoryBuffer and its data size and all the output blocks
   */
  private def readPartFiles(
      blocks: Seq[(Path, DataBlockBase)],
      clippedSchema: SchemaBase): (HostMemoryBuffer, Long, Seq[DataBlockBase]) = {

    withResource(new NvtxWithMetrics("Buffer file split", NvtxColor.YELLOW,
      metrics("bufferTime"))) { _ =>
      // ugly but we want to keep the order
      val filesAndBlocks = LinkedHashMap[Path, ArrayBuffer[DataBlockBase]]()
      blocks.foreach { case (path, block) =>
        filesAndBlocks.getOrElseUpdate(path, new ArrayBuffer[DataBlockBase]) += block
      }
      val tasks = new java.util.ArrayList[Future[(Seq[DataBlockBase], Long)]]()

      val allBlocks = blocks.map(_._2)

      val initTotalSize = calculateEstimatedBlocksOutputSize(allBlocks, clippedSchema)

      closeOnExcept(HostMemoryBuffer.allocate(initTotalSize)) { allocBuf =>
        val hmb = allocBuf
        val out = new HostMemoryOutputStream(hmb)
        var offset = out.getPos
        val allOutputBlocks = scala.collection.mutable.ArrayBuffer[DataBlockBase]()
        filesAndBlocks.foreach { case (file, blocks) =>
          val fileBlockSize = blocks.map(_.getFileBlockSize).sum
          // use a single buffer and slice it up for different files if we need
          val outLocal = hmb.slice(offset, fileBlockSize)
          // copy the blocks for each file in parallel using background threads
          // copy the blocks for each file in parallel using background threads
          tasks.add(getThreadPool(numThreads).submit(
            getBatchRunner(file, outLocal, blocks, offset)))
          offset += fileBlockSize
        }

        for (future <- tasks.asScala) {
          val (blocks, bytesRead) = future.get()
          allOutputBlocks ++= blocks
          TrampolineUtil.incBytesRead(inputMetrics, bytesRead)
        }

        out.close()
        (hmb, offset, allOutputBlocks)
      }
    }
  }

  /**
   * Populate block chunk with meta info according to maxReadBatchSizeRows
   * and maxReadBatchSizeBytes
   *
   * @return [[CurrentChunkMeta]]
   */
  private def populateCurrentBlockChunk(): CurrentChunkMeta = {
    val currentChunk = new ArrayBuffer[(Path, DataBlockBase)]
    var numRows: Long = 0
    var numBytes: Long = 0
    var numChunkBytes: Long = 0
    var currentFile: Path = null
    var currentPartitionValues: InternalRow = null
    var currentClippedSchema: SchemaBase = null
    var currentIsCorrectRebaseMode: Boolean = false
    val rowsPerPartition = new ArrayBuffer[Long]()
    var lastPartRows: Long = 0
    val allPartValues = new ArrayBuffer[InternalRow]()

    var currrentDataBlock: SingleDataBlockInfo = null

    @tailrec
    def readNextBatch(): Unit = {
      if (blockIterator.hasNext) {
        if (currentFile == null) {
          // first time of readNextBatch
          currrentDataBlock = blockIterator.head
          currentFile = blockIterator.head.filePath
          currentPartitionValues = blockIterator.head.partitionValues
          allPartValues += currentPartitionValues
          currentClippedSchema = blockIterator.head.schema
          currentIsCorrectRebaseMode = blockIterator.head.isCorrectedRebaseMode
        }

        val peekedRowCount = blockIterator.head.dataBlock.getRowCount
        if (peekedRowCount > Integer.MAX_VALUE) {
          throw new UnsupportedOperationException("Too many rows in split")
        }

        if (numRows == 0 || numRows + peekedRowCount <= maxReadBatchSizeRows) {
          val estimatedBytes = GpuBatchUtils.estimateGpuMemory(readDataSchema, peekedRowCount)
          if (numBytes == 0 || numBytes + estimatedBytes <= maxReadBatchSizeBytes) {
            // only care to check if we are actually adding in the next chunk
            if (currentFile != blockIterator.head.filePath) {
              // check if need to split next data block into another ColumnarBatch
              if (checkIfNeededToSplitDataBlock(currrentDataBlock, blockIterator.head)) {
                logInfo(s"splitting ${blockIterator.head.filePath} into another batch!")
                return
              }
              // If the partition values are different we have to track the number of rows that get
              // each partition value and the partition values themselves so that we can build
              // the full columns with different partition values later.
              if (blockIterator.head.partitionValues != currentPartitionValues) {
                rowsPerPartition += (numRows - lastPartRows)
                lastPartRows = numRows
                // we add the actual partition values here but then
                // the number of rows in that partition at the end or
                // when the partition changes
                allPartValues += blockIterator.head.partitionValues
              }
              currentFile = blockIterator.head.filePath
              currentPartitionValues = blockIterator.head.partitionValues
              currentClippedSchema = blockIterator.head.schema
              currrentDataBlock = blockIterator.head
            }

            val nextBlock = blockIterator.next()
            val nextTuple = (nextBlock.filePath, nextBlock.dataBlock)
            currentChunk += nextTuple
            numRows += currentChunk.last._2.getRowCount
            numChunkBytes += currentChunk.last._2.getTotalUnCompressedByteSize
            numBytes += estimatedBytes
            readNextBatch()
          }
        }
      }
    }
    readNextBatch()
    rowsPerPartition += (numRows - lastPartRows)
    logDebug(s"Loaded $numRows rows from ${getFileFormatShortName}. " +
      s"${getFileFormatShortName} bytes read: $numChunkBytes. Estimated GPU bytes: $numBytes. " +
      s"Number of different partitions: ${allPartValues.size}")
    CurrentChunkMeta(currentIsCorrectRebaseMode, currentClippedSchema, currentChunk,
      numRows, rowsPerPartition.toArray, allPartValues.toArray)
  }

  /**
   * Add all partition values found to the batch. There could be more then one partition
   * value in the batch so we have to build up columns with the correct number of rows
   * for each partition value.
   *
   * @param batch - columnar batch to append partition values to
   * @param inPartitionValues - array of partition values
   * @param rowsPerPartition - the number of rows that require each partition value
   * @param partitionSchema - schema of the partitions
   * @return
   */
  private def addAllPartitionValues(
      batch: Option[ColumnarBatch],
      inPartitionValues: Array[InternalRow],
      rowsPerPartition: Array[Long],
      partitionSchema: StructType): Option[ColumnarBatch] = {
    assert(rowsPerPartition.length == inPartitionValues.length)
    if (partitionSchema.nonEmpty) {
      batch.map { cb =>
        val numPartitions = inPartitionValues.length
        if (numPartitions > 1) {
          concatAndAddPartitionColsToBatch(cb, rowsPerPartition, inPartitionValues)
        } else {
          // single partition, add like other readers
          addPartitionValues(Some(cb), inPartitionValues.head, partitionSchema).get
        }
      }
    } else {
      batch
    }
  }

  private def concatAndAddPartitionColsToBatch(
      cb: ColumnarBatch,
      rowsPerPartition: Array[Long],
      inPartitionValues: Array[InternalRow]): ColumnarBatch = {
    withResource(cb) { _ =>
      closeOnExcept(buildAndConcatPartitionColumns(rowsPerPartition, inPartitionValues)) {
        allPartCols =>
          ColumnarPartitionReaderWithPartitionValues.addGpuColumVectorsToBatch(cb, allPartCols)
      }
    }
  }

  private def buildAndConcatPartitionColumns(
      rowsPerPartition: Array[Long],
      inPartitionValues: Array[InternalRow]): Array[GpuColumnVector] = {
    val numCols = partitionSchema.fields.length
    val allPartCols = new Array[GpuColumnVector](numCols)
    // build the partitions vectors for all partitions within each column
    // and concatenate those together then go to the next column
    for ((field, colIndex) <- partitionSchema.fields.zipWithIndex) {
      val dataType = field.dataType
      withResource(new Array[GpuColumnVector](inPartitionValues.length)) {
        partitionColumns =>
          for ((rowsInPart, partIndex) <- rowsPerPartition.zipWithIndex) {
            val partInternalRow = inPartitionValues(partIndex)
            val partValueForCol = partInternalRow.get(colIndex, dataType)
            val partitionScalar = GpuScalar.from(partValueForCol, dataType)
            withResource(partitionScalar) { scalar =>
              partitionColumns(partIndex) = GpuColumnVector.from(
                ai.rapids.cudf.ColumnVector.fromScalar(scalar, rowsInPart.toInt),
                dataType)
            }
          }
          val baseOfCols = partitionColumns.map(_.getBase)
          allPartCols(colIndex) = GpuColumnVector.from(
            ColumnVector.concatenate(baseOfCols: _*), field.dataType)
      }
    }
    allPartCols
  }

}

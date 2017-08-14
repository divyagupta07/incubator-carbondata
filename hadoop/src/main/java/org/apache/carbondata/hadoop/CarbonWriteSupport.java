package org.apache.carbondata.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.block.Distributable;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.processing.csvload.BlockDetails;
import org.apache.carbondata.processing.model.CarbonLoadModel;
import org.apache.carbondata.spark.DataLoadResultImpl;
import org.apache.carbondata.spark.load.CarbonLoaderUtil;
import org.apache.carbondata.spark.rdd.NewCarbonDataLoadRDD;
import org.apache.carbondata.spark.util.CommonUtil;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.execution.command.ExecutionErrors;
import org.apache.spark.sql.hive.DistributionUtil;
import scala.Tuple2;
import scala.collection.JavaConversions;

public class CarbonWriteSupport<T> {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonWriteSupport.class.getName());

  /**
   * Initialization if needed based on the projected column list
   *
   * @param
   * @param absoluteTableIdentifier table identifier
   */
  void initialize(CarbonLoadModel loadModel, AbsoluteTableIdentifier absoluteTableIdentifier)
      throws IOException {
    //if singlepass is true start dictionaryserverClient and generate global dictionary
  }

  /**
   * convert column data back to row representation
   *
   * @param data column data
   */
  void writeRecord(Object[] data) {

  }

  Tuple2<String, Tuple2<LoadMetadataDetails, ExecutionErrors>>[] status = null;

  void loadDataFile(SQLContext sqlContext, CarbonLoadModel loadModel) throws IOException {

    org.apache.hadoop.conf.Configuration hadoopConfiguration =
        new org.apache.hadoop.conf.Configuration(sqlContext.sparkContext().hadoopConfiguration());
    // FileUtils will skip file which is no csv, and return all file path which split by ','
    String filePaths = loadModel.getFactFilePath();
    hadoopConfiguration.set(FileInputFormat.INPUT_DIR, filePaths);
    hadoopConfiguration.set(FileInputFormat.INPUT_DIR_RECURSIVE, "true");
    String someThing =
        "org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.BZip2Codec";
    hadoopConfiguration.set("io.compression.codecs", someThing);

    CommonUtil.configSplitMaxSize(sqlContext.sparkContext(), filePaths, hadoopConfiguration);

    TextInputFormat inputFormat = new org.apache.hadoop.mapreduce.lib.input.TextInputFormat();
    Job jobContext = new Job(hadoopConfiguration);
    Object rawSplits[] = inputFormat.getSplits(jobContext).toArray();

    List<Distributable> blockList = new ArrayList<>();

    for (Object inputSplit : rawSplits) {
      FileSplit fileSplit = (FileSplit) inputSplit;
      Distributable distributable =
          new TableBlockInfo(fileSplit.getPath().toString(), fileSplit.getStart(), "1",
              fileSplit.getLocations(), fileSplit.getLength(), ColumnarFormatVersion.V1, null);
      blockList.add(distributable);
    }

    // group blocks to nodes, tasks
    Long startTime = System.currentTimeMillis();
    List<String> activeNodes = JavaConversions.seqAsJavaList(DistributionUtil
        .ensureExecutorsAndGetNodeList(JavaConversions.asScalaBuffer(blockList).toSeq(),
            sqlContext.sparkContext()));
    Map<String, List<Distributable>> nodeBlockMapping =
        CarbonLoaderUtil.nodeBlockMapping(blockList, -1, activeNodes);

    Long timeElapsed = System.currentTimeMillis() - startTime;
    LOGGER.info("Total Time taken in block allocation: " + timeElapsed);
    LOGGER.info(
        "Total no of blocks: " + blockList.size() + ", No.of Nodes: " + nodeBlockMapping.size());
    String str = "";

    Set<String> nodeBlockMappingKeys = nodeBlockMapping.keySet();

    for (String nodeBlockMappingKey : nodeBlockMappingKeys) {
      List<Distributable> tableBlock = nodeBlockMapping.get(nodeBlockMappingKey);
      str = str + "#Node: " + nodeBlockMappingKey + " no.of.blocks: " + tableBlock.size();
      Boolean exists = false;
      for (Distributable tableBlockInfo : tableBlock) {
        String[] locations = tableBlockInfo.getLocations();
        for (String location : locations) {
          if (location.equalsIgnoreCase(nodeBlockMappingKey)) {
            exists = true;
          }
        }
        if (exists) {
          String locationString = ",";
          for (String location : locations) {
            locationString += location + ",";
          }
          str = str + " , mismatch locations: " + locationString;
        }
      }
      str += "\n";
    }

    LOGGER.info(str);

    List<Tuple2<String, BlockDetails[]>> blocksGroupByList = new ArrayList<>();
    for (String nodeBlockMappingKey : nodeBlockMappingKeys) {
      List<Distributable> value = nodeBlockMapping.get(nodeBlockMappingKey);
      List<BlockDetails> blockDetailsList = new ArrayList<>();
      for (Distributable distributable : value) {
        TableBlockInfo tableBlock = (TableBlockInfo) distributable;
        blockDetailsList.add(
            new BlockDetails(new org.apache.hadoop.fs.Path(tableBlock.getFilePath()),
                tableBlock.getBlockOffset(), tableBlock.getBlockLength(),
                tableBlock.getLocations()));
      }
      BlockDetails[] blockDetailsArray = (BlockDetails[]) blockDetailsList.toArray();
      blocksGroupByList.add(new Tuple2<>(nodeBlockMappingKey, blockDetailsArray);
    }

    Tuple2<String, BlockDetails[]>[] blockGroupBy = (Tuple2<String, BlockDetails[]>[]) blocksGroupByList.toArray();

    //status: Array[(String, (LoadMetadataDetails, ExecutionErrors))]

    status = (Tuple2<String, Tuple2<LoadMetadataDetails, ExecutionErrors>>[]) new NewCarbonDataLoadRDD(
            sqlContext.sparkContext(), new DataLoadResultImpl(), loadModel, blockGroupBy, false)
            .collect();

        /*
        blocksGroupBy = nodeBlockMapping.map(entry => {
            val blockDetailsList =
              entry._2.asScala.map(distributable => {
                val tableBlock = distributable.asInstanceOf[TableBlockInfo]
                new BlockDetails(new Path(tableBlock.getFilePath),
                  tableBlock.getBlockOffset, tableBlock.getBlockLength, tableBlock.getLocations
                )
              }).toArray
            (entry._1, blockDetailsList)
          }
          ).toArray
        }



         */

  }

  /**
   * cleanup step if necessary
   */
  void close() {

  }
}

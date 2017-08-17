package org.apache.carbondata.hadoop;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.hadoop.util.ObjectSerializationUtil;
import org.apache.carbondata.processing.model.CarbonLoadModel;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;

public class CarbonOutputFormat<T> extends OutputFormat<Void, T> {

  private static final String CARBON_WRITE_SUPPORT = "mapreduce.output.carbonoutputformat.writesupport";

  private static final String TABLE_INFO = "mapreduce.input.carboninputformat.tableinfo";

  private CarbonTable carbonTable;

  @Override public RecordWriter<Void, T> getRecordWriter(TaskAttemptContext job)
      throws IOException, InterruptedException {
    Configuration configuration = job.getConfiguration();
    //validate database & table exist by reading the metastore
    CarbonLoadModel loadModel = getCarbonLoadModel(configuration);
    CarbonWriteSupport<T> writeSupport = getWriteSupport(configuration);
    writeSupport.initialize(loadModel, null);

    return new CarbonRecordWriter<>(loadModel, writeSupport);
  }

  @Override public void checkOutputSpecs(JobContext context)
      throws IOException, InterruptedException {

  }

  /**
   * Set the `tableInfo` in `configuration`
   */
  public static void setTableInfo(Configuration configuration, TableInfo tableInfo)
      throws IOException {
    if (null != tableInfo) {
      configuration.set(TABLE_INFO, ObjectSerializationUtil.encodeToString(tableInfo.serialize()));
    }
  }

  /**
   * Get TableInfo object from `configuration`
   */
  private TableInfo getTableInfo(Configuration configuration) throws IOException {
    String tableInfoStr = configuration.get(TABLE_INFO);
    if (tableInfoStr == null) {
      return null;
    } else {
      TableInfo output = new TableInfo();
      output.readFields(
          new DataInputStream(
              new ByteArrayInputStream(
                  ObjectSerializationUtil.decodeStringToBytes(tableInfoStr))));
      return output;
    }
  }

  public CarbonLoadModel getCarbonLoadModel(Configuration configuration) throws IOException{
    CarbonLoadModel loadModel = new CarbonLoadModel();
        //create load model using values from configuration
    TableInfo tableInfo = getTableInfo(configuration);
   /* loadModel.setTableName(tableInfo.getFactTable().getTableName());
    loadModel.setDatabaseName(tableInfo.getDatabaseName());
    //Verify whether database and table exist
    // if not sys.error(s"Table $dbName.$tableName does not exist")
    // Also, if (null == relation.tableMeta.carbonTable) sys.error(s"Data loading failed. table not found: $dbName.$tableName")
    loadModel.setStorePath(tableInfo.getStorePath());
    // add the start entry for the new load in the table status file
    loadModel.setUseOnePass(true);
    // generate predefined dictionary
    //carbonLoadModel.setCarbonDataLoadSchema(dataLoadSchema)
    //as we are using One pass sort_scope should not be Global_Sort
    //Validate bad_record_path
    //loadModel.setbadrecordsLocation
    //set SinglePass to true*/
    return loadModel;
  }

  public CarbonWriteSupport<T> getWriteSupport(Configuration configuration) {
    String className = configuration.get(CARBON_WRITE_SUPPORT);
    CarbonWriteSupport writeSupport = null;
    if (className != null) {
      Class<?> writeSupportClass;
      try {
        writeSupportClass = configuration.getClassByName(className);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e.getMessage(), e);
      }

      try {
        if (writeSupportClass != null) {
          writeSupport = ((CarbonWriteSupport) writeSupportClass.newInstance());
        }
      } catch (InstantiationException e) {
        throw new RuntimeException(e.getMessage(), e);
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e.getMessage(), e);
      }
    }

    if(writeSupport == null) {
      writeSupport = new CarbonWriteSupport();
    }

    return writeSupport;
  }

  public static void setOutputPath(TaskAttemptContext context, Path outputDir) {
    context.getConfiguration().set("mapred.output.dir", outputDir.toString());
  }

  public static Path getOutputPath(TaskAttemptContext context) {
    String name = context.getConfiguration().get("mapred.output.dir");
    return name == null?null:new Path(name);
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context)
      throws IOException {
    Path output = getOutputPath(context);
      return new CarbonOutputCommiter(output, context);
  }
}

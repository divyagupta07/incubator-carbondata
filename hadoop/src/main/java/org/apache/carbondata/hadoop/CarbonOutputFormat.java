package org.apache.carbondata.hadoop;

import java.io.IOException;

import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.TableSchema;
import org.apache.carbondata.core.mutate.CarbonUpdateUtil;
import org.apache.carbondata.hadoop.util.ObjectSerializationUtil;
import org.apache.carbondata.processing.model.CarbonDataLoadSchema;
import org.apache.carbondata.processing.model.CarbonLoadModel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class CarbonOutputFormat<T> extends OutputFormat<Void, T> {

  private static final String CARBON_WRITE_SUPPORT =
      "mapreduce.output.carbonoutputformat.writesupport";

  private static final String TABLE_INFO = "mapreduce.input.carboninputformat.tableinfo";
  private static final String DATABASE_NAME = "carbondata.current.database";

  private static final String LOCATION = "carbondata.current.location";

  private static final String COLUMN_TYPES = "carbondata.schema.dataTypes";

  private static final String COLUMN_NAMES = "carbondata.schema.columnNames";

  private static final String SERDE_LIB = "carbondata.serde.lib";

  private static final String CARBONDATA_INPUT_FORMAT = "carbondata.input.format";

  private CarbonTable carbonTable;

  /**
   * Set the `tableInfo` in `configuration`
   */
  public static void setTableInfo(Configuration configuration, TableInfo tableInfo)
      throws IOException {
    if (null != tableInfo) {
      configuration.set(TABLE_INFO, ObjectSerializationUtil.encodeToString(tableInfo.serialize()));
    }
  }

  public static void setOutputPath(TaskAttemptContext context, Path outputDir) {
    context.getConfiguration().set("mapred.output.dir", outputDir.toString());
  }

  public static Path getOutputPath(TaskAttemptContext context) {
    String name = context.getConfiguration().get("mapred.output.dir");
    return name == null ? null : new Path(name);
  }

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

  private TableSchema getTableSchema(Configuration configuration) {
    String storePath = configuration.get(LOCATION);
    String tableName = storePath.substring(storePath.lastIndexOf("/") + 1, storePath.length()-1);
    TableSchema tableSchema = new TableSchema();
    tableSchema.setTableName(tableName);
    return tableSchema;
  }

  /**
   * Get TableInfo object from `configuration`
   */
  private TableInfo getTableInfo(Configuration configuration) throws Exception {
    TableInfo tableInfo = new TableInfo();
    try {
      String uniqueTableName = configuration.get(LOCATION);
      tableInfo.setFactTable(getTableSchema(configuration));
      tableInfo.setDatabaseName(configuration.get(DATABASE_NAME));
      tableInfo.setTableUniqueName(configuration.get(DATABASE_NAME) + uniqueTableName
              .substring(uniqueTableName.lastIndexOf("/") + 1));
      //todo add store location in the tableInfo, extract it from the above defined variable LOCATION
      tableInfo.setStorePath("");
      return tableInfo;
    } catch (Exception exception) {
      System.out.println("Exception occured while fetching TableInfo " + exception.getMessage());
      throw new RuntimeException(exception.getMessage());
    }
  }

  public CarbonLoadModel getCarbonLoadModel(Configuration configuration) throws IOException {
    try {
      CarbonLoadModel loadModel = new CarbonLoadModel();
      TableInfo tableInfo = getTableInfo(configuration);
      loadModel.setTableName(tableInfo.getFactTable().getTableName());
      loadModel.setDatabaseName(tableInfo.getDatabaseName());
      loadModel.setStorePath(tableInfo.getStorePath());
      /**
       * Use one pass to generate dictionary
       */
      loadModel.setUseOnePass(true);
      loadModel.setCarbonDataLoadSchema(getCarbonLoadSchema(configuration));
      loadModel.setFactTimeStamp(CarbonUpdateUtil.readCurrentTime());
      //todo add path for bad records
      loadModel.setBadRecordsLocation("");
      return loadModel;
    } catch (Exception exception) {
      System.out.println("Exception occured while fetching load Model " + exception.getMessage());
      throw new RuntimeException(exception.getMessage());
    }
  }

  private CarbonDataLoadSchema getCarbonLoadSchema(Configuration configuration) throws Exception {

    CarbonTable carbonTable = CarbonTable.buildFromTableInfo(getTableInfo(configuration));
    return new CarbonDataLoadSchema(carbonTable);
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

    if (writeSupport == null) {
      writeSupport = new CarbonWriteSupport();
    }

    return writeSupport;
  }

  @Override public OutputCommitter getOutputCommitter(TaskAttemptContext context)
      throws IOException {
    Path output = getOutputPath(context);
    return new CarbonOutputCommiter(output, context);
  }
}

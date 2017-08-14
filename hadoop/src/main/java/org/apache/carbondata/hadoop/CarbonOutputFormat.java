package org.apache.carbondata.hadoop;

import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.hadoop.util.ObjectSerializationUtil;
import org.apache.carbondata.hadoop.util.SchemaReader;
import org.apache.carbondata.processing.model.CarbonDataLoadSchema;
import org.apache.carbondata.processing.model.CarbonLoadModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Map;

import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR;

public class CarbonOutputFormat<T> extends OutputFormat<Void, T> {

    private static final String CARBON_WRITE_SUPPORT = "mapreduce.output.carbonoutputformat.writesupport";

    private static final String TABLE_INFO = "mapreduce.input.carboninputformat.tableinfo";

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

    @Override
    public RecordWriter<Void, T> getRecordWriter(TaskAttemptContext job)
            throws IOException, InterruptedException {
        Configuration configuration = job.getConfiguration();
        //validate database & table exist by reading the metastore
        CarbonLoadModel loadModel = getCarbonLoadModel(configuration);
        CarbonWriteSupport<T> writeSupport = getWriteSupport(configuration);

        return new CarbonRecordWriter<>(loadModel, writeSupport);
    }

    @Override
    public void checkOutputSpecs(JobContext context)
            throws IOException, InterruptedException {

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

    public CarbonLoadModel getCarbonLoadModel(Configuration configuration) throws IOException {
        CarbonLoadModel loadModel = new CarbonLoadModel();
        //create load model using values from configuration
        TableInfo tableInfo = getTableInfo(configuration);
        loadModel.setTableName(tableInfo.getFactTable().getTableName());
        loadModel.setDatabaseName(tableInfo.getDatabaseName());
        loadModel.setStorePath(tableInfo.getStorePath());

        loadModel.setUseOnePass(true);
        //carbonLoadModel.setCarbonDataLoadSchema(dataLoadSchema)
        //as we are using One pass sort_scope should not be Global_Sort
        //Validate bad_record_path
        //loadModel.setbadrecordsLocation
        //set SinglePass to true

        CarbonTable table = getOrCreateCarbonTable(configuration);
        CarbonDataLoadSchema dataLoadSchema = new CarbonDataLoadSchema(table);
        // Need to fill dimension relation
        loadModel.setCarbonDataLoadSchema(dataLoadSchema);

        //properties code

        /*

        val partitionLocation = relation.tableMeta.storePath + "/partition/" +
          relation.tableMeta.carbonTableIdentifier.getDatabaseName + "/" +
          relation.tableMeta.carbonTableIdentifier.getTableName + "/"
      val columnar = sparkSession.conf.get("carbon.is.columnar.storage", "true").toBoolean
      val sort_scope = optionsFinal("sort_scope")
      val single_pass = optionsFinal("single_pass")
      val bad_records_logger_enable = optionsFinal("bad_records_logger_enable")
      val bad_records_action = optionsFinal("bad_records_action")
      val bad_record_path = optionsFinal("bad_record_path")
      val global_sort_partitions = optionsFinal("global_sort_partitions")
      val dateFormat = optionsFinal("dateformat")
      val delimeter = optionsFinal("delimiter")
      val complex_delimeter_level1 = optionsFinal("complex_delimiter_level_1")
      val complex_delimeter_level2 = optionsFinal("complex_delimiter_level_2")
      val all_dictionary_path = optionsFinal("all_dictionary_path")
      val column_dict = optionsFinal("columndict")
      if (sort_scope.equals("GLOBAL_SORT") &&
          single_pass.equals("TRUE")) {
        sys.error("Global_Sort can't be used with single_pass flow")
      }
      ValidateUtil.validateDateFormat(dateFormat, table, tableName)
      ValidateUtil.validateSortScope(table, sort_scope)


      if (bad_records_logger_enable.toBoolean ||
          LoggerAction.REDIRECT.name().equalsIgnoreCase(bad_records_action)) {
        if (!CarbonUtil.isValidBadStorePath(bad_record_path)) {
          sys.error("Invalid bad records location.")
        }
      }
      carbonLoadModel.setBadRecordsLocation(bad_record_path)

      ValidateUtil.validateGlobalSortPartitions(global_sort_partitions)
      carbonLoadModel.setEscapeChar(checkDefaultValue(optionsFinal("escapechar"), "\\"))
      carbonLoadModel.setQuoteChar(checkDefaultValue(optionsFinal("quotechar"), "\""))
      carbonLoadModel.setCommentChar(checkDefaultValue(optionsFinal("commentchar"), "#"))


         */
        /*CarbonProperties carbonProperty = CarbonProperties.getInstance();
        carbonProperty.addProperty("zookeeper.enable.lock", "false");
        Map<String,String> optionsFinal = getFinalOptions(carbonProperty)
        String fileHeader = optionsFinal("fileheader")
        val headerOption = options.get("header")
        if (headerOption.isDefined) {
            // whether the csv file has file header
            // the default value is true
            val header = try {
                headerOption.get.toBoolean
            } catch {
                case ex: IllegalArgumentException =>
                    throw new MalformedCarbonCommandException(
                            "'header' option should be either 'true' or 'false'. " + ex.getMessage)
            }
            header match {
                case true =>
                    if (fileHeader.nonEmpty) {
                        throw new MalformedCarbonCommandException(
                                "When 'header' option is true, 'fileheader' option is not required.")
                    }
                case false =>
                    // generate file header
                    if (fileHeader.isEmpty) {
                        fileHeader = table.getCreateOrderColumn(table.getFactTableName)
                                .asScala.map(_.getColName).mkString(",")
                    }
            }
        }*/



        return loadModel;
    }

    private AbsoluteTableIdentifier getAbsoluteTableIdentifier(Configuration configuration)
            throws IOException {
        String dirs = configuration.get(INPUT_DIR, "");
        String[] inputPaths = StringUtils.split(dirs);
        if (inputPaths.length == 0) {
            throw new InvalidPathException("No input paths specified in job");
        }
        return AbsoluteTableIdentifier.fromTablePath(inputPaths[0]);
    }

    private CarbonTable getOrCreateCarbonTable(Configuration configuration) throws IOException {
        if (carbonTable == null) {
            // carbon table should be created either from deserialized table info (schema saved in
            // hive metastore) or by reading schema in HDFS (schema saved in HDFS)
            TableInfo tableInfo = getTableInfo(configuration);
            CarbonTable carbonTable;
            if (tableInfo != null) {
                carbonTable = CarbonTable.buildFromTableInfo(tableInfo);
            } else {
                carbonTable = SchemaReader.readCarbonTableFromStore(
                        getAbsoluteTableIdentifier(configuration));
            }
            this.carbonTable = carbonTable;
            return carbonTable;
        } else {
            return this.carbonTable;
        }
    }

    public CarbonWriteSupport<T> getWriteSupport(Configuration configuration) {
        String className = configuration.get(CARBON_WRITE_SUPPORT);

        if (className == null) {
            return null;
        }
        Class<?> writeSupportClass;
        try {
            writeSupportClass = configuration.getClassByName(className);

        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e.getMessage(), e);
        }

        try {
            if (writeSupportClass != null) {
                return ((CarbonWriteSupport) writeSupportClass.newInstance());
            } else return null;

        } catch (InstantiationException e) {
            throw new RuntimeException(e.getMessage(), e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context)
            throws IOException {
        Path output = getOutputPath(context);
        return new CarbonOutputCommiter(output, context);
    }
}

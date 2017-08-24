package org.apache.carbondata.hadoop;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.block.Distributable;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.dictionary.server.DictionaryServer;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.processing.csvload.BlockDetails;
import org.apache.carbondata.processing.model.CarbonLoadModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CarbonWriteSupport<T> {

    DictionaryServer server = null;


    private ArrayList<BlockDetails> getBlockDetails(CarbonLoadModel carbonLoadModel) {
        Configuration hadoopConfiguration = new Configuration();
        // FileUtils will skip file which is no csv, and return all file path which split by ','
        String filePaths = carbonLoadModel.getFactFilePath();
        hadoopConfiguration.set(FileInputFormat.INPUT_DIR, filePaths);
        hadoopConfiguration.set(FileInputFormat.INPUT_DIR_RECURSIVE, "true");
        hadoopConfiguration.set("io.compression.codecs", "org.apache.hadoop.io.compress.GzipCodec," +
                 "org.apache.hadoop.io.compress.DefaultCodec, org.apache.hadoop.io.compress.BZip2Codec");

        TextInputFormat inputFormat = new org.apache.hadoop.mapreduce.lib.input.TextInputFormat();
        try {
            JobContext jobContext = new Job(hadoopConfiguration);
            Object[] rawSplits = inputFormat.getSplits(jobContext).toArray();
            ArrayList<Distributable> blockList = new ArrayList<>(rawSplits.length);
            ArrayList<BlockDetails> blockDetails = new ArrayList<>(rawSplits.length);
            for(Object split: rawSplits) {
                FileSplit fileSplit = (FileSplit)split;
                Distributable distributable = new TableBlockInfo(fileSplit.getPath().toString(), fileSplit.getStart(),
                        "1", fileSplit.getLocations(), fileSplit.getLength(), ColumnarFormatVersion.V1,
                        null);
                blockList.add(distributable);
            }
            for(Distributable block: blockList) {
                TableBlockInfo tableBlockInfo = (TableBlockInfo) block;
                blockDetails.add(new BlockDetails(new Path(tableBlockInfo.getFilePath()), tableBlockInfo.getBlockOffset(),
                        tableBlockInfo.getBlockLength(), tableBlockInfo.getLocations()));
            }
            return blockDetails;
        } catch (IOException e) {
            System.out.println("Unable to create hadoop job" + e.getMessage());
            return null;
        }
    }


    /**
     * Initialization if needed based on the projected column list
     *
     * @param
     * @param absoluteTableIdentifier table identifier
     */
    void initialize(CarbonLoadModel loadModel,
                    AbsoluteTableIdentifier absoluteTableIdentifier) throws IOException {
        //if singlepass is true start dictionaryserverClient and generate global dictionary
        CarbonTable table = loadModel.getCarbonDataLoadSchema().getCarbonTable();
        List<CarbonDimension> allDimensions = table.getAllDimensions();
        Boolean createDictionary =false;
        for(CarbonDimension dimension : allDimensions){
            if(dimension.hasEncoding(Encoding.DICTIONARY) && !dimension.hasEncoding(Encoding.DIRECT_DICTIONARY)) {
                createDictionary = true;
            }
        }

        CarbonProperties carbonProperty = CarbonProperties.getInstance();
        if(createDictionary) {
            String dictionaryServerPort = carbonProperty
                    .getProperty(CarbonCommonConstants.DICTIONARY_SERVER_PORT,
                            CarbonCommonConstants.DICTIONARY_SERVER_PORT_DEFAULT);
            server =  DictionaryServer.getInstance(Integer.parseInt(dictionaryServerPort));
            loadModel.setDictionaryServerPort(server.getPort());
        }
        getBlockDetails(loadModel);
    }

    /**
     * convert column data back to row representation
     *
     * @param data column data
     */
    void write(T data) {
        //write the data
        //Array[CarbonIterator[Array[AnyRef]]] recordReaders = getInputIterators
        //val executor = new DataLoadExecutor()
        //executor.execute(loadModel, storeLocation, recordReaders)
    }

    /**
     * cleanup step if necessary
     */
    void close() {
        try {
            server.shutdown();
        } catch (Exception e) {
            System.out.println("Failed to shutdown dictionary server" + e.getMessage());
        }
    }
}

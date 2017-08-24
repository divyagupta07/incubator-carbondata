package org.apache.carbondata.hadoop;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.dictionary.server.DictionaryServer;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonProperty;
import org.apache.carbondata.processing.model.CarbonLoadModel;
import scala.Option;

import java.io.IOException;
import java.util.List;

public class CarbonWriteSupport<T> {

    DictionaryServer server = null;

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

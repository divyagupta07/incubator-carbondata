package org.apache.carbondata.hadoop.writesupport;

import org.apache.carbondata.core.dictionary.server.DictionaryServer;
import org.apache.carbondata.processing.model.CarbonLoadModel;

public class CarbonDictionaryUtil {

  private void writeDictionary(CarbonLoadModel carbonLoadModel, DictionaryServer dictionaryServer,
      Boolean writeAll) throws Exception {

    String uniqueTableName = carbonLoadModel.getDatabaseName() + carbonLoadModel.getTableName();

    if (dictionaryServer != null) {
      try {
        if (writeAll) {
          dictionaryServer.writeDictionary();
        } else {
          dictionaryServer.writeTableDictionary(uniqueTableName);
        }
      } catch (Exception e) {
        System.out.println("Error while writing dictionary file for " + uniqueTableName);
        throw new Exception("Data load failed due to error while writing dictionary file!");
      }
    }

  }
}
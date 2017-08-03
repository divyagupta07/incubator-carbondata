package org.apache.carbondata.hadoop;

import java.io.IOException;

import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.processing.model.CarbonLoadModel;

public class CarbonWriteSupport<T> {

  /**
   * Initialization if needed based on the projected column list
   *
   * @param
   * @param absoluteTableIdentifier table identifier
   */
  void initialize(CarbonLoadModel loadModel,
      AbsoluteTableIdentifier absoluteTableIdentifier) throws IOException{
      //if singlepass is true start dictionaryserverClient and generate global dictionary
  }

  /**
   * convert column data back to row representation
   * @param data column data
   */
  void writeRecord(Object[] data){

  }

  /**
   * cleanup step if necessary
   */
  void close(){

  }
}

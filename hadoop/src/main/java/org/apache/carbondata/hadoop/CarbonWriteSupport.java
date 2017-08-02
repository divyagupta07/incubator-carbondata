package org.apache.carbondata.hadoop;

import java.io.IOException;

import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;


public class CarbonWriteSupport<T> {

  /**
   * Initialization if needed based on the projected column list
   *
   * @param carbonColumns column list
   * @param absoluteTableIdentifier table identifier
   */
  void initialize(CarbonColumn[] carbonColumns,
      AbsoluteTableIdentifier absoluteTableIdentifier) throws IOException{

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

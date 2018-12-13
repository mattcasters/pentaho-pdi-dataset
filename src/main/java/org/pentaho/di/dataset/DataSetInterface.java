package org.pentaho.di.dataset;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMetaInterface;

import java.util.List;

public interface DataSetInterface {

  /**
   * Get standard Kettle row metadata from the defined data set fields
   *
   * @param columnName true if you want the field names to be called after the columns, false if you prefer the field names in the result.
   * @return The row metadata
   * @throws KettlePluginException
   */
  public RowMetaInterface getSetRowMeta( boolean columnName ) throws KettlePluginException;


  public DataSetField findFieldWithName( String fieldName );


  public String findColumnForField( String dataSetFieldName );

  public int indexOfField( String fieldName );


  /**
   * Get the rows for this data set in the format of the data set.
   *
   * @param log      the logging channel to which you can write.
   * @param location The fields to obtain in the order given
   * @return The rows for the given location
   * @throws KettleException
   */
  public List<Object[]> getAllRows( LogChannelInterface log, DataSetGroup group, TransUnitTestSetLocation location ) throws KettleException;



  /**
   * Gets fields
   *
   * @return value of fields
   */
  public List<DataSetField> getFields();

  /**
   * @param fields The fields to set
   */
  public void setFields( List<DataSetField> fields );

}

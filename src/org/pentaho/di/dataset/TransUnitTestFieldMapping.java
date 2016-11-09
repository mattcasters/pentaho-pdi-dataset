package org.pentaho.di.dataset;

import org.pentaho.metastore.persist.MetaStoreAttribute;

/**
 * This class simply describes a mapping between the step fields we want to test and the data set fields you want to match with.
 * @author matt
 *
 */
public class TransUnitTestFieldMapping {

  @MetaStoreAttribute( key = "step_field" )
  private String stepFieldName;

  @MetaStoreAttribute( key = "data_set_field" )
  private String dataSetFieldName;

  public TransUnitTestFieldMapping() {
  }

  public TransUnitTestFieldMapping( String stepFieldName, String dataSetFieldName ) {
    this();
    this.stepFieldName = stepFieldName;
    this.dataSetFieldName = dataSetFieldName;
  }

  public String getStepFieldName() {
    return stepFieldName;
  }

  public void setStepFieldName( String stepFieldName ) {
    this.stepFieldName = stepFieldName;
  }

  public String getDataSetFieldName() {
    return dataSetFieldName;
  }

  public void setDataSetFieldName( String dataSetFieldName ) {
    this.dataSetFieldName = dataSetFieldName;
  }
}

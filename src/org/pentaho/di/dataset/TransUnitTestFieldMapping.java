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

  @MetaStoreAttribute( key = "sort_order" )
  private String sortOrder;

  public TransUnitTestFieldMapping() {
  }

  public TransUnitTestFieldMapping( String stepFieldName, String dataSetFieldName, String sortOrder ) {
    this();
    this.stepFieldName = stepFieldName;
    this.dataSetFieldName = dataSetFieldName;
    this.sortOrder = sortOrder;
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

  public String getSortOrder() {
    return sortOrder;
  }

  public void setSortOrder( String sortOrder ) {
    this.sortOrder = sortOrder;
  }

}

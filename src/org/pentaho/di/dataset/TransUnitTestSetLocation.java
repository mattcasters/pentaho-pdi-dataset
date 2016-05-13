package org.pentaho.di.dataset;

import java.util.ArrayList;
import java.util.List;

import org.pentaho.metastore.persist.MetaStoreAttribute;

/** 
 * This defines the place where we need to inject an input data set
 * 
 * @author matt
 *
 */
public class TransUnitTestSetLocation {
  
  @MetaStoreAttribute(key="step_name")
  protected String stepname;
  
  @MetaStoreAttribute(key="data_set_name")
  protected String dataSetName;
  
  @MetaStoreAttribute(key="field_mappings")
  protected List<TransUnitTestFieldMapping> fieldMappings;
  
  public TransUnitTestSetLocation() {
    fieldMappings = new ArrayList<TransUnitTestFieldMapping>();
  }

  public TransUnitTestSetLocation( String stepname, String dataSetName, List<TransUnitTestFieldMapping> fieldMappings ) {
    this();
    this.stepname = stepname;
    this.dataSetName = dataSetName;
    this.fieldMappings = fieldMappings;
  }

  public String getStepname() {
    return stepname;
  }

  public void setStepname( String stepname ) {
    this.stepname = stepname;
  }

  public String getDataSetName() {
    return dataSetName;
  }

  public void setDataSetName( String dataSetName ) {
    this.dataSetName = dataSetName;
  }
  
  public List<TransUnitTestFieldMapping> getFieldMappings() {
    return fieldMappings;
  }

  public void setFieldMappings( List<TransUnitTestFieldMapping> fieldMappings ) {
    this.fieldMappings = fieldMappings;
  }
}

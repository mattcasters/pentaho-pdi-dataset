/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.dataset;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.dataset.util.DataSetConst;
import org.pentaho.metastore.persist.MetaStoreAttribute;
import org.pentaho.metastore.persist.MetaStoreElementType;

import java.util.ArrayList;
import java.util.List;

@MetaStoreElementType(
  name = "Kettle Data Set",
  description = "This defines a Kettle data set" )
public class DataSet {

  private String name;

  @MetaStoreAttribute( key = "description" )
  private String description;

  @MetaStoreAttribute( key = "table_name" )
  private String tableName;

  @MetaStoreAttribute( key = "dataset_fields" )
  private List<DataSetField> fields;

  @MetaStoreAttribute( key = "group_name", nameReference = true, nameListKey = DataSetConst.GROUP_LIST_KEY )
  private DataSetGroup group;


  public DataSet() {
    fields = new ArrayList<>();
  }

  public DataSet( String name, String description, DataSetGroup group, String tableName, List<DataSetField> fields ) {
    this();
    this.name = name;
    this.description = description;
    this.group = group;
    this.tableName = tableName;
    this.fields = fields;
  }

  @Override
  public boolean equals( Object obj ) {
    if ( this == obj ) {
      return true;
    }
    if ( !( obj instanceof DataSet ) ) {
      return false;
    }
    DataSet cmp = (DataSet) obj;
    return name.equals( cmp );
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }


  /**
   * Get standard Kettle row metadata from the defined data set fields
   *
   * @param columnName true if you want the field names to be called after the columns, false if you prefer the field names in the result.
   * @return The row metadata
   * @throws KettlePluginException
   */
  public RowMetaInterface getSetRowMeta( boolean columnName ) throws KettlePluginException {
    RowMetaInterface rowMeta = new RowMeta();
    for ( DataSetField field : getFields() ) {
      ValueMetaInterface valueMeta = ValueMetaFactory.createValueMeta(
        columnName ? field.getColumnName() : field.getFieldName(),
        field.getType(),
        field.getLength(),
        field.getPrecision() );
      valueMeta.setComments( field.getComment() );
      valueMeta.setConversionMask( field.getFormat() );
      rowMeta.addValueMeta( valueMeta );
    }
    return rowMeta;
  }

  public DataSetField findFieldWithName( String fieldName ) {
    for ( DataSetField field : fields ) {
      if ( field.getFieldName().equalsIgnoreCase( fieldName ) ) {
        return field;
      }
    }
    return null;
  }

  public String findColumnForField( String dataSetFieldName ) {
    DataSetField field = findFieldWithName( dataSetFieldName );
    if ( field == null ) {
      return null;
    }
    return field.getColumnName();
  }

  public int indexOfField( String fieldName ) {
    for ( int i = 0; i < fields.size(); i++ ) {
      DataSetField field = fields.get( i );
      if ( field.getFieldName().equalsIgnoreCase( fieldName ) ) {
        return i;
      }
    }
    return -1;
  }


  public List<Object[]> getAllRows( LogChannelInterface log, TransUnitTestSetLocation location ) throws KettleException {
    return group.getAllRows( log, this, location );
  }

  public List<Object[]> getAllRows( LogChannelInterface log ) throws KettleException {
    return group.getAllRows( log, this );
  }


  /**
   * Calculate the row metadata for the data set fields needed for the given location.
   *
   * @param location
   * @return The fields metadata for those fields that are mapped against a certain step (location)
   */
  public RowMetaInterface getMappedDataSetFieldsRowMeta( TransUnitTestSetLocation location ) throws KettlePluginException {

    RowMetaInterface setRowMeta = getSetRowMeta( false );
    RowMetaInterface rowMeta = new RowMeta();
    for ( TransUnitTestFieldMapping fieldMapping : location.getFieldMappings() ) {
      ValueMetaInterface valueMeta = setRowMeta.searchValueMeta( fieldMapping.getDataSetFieldName() );
      rowMeta.addValueMeta( valueMeta );
    }
    return rowMeta;
  }


  /**
   * Gets name
   *
   * @return value of name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name The name to set
   */
  public void setName( String name ) {
    this.name = name;
  }

  /**
   * Gets description
   *
   * @return value of description
   */
  public String getDescription() {
    return description;
  }

  /**
   * @param description The description to set
   */
  public void setDescription( String description ) {
    this.description = description;
  }

  /**
   * Gets tableName
   *
   * @return value of tableName
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * @param tableName The tableName to set
   */
  public void setTableName( String tableName ) {
    this.tableName = tableName;
  }

  /**
   * Gets fields
   *
   * @return value of fields
   */
  public List<DataSetField> getFields() {
    return fields;
  }

  /**
   * @param fields The fields to set
   */
  public void setFields( List<DataSetField> fields ) {
    this.fields = fields;
  }

  /**
   * Gets group
   *
   * @return value of group
   */
  public DataSetGroup getGroup() {
    return group;
  }

  /**
   * @param group The group to set
   */
  public void setGroup( DataSetGroup group ) {
    this.group = group;
  }
}

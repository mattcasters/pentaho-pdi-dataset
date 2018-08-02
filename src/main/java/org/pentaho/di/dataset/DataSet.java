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

import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LoggingObject;
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

  @MetaStoreAttribute( key = "group_name", nameReference = true, nameListKey = DataSetConst.GROUP_LIST_KEY )
  private DataSetGroup group;

  @MetaStoreAttribute( key = "table_name" )
  private String tableName;

  @MetaStoreAttribute( key = "dataset_fields" )
  private List<DataSetField> fields;

  public DataSet() {
    fields = new ArrayList<DataSetField>();
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

  public String getName() {
    return name;
  }

  public void setName( String name ) {
    this.name = name;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription( String description ) {
    this.description = description;
  }

  public DataSetGroup getGroup() {
    return group;
  }

  public void setGroup( DataSetGroup group ) {
    this.group = group;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName( String tableName ) {
    this.tableName = tableName;
  }

  public List<DataSetField> getFields() {
    return fields;
  }

  public void setFields( List<DataSetField> fields ) {
    this.fields = fields;
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
      DataSetField field = fields.get(i);
      if ( field.getFieldName().equalsIgnoreCase( fieldName ) ) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Get the rows for this data set in the format of the data set.
   *
   * @param log      the logging channel to which you can write.
   * @param location The fields to obtain in the order given
   * @return The rows for the given location
   * @throws KettleException
   */
  public List<Object[]> getAllRows( LogChannelInterface log, TransUnitTestSetLocation location ) throws KettleException {
    try {
      DatabaseMeta databaseMeta = group.getDatabaseMeta();
      synchronized ( databaseMeta ) {
        String schemaTable = databaseMeta.getQuotedSchemaTableCombination( group.getSchemaName(), getTableName() );
        Database database = null;
        List<Object[]> rows = null;
        List<String> sortFields = location.getFieldOrder();

        List<String> selectColumns = new ArrayList<String>();

        // Which columns do we need for the location (input or golden)
        //
        for ( DataSetField field : fields ) {

          // Pick this data set field from the fields list.
          //
          String column = field.getColumnName();
          selectColumns.add( column );
        }

        // Which columns are we sorting on (if any)
        //
        List<String> sortColumns = new ArrayList<String>();
        for ( String sortField : sortFields ) {
          String sortColumn = findColumnForField( sortField );
          if ( sortColumn == null ) {
            throw new KettleException( "Unable to find sort column with field name '" + sortField + "' (from mapping) in data set '" + getName() + "'" );
          }
          sortColumns.add( sortColumn );
        }

        try {
          database = new Database( new LoggingObject( "DataSetDialog" ), group.getDatabaseMeta() );
          database.connect();

          String sql = "SELECT ";
          for ( int i = 0; i < selectColumns.size(); i++ ) {
            String column = selectColumns.get( i );
            if ( i > 0 ) {
              sql += ", ";
            }
            sql += databaseMeta.quoteField( column );
          }
          sql += " FROM " + schemaTable;

          if ( sortColumns != null && !sortColumns.isEmpty() ) {
            sql += " ORDER BY ";
            boolean first = true;
            for ( String sortColumn : sortColumns ) {
              if ( first ) {
                first = false;
              } else {
                sql += ", ";
              }
              sql += databaseMeta.quoteField( sortColumn );
            }
          }

          if ( log.isDetailed() ) {
            log.logDetailed( "---------------------------------------------" );
            log.logDetailed( "SQL = " + sql );
            log.logDetailed( "---------------------------------------------" );
          }
          rows = database.getRows( sql, 0 );

          // Now, our work is not done...
          // We will probably have data conversation issues so let's handle this...
          //
          RowMetaInterface dbRowMeta = database.getReturnRowMeta();
          if ( log.isDetailed() ) {
            log.logDetailed( "DB RowMeta = " + dbRowMeta.toStringMeta() );
            log.logDetailed( "---------------------------------------------" );
          }

          // Correct data types if needed
          //
          for ( int i = 0; i < dbRowMeta.size(); i++ ) {
            // In case the data in the table is a different data type for some reasons, bring it back up to spec.
            // The spec is given in getSetRowMeta()
            //
            DataSetField field = fields.get(i);
            ValueMetaInterface dataSetValueMeta = ValueMetaFactory.createValueMeta( field.getFieldName(), field.getType(), field.getLength(), field.getPrecision() );

            ValueMetaInterface dbValueMeta = dbRowMeta.getValueMeta( i );
            if ( dbValueMeta.getType() != dataSetValueMeta.getType() ) {
              // Convert the values in the result set...
              //
              for ( Object[] row : rows ) {
                row[ i ] = dataSetValueMeta.convertData( dbValueMeta, row[ i ] );
              }
            }
          }
        } finally {
          if ( database != null ) {
            database.disconnect();
          }
        }

        return rows;
      }
    } catch ( Exception e ) {
      throw new KettleException( "Unable to get all rows for data set " + name, e );
    }
  }
}

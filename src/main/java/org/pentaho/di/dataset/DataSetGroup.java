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

import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LoggingObjectType;
import org.pentaho.di.core.logging.SimpleLoggingObject;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.dataset.util.DataSetConst;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.metastore.persist.MetaStoreAttribute;
import org.pentaho.metastore.persist.MetaStoreElementType;

import java.util.List;

@MetaStoreElementType( name = "Kettle Data Set Group", description = "Describes a data set group, the database where the sets are stored." )
public class DataSetGroup {

  private static Class<?> PKG = DataSetGroup.class; // for i18n purposes, needed by Translator2!!


  private String name;

  @MetaStoreAttribute( key = "group_type" )
  private DataSetGroupType type;

  @MetaStoreAttribute( key = "description" )
  private String description;

  @MetaStoreAttribute( key = "database_name", nameReference = true, nameListKey = DataSetConst.DATABASE_LIST_KEY )
  private DatabaseMeta databaseMeta;

  @MetaStoreAttribute( key = "schema_name" )
  private String schemaName;

  @MetaStoreAttribute
  private String folderName;

  public DataSetGroup() {
    // empty constructor for the IMetaStore factory
    //
    type = DataSetGroupType.Database;
  }

  public String getName() {
    return name;
  }

  public DataSetGroup( DataSetGroupType type, String name, String description, DatabaseMeta databaseMeta, String schemaName ) {
    this();
    this.type = type;
    this.name = name;
    this.description = description;
    this.databaseMeta = databaseMeta;
    this.schemaName = schemaName;
  }

  @Override
  public boolean equals( Object obj ) {

    if ( !( obj instanceof DataSetGroup ) ) {
      return false;
    }
    if ( this == obj ) {
      return true;
    }

    DataSetGroup cmp = (DataSetGroup) obj;

    return name.equals( cmp.name );
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }

  /**
   * Simply get all the rows without fuss in the order of the database.  Just for preview and some such.
   *
   * @param log
   * @param dataSet
   * @return The rows
   * @throws KettleException
   */
  public List<Object[]> getAllRows( LogChannelInterface log, DataSet dataSet ) throws KettleException {
    switch ( type ) {
      case Database:
        return DataSetDatabaseGroup.getAllRows( log, this, dataSet );
      case CSV:
        return DataSetCsvGroup.getAllRows( log, this, dataSet );
      default:
        throw new KettleException( type.name() + " : not supported yet" );
    }
  }

  /**
   * Get the dataset rows from the in a particular format in a particular order, determined by the given location
   *
   * @param log
   * @param dataSet
   * @param location
   * @return
   * @throws KettleException
   */
  public List<Object[]> getAllRows( LogChannelInterface log, DataSet dataSet, TransUnitTestSetLocation location ) throws KettleException {
    switch ( type ) {
      case Database:
        return DataSetDatabaseGroup.getAllRows( log, this, dataSet, location );
      case CSV:
        return DataSetCsvGroup.getAllRows( log, this, dataSet, location );
      default:
        throw new KettleException( type.name() + " : not supported yet" );
    }
  }

  public void writeDataSetData( String tableName, RowMetaInterface rowMeta, List<Object[]> dataRows ) throws KettleException {
    SimpleLoggingObject loggingObject = new SimpleLoggingObject( "Writing Data Set", LoggingObjectType.TRANS, null );

    switch ( type ) {
      case Database:
        DataSetDatabaseGroup.writeDataSetData( loggingObject, this, tableName, rowMeta, dataRows );
        break;
      case CSV:
        DataSetCsvGroup.writeDataSetData( loggingObject, this, tableName, rowMeta, dataRows );
        break;
      default:
        throw new KettleException( type.name() + " : not supported yet" );
    }
  }

  public void verifySettings() throws KettleException {
    if ( type == null ) {
      throw new KettleException( "No group type specified" );
    }
    switch ( type ) {
      case Database:
        if ( databaseMeta == null ) {
          throw new KettleException( BaseMessages.getString( PKG, "DataSetDialog.Error.GroupHasNoDatabaseSpecified" ) );
        }
        break;
      case CSV:
        break;
    }
  }

  public void createTable( String tableName, RowMetaInterface rowMeta ) throws KettleException {
    switch ( type ) {
      case Database:
        DataSetDatabaseGroup.createTable( this, tableName, rowMeta );
        break;
      case CSV:
        DataSetCsvGroup.createTable( this, tableName, rowMeta );
        break;
      default:
        throw new KettleException( type.name() + " : not supported yet" );
    }

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
   * Gets databaseMeta
   *
   * @return value of databaseMeta
   */
  public DatabaseMeta getDatabaseMeta() {
    return databaseMeta;
  }

  /**
   * @param databaseMeta The databaseMeta to set
   */
  public void setDatabaseMeta( DatabaseMeta databaseMeta ) {
    this.databaseMeta = databaseMeta;
  }

  /**
   * Gets schemaName
   *
   * @return value of schemaName
   */
  public String getSchemaName() {
    return schemaName;
  }

  /**
   * @param schemaName The schemaName to set
   */
  public void setSchemaName( String schemaName ) {
    this.schemaName = schemaName;
  }

  /**
   * Gets type
   *
   * @return value of type
   */
  public DataSetGroupType getType() {
    return type;
  }

  /**
   * @param type The type to set
   */
  public void setType( DataSetGroupType type ) {
    this.type = type;
  }

  /**
   * Gets folderName
   *
   * @return value of folderName
   */
  public String getFolderName() {
    return folderName;
  }

  /**
   * @param folderName The folderName to set
   */
  public void setFolderName( String folderName ) {
    this.folderName = folderName;
  }
}

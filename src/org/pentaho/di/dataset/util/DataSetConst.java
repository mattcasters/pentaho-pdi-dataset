package org.pentaho.di.dataset.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LoggingObjectType;
import org.pentaho.di.core.logging.SimpleLoggingObject;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.dataset.DataSet;
import org.pentaho.di.dataset.DataSetField;
import org.pentaho.di.dataset.DataSetGroup;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.shared.SharedObjectInterface;
import org.pentaho.di.shared.SharedObjects;

public class DataSetConst {
  public static String DATA_SET_GROUP_TYPE_NAME = "Data Set Group";
  public static String DATA_SET_GROUP_TYPE_DESCRIPTION = "A collection of data sets and unit tests";

  public static String DATA_SET_GROUP_DESCRIPTION = "description";
  public static String DATA_SET_GROUP_DATABASE_NAME = "connection";

  public static String DATA_SET_TYPE_NAME = "Data Set";
  public static String DATA_SET_TYPE_DESCRIPTION = "A data set";

  public static String DATA_SET_DESCRIPTION = "description";
  public static String DATA_SET_SCHEMA_NAME = "schema";
  public static String DATA_SET_TABLE_NAME = "table";
  public static String DATA_SET_ROWMETA_XML = "rowmeta-xml";
  public static String DATA_SET_GROUP_NAME = "group";

  public static final String DATABASE_LIST_KEY = "Databases";
  public static final String GROUP_LIST_KEY = "DataSetGroups";
  public static final String SET_LIST_KEY = "DataSets";

  public static final String ATTR_GROUP_DATASET = "DataSet";
  public static final String ATTR_STEP_DATASET_INPUT = "DataSetInput";
  public static final String VAR_STEP_DATASET_ENABLED = "__DataSetEnabled__";
  public static final String VAR_UNIT_TEST_NAME = "__UnitTest__";
  public static final String ATTR_STEP_UNIT_TEST = "UnitTest";
  
  public static final String ROW_COLLECTION_MAP = "RowCollectionMap";

  public static final DataSet findDataSet( List<DataSet> list, String dataSetName ) {
    if ( Const.isEmpty( dataSetName ) ) {
      return null;
    }
    for ( DataSet dataSet : list ) {
      if ( dataSetName.equals( dataSet.getName() ) ) {
        return dataSet;
      }
    }
    return null;
  }

  public static final DataSetGroup findDataSetGroup( List<DataSetGroup> list, String dataSetGroupName ) {
    if ( Const.isEmpty( dataSetGroupName ) ) {
      return null;
    }
    for ( DataSetGroup dataSetGroup : list ) {
      if ( dataSetGroupName.equals( dataSetGroup.getName() ) ) {
        return dataSetGroup;
      }
    }
    return null;
  }

  public static List<DatabaseMeta> getAvailableDatabases( Repository repository, SharedObjects sharedObjects ) throws KettleException {
    List<DatabaseMeta> list = new ArrayList<DatabaseMeta>();

    // Load database connections from the central repository if we're connected to one
    //
    if ( repository != null ) {
      ObjectId[] databaseIDs = repository.getDatabaseIDs( false );
      for ( ObjectId databaseId : databaseIDs ) {
        list.add( repository.loadDatabaseMeta( databaseId, null ) );
      }
    }

    // Also load from the shared objects file of the transformation
    //
    Collection<SharedObjectInterface> localSharedObjects = sharedObjects.getObjectsMap().values();

    for ( SharedObjectInterface localSharedObject : localSharedObjects ) {
      if ( localSharedObject instanceof DatabaseMeta ) {
        DatabaseMeta databaseMeta = (DatabaseMeta) localSharedObject;
        // Only add a local database if it doesn't exist in the central repository
        //
        if ( !list.contains( databaseMeta ) ) {
          list.add( databaseMeta );
        }
      }
    }

    return list;
  }
  
  public static final DataSet writeDataSet(String name, String description, DataSetGroup dataSetGroup, String tableName, List<DataSetField> fields, List<Object[]> dataRows) throws KettleException {
    DataSet dataSet = new DataSet( name, description, dataSetGroup, tableName, fields );
    RowMetaInterface rowMeta = dataSet.getSetRowMeta( true );
    List<RowMetaAndData> rows = new ArrayList<RowMetaAndData>();
    for (Object[] dataRow : dataRows) {
      RowMetaAndData row = new RowMetaAndData();
      row.setRowMeta( rowMeta );
      row.setData( dataRow );
      rows.add( row );
    }
    
    Database database = new Database( new SimpleLoggingObject( "Writing Data Set", LoggingObjectType.TRANS, null ), dataSetGroup.getDatabaseMeta());
    try {
      database.connect();
      String sql;
      if ( database.checkTableExists( tableName) ) {
        sql = database.getAlterTableStatement( tableName, rowMeta, null, false, null, true );
      } else {
        sql = database.getCreateTableStatement( tableName, rowMeta, null, false, null, true );
      }
      if ( !Const.isEmpty( sql ) ) {
        database.execStatements( sql );
      }
      database.prepareInsert( rowMeta, tableName );
      for ( RowMetaAndData row : rows ) {
        database.setValuesInsert( row );
        database.insertRow();
      }
      database.commit();
    } finally {
      database.disconnect();
    }

    return dataSet;
  }
  
}

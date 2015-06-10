package org.pentaho.di.dataset.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.dataset.DataSet;
import org.pentaho.di.dataset.DataSetGroup;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.shared.SharedObjectInterface;
import org.pentaho.di.shared.SharedObjects;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.util.PentahoDefaults;

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
  public static final String ATTR_STEP_UNIT_TEST = "UnitTest";

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

  public static List<DatabaseMeta> getAvailableDatabases( Repository repository ) throws KettleException {
    List<DatabaseMeta> list = new ArrayList<DatabaseMeta>();

    // Load database connections from the central repository if we're connected to one
    //
    if ( repository != null ) {
      ObjectId[] databaseIDs = repository.getDatabaseIDs( false );
      for ( ObjectId databaseId : databaseIDs ) {
        list.add( repository.loadDatabaseMeta( databaseId, null ) );
      }
    }

    // Also load from the standard shared objects file
    //
    SharedObjects sharedObjects = new SharedObjects( Const.getSharedObjectsFile() );
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

  public static final MetaStoreFactory<DataSet> createDataSetFactory( IMetaStore metaStore, Repository repository ) throws KettleException, MetaStoreException {
    MetaStoreFactory<DataSetGroup> groupFactory = new MetaStoreFactory<DataSetGroup>( DataSetGroup.class, metaStore, PentahoDefaults.NAMESPACE );
    List<DatabaseMeta> databases = getAvailableDatabases( repository );
    groupFactory.addNameList( DataSetConst.DATABASE_LIST_KEY, databases );
    List<DataSetGroup> groups = groupFactory.getElements();

    MetaStoreFactory<DataSet> setFactory = new MetaStoreFactory<DataSet>( DataSet.class, metaStore, PentahoDefaults.NAMESPACE );
    setFactory.addNameList( DataSetConst.GROUP_LIST_KEY, groups );

    return setFactory;
  }

}

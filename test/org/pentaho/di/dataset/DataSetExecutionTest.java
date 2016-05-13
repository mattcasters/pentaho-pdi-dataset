package org.pentaho.di.dataset;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.KettleClientEnvironment;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LoggingObjectType;
import org.pentaho.di.core.logging.SimpleLoggingObject;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.dataset.trans.ChangeTransMetaPriorToExecutionExtensionPoint;
import org.pentaho.di.dataset.trans.IgnoreInitOfDataSetInputStepExtensionPoint;
import org.pentaho.di.dataset.trans.IndicateUsingDataSetExtensionPoint;
import org.pentaho.di.dataset.trans.InjectDataSetIntoTransExtensionPoint;
import org.pentaho.di.dataset.util.DataSetConst;
import org.pentaho.di.dataset.util.FactoriesHierarchy;
import org.pentaho.di.shared.SharedObjects;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.stores.memory.MemoryMetaStore;

public class DataSetExecutionTest extends TestCase {

  public static final String NAMESPACE = "test";

  public static final String GROUP_NAME = "mappings";
  public static final String GROUP_DESC = "mappings";
  public static final String GROUP_SCHEMA = null;

  public static final String NAME = "Mapping input";
  public static final String DESC = "Input test-set for mapping";
  public static final String TABLE = "mapping_input";

  protected IMetaStore metaStore;
  protected DatabaseMeta databaseMeta;
  protected DataSetGroup dataSetGroup;
  protected FactoriesHierarchy factories;
  private DataSet dataSet;

  @Override
  protected void setUp() throws Exception {
    KettleClientEnvironment.init();
    metaStore = new MemoryMetaStore();
    
    // Temporary databases and so on.
    //
    File tmpSharedObjectsFile = File.createTempFile("shared-objects-unit-test", "xml");
    SharedObjects sharedObjects = new SharedObjects(tmpSharedObjectsFile.getAbsolutePath());
    
    // This Database needs to be found in a shared objects store...
    //
    databaseMeta = new DatabaseMeta( "dataset", "H2", "JDBC", null, "/tmp/datasets", null, null, null );
    sharedObjects.storeObject(databaseMeta);
    
    dataSetGroup = new DataSetGroup( GROUP_NAME, GROUP_DESC, databaseMeta, GROUP_SCHEMA );

    List<DataSetField> fields = new ArrayList<>();
    fields.add( new DataSetField( "a", "column_a", ValueMetaInterface.TYPE_STRING, 20, 0, null ) );
    fields.add( new DataSetField( "b", "column_b", ValueMetaInterface.TYPE_STRING, 20, 0, null ) );
    fields.add( new DataSetField( "c", "column_c", ValueMetaInterface.TYPE_STRING, 20, 0, null ) );
    dataSet = new DataSet( NAME, DESC, dataSetGroup, TABLE, fields );

    RowMetaInterface rowMeta = dataSet.getSetRowMeta( true );
    Database database = new Database( new SimpleLoggingObject( "DataSetExecutionTest", LoggingObjectType.TRANS, null ), databaseMeta );
    try {
      database.connect();
      String sql;
      if ( database.checkTableExists( TABLE ) ) {
        sql = database.getAlterTableStatement( TABLE, rowMeta, null, false, null, true );
      } else {
        sql = database.getCreateTableStatement( TABLE, rowMeta, null, false, null, true );
      }
      if ( !Const.isEmpty( sql ) ) {
        database.execStatements( sql );
      }
      List<RowMetaAndData> rows = new ArrayList<RowMetaAndData>();
      rows.add( new RowMetaAndData( rowMeta, "a1", "b1", "c1" ) );
      rows.add( new RowMetaAndData( rowMeta, "a2", "b2", "c2" ) );
      rows.add( new RowMetaAndData( rowMeta, "a3", "b3", "c3" ) );
      database.prepareInsert( rowMeta, TABLE );
      for ( RowMetaAndData row : rows ) {
        database.setValuesInsert( row );
        database.insertRow();
      }
      database.commit();
    } finally {
      database.disconnect();
    }

    factories = new FactoriesHierarchy( metaStore, Arrays.asList( databaseMeta ) );
    factories.getGroupFactory().saveElement( dataSetGroup );
    factories.getSetFactory().saveElement( dataSet );
  }

  public void testExecution() throws Exception {

    List<Class<? extends ExtensionPointInterface>> pluginClasses = Arrays.asList(
      ChangeTransMetaPriorToExecutionExtensionPoint.class,
      IgnoreInitOfDataSetInputStepExtensionPoint.class,
      IndicateUsingDataSetExtensionPoint.class,
      InjectDataSetIntoTransExtensionPoint.class
      );
    String plugins = Const.NVL( EnvUtil.getSystemProperty( Const.KETTLE_PLUGIN_CLASSES ), "" );
    for ( Class<? extends ExtensionPointInterface> cl : pluginClasses ) {
      if ( plugins.length() > 0 ) {
        plugins += ",";
      }
      plugins += cl.getName();
    }
    System.setProperty( Const.KETTLE_PLUGIN_CLASSES, plugins );

    KettleEnvironment.init();

    TransMeta transMeta = new TransMeta( "test-files/simple-mapping.ktr" );
    transMeta.addDatabase( databaseMeta );

    // Enable data sets
    //
    transMeta.setVariable( DataSetConst.VAR_STEP_DATASET_ENABLED, "Y" );

    // pass our metastore reference
    //
    transMeta.setMetaStore( metaStore );

    // Create an in-memory data-set group and data sets to test with...
    //
    StepMeta stepMeta = transMeta.findStep( "Input" );
    stepMeta.setAttribute( DataSetConst.ATTR_GROUP_DATASET, DataSetConst.ATTR_STEP_DATASET_INPUT, dataSet.getName() );

    Trans trans = new Trans( transMeta );
    trans.setPreview( true ); // data set only works in preview right now
    trans.execute( null );
    trans.waitUntilFinished();
  }
}

package org.pentaho.di.dataset;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.KettleClientEnvironment;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.dataset.trans.ChangeTransMetaPriorToExecutionExtensionPoint;
import org.pentaho.di.dataset.trans.IndicateUsingDataSetExtensionPoint;
import org.pentaho.di.dataset.trans.InjectDataSetIntoTransExtensionPoint;
import org.pentaho.di.dataset.util.DataSetConst;
import org.pentaho.di.dataset.util.FactoriesHierarchy;
import org.pentaho.di.shared.SharedObjects;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.stores.memory.MemoryMetaStore;
import org.pentaho.metastore.util.PentahoDefaults;

import junit.framework.TestCase;

public class TransUnitTestExecutionTest extends TestCase {



  public static final String NAMESPACE = "test";

  public static final String GROUP_NAME = "mappings";
  public static final String GROUP_DESC = "mappings";
  public static final String GROUP_SCHEMA = null;

  public static final String INPUT_SET_NAME = "Mapping input";
  public static final String INPUT_SET_DESC = "Input test-set for mapping";
  public static final String INPUT_SET_TABLE = "mapping_input";

  public static final String GOLDEN_SET_NAME = "Mapping output golden";
  public static final String GOLDEN_SET_DESC = "Golden data set for the mapping";
  public static final String GOLDEN_SET_TABLE = "mapping_output";

  public static final String UNIT_TEST_NAME = "Unit Test";
  public static final String UNIT_TEST_DESCRIPTION = "Tests golden data";
  
  protected IMetaStore metaStore;
  protected SharedObjects sharedObjects;
  protected DatabaseMeta databaseMeta;
  protected DataSetGroup dataSetGroup;
  protected FactoriesHierarchy factories;
  protected DataSet inputDataSet;
  protected DataSet goldenDataSet;
  protected TransUnitTest unitTest;

  @Override
  protected void setUp() throws Exception {
    KettleClientEnvironment.init();
    metaStore = new MemoryMetaStore();
    // metaStore = MetaStoreConst.openLocalPentahoMetaStore();
    
    // Temporary databases and so on.
    //
    File tmpSharedObjectsFile = File.createTempFile("shared-objects-unit-test", ".xml");
    sharedObjects = new SharedObjects();
    
    // This Database needs to be found in a shared objects store...
    //
    databaseMeta = new DatabaseMeta( "dataset", "H2", "JDBC", null, "/tmp/datasets", null, null, null );
    sharedObjects.storeObject(databaseMeta);
    sharedObjects.setFilename(tmpSharedObjectsFile.getAbsolutePath());
    sharedObjects.saveToFile();
    
    dataSetGroup = new DataSetGroup( GROUP_NAME, GROUP_DESC, databaseMeta, GROUP_SCHEMA );

    // Write an input data set...
    //
    createInputDataSet();
    
    // Write the golden data set...
    //
    createGoldenDataSet();
    
    // Create a unit test...
    //
    createUnitTest();
    
    // Create the data set group in the metastore...
    //
    MetaStoreFactory<DataSetGroup> groupFactory = new MetaStoreFactory<DataSetGroup>( DataSetGroup.class, metaStore, PentahoDefaults.NAMESPACE);
    groupFactory.addNameList( DataSetConst.DATABASE_LIST_KEY, Arrays.asList( databaseMeta ) );
    groupFactory.saveElement( dataSetGroup );
    
    // Create the input data set in the metastore...
    //
    MetaStoreFactory<DataSet> setFactory = new MetaStoreFactory<DataSet>( DataSet.class, metaStore, PentahoDefaults.NAMESPACE);
    setFactory.addNameList( DataSetConst.GROUP_LIST_KEY, groupFactory.getElements() );
    setFactory.saveElement( inputDataSet );
    
    // Create the golden data set in the metastore...
    //
    setFactory.saveElement( goldenDataSet );
    
    // Create a unit test...
    MetaStoreFactory<TransUnitTest> testFactory = new MetaStoreFactory<TransUnitTest>( TransUnitTest.class, metaStore, PentahoDefaults.NAMESPACE);
    testFactory.saveElement( unitTest );
    
    // Reload the whole thing...
    factories = new FactoriesHierarchy( metaStore, Arrays.asList( databaseMeta ) );
    
    DataSetGroup verifyGroup = factories.getGroupFactory().loadElement(dataSetGroup.getName());
    assertNotNull(verifyGroup.getDatabaseMeta());
  }

  @Override
  protected void tearDown() throws Exception {
    // Clean up the data sets database...
    //
    new File("/tmp/datasets.h2.db").delete();
    new File("/tmp/datasets.trace.db").delete();
    
  }
  
  private void createInputDataSet() throws KettleException {
    List<DataSetField> fields = new ArrayList<>();
    fields.add( new DataSetField( "a", "column_a", ValueMetaInterface.TYPE_STRING, 20, 0, null ) );
    fields.add( new DataSetField( "b", "column_b", ValueMetaInterface.TYPE_STRING, 20, 0, null ) );
    fields.add( new DataSetField( "c", "column_c", ValueMetaInterface.TYPE_STRING, 20, 0, null ) );

    List<Object[]> rows = new ArrayList<Object[]>();
    rows.add( new Object[] { "a1", "b1", "c1",  });
    rows.add( new Object[] { "a2", "b2", "c2",  });
    rows.add( new Object[] { "a3", "b3", "c3",  });
    
    inputDataSet = DataSetConst.writeDataSet( INPUT_SET_NAME, INPUT_SET_DESC, dataSetGroup, INPUT_SET_TABLE, fields, rows );
  }

  private void createGoldenDataSet() throws KettleException {
    List<DataSetField> fields = new ArrayList<>();
    fields.add( new DataSetField( "a", "column_a", ValueMetaInterface.TYPE_STRING, 20, 0, null ) );
    fields.add( new DataSetField( "b", "column_b", ValueMetaInterface.TYPE_STRING, 20, 0, null ) );
    fields.add( new DataSetField( "c", "column_c", ValueMetaInterface.TYPE_STRING, 20, 0, null ) );
    fields.add( new DataSetField( "d", "column_d", ValueMetaInterface.TYPE_INTEGER, 6, 0, null ) );

    List<Object[]> rows = new ArrayList<Object[]>();
    rows.add( new Object[] { "a1", "b1", "c1", Long.valueOf( 123456), });
    rows.add( new Object[] { "a2", "b2", "c2", Long.valueOf( 123456), });
    rows.add( new Object[] { "a3", "b3", "c3", Long.valueOf( 123456), });
    
    goldenDataSet = DataSetConst.writeDataSet( GOLDEN_SET_NAME, GOLDEN_SET_DESC, dataSetGroup, GOLDEN_SET_TABLE, fields, rows );
  }

  
  private void createUnitTest() {
    
    List<TransUnitTestSetLocation> inputs = new ArrayList<TransUnitTestSetLocation>();
    inputs.add( new TransUnitTestSetLocation("Input", INPUT_SET_NAME, Arrays.asList( 
        new TransUnitTestFieldMapping( "a", "a", "1" ),
        new TransUnitTestFieldMapping( "b", "b", "2" ),
        new TransUnitTestFieldMapping( "c", "c", "3" )
    )) );
    
    List<TransUnitTestSetLocation> goldens = new ArrayList<TransUnitTestSetLocation>();
    goldens.add( new TransUnitTestSetLocation("Output", GOLDEN_SET_NAME, Arrays.asList( 
        new TransUnitTestFieldMapping( "a", "a", "1" ),
        new TransUnitTestFieldMapping( "b", "b", "2" ),
        new TransUnitTestFieldMapping( "c", "c", "3" ),
        new TransUnitTestFieldMapping( "d", "d", "4" )
    )) );
    
    unitTest = new TransUnitTest(UNIT_TEST_NAME, UNIT_TEST_DESCRIPTION, null, null, "test-files/simple-mapping.ktr", inputs, goldens);
  }

  public void testExecution() throws Exception {

    List<Class<? extends ExtensionPointInterface>> pluginClasses = Arrays.asList(
      ChangeTransMetaPriorToExecutionExtensionPoint.class,
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

    TransMeta transMeta = new TransMeta( unitTest.getTransFilename() );
    transMeta.setSharedObjects(sharedObjects);
    transMeta.addDatabase( databaseMeta );

    // Enable data sets
    //
    transMeta.setVariable( DataSetConst.VAR_STEP_DATASET_ENABLED, "Y" );
    
    // This will cause the transformation to be a unit test...
    //
    transMeta.setVariable( DataSetConst.VAR_UNIT_TEST_NAME, unitTest.getName() );

    // pass our metastore reference
    //
    transMeta.setMetaStore( metaStore );

    // Create an in-memory data-set group and data sets to test with...
    //
    StepMeta stepMeta = transMeta.findStep( "Input" );
    stepMeta.setAttribute( DataSetConst.ATTR_GROUP_DATASET, DataSetConst.ATTR_STEP_DATASET_INPUT, inputDataSet.getName() );

    Trans trans = new Trans( transMeta );
    trans.setPreview( true ); // data set only works in preview right now
    trans.execute( null );
    trans.waitUntilFinished();
  }
}

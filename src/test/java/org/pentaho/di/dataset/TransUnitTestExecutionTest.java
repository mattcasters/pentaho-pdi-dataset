package org.pentaho.di.dataset;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.KettleClientEnvironment;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.dataset.spoon.xtpoint.ChangeTransMetaPriorToExecutionExtensionPoint;
import org.pentaho.di.dataset.spoon.xtpoint.InjectDataSetIntoTransExtensionPoint;
import org.pentaho.di.dataset.spoon.xtpoint.RowCollection;
import org.pentaho.di.dataset.spoon.xtpoint.ValidateTransUnitTestExtensionPoint;
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

  private static final String INPUT_STEP_NAME = "Input";
  private static final String OUTPUT_STEP_NAME = "Output";

  public static final String NAMESPACE = "test";

  public static final DataSetGroupType GROUP_TYPE = DataSetGroupType.Database;
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
  protected int setSize;

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
    
    dataSetGroup = new DataSetGroup( GROUP_TYPE, GROUP_NAME, GROUP_DESC, databaseMeta, GROUP_SCHEMA );

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

    // Add the fields in a different order to see if we can correctly compare data against it!
    //
    fields.add( new DataSetField( "d", "column_d", ValueMetaInterface.TYPE_INTEGER, 6, 0, null ) );
    fields.add( new DataSetField( "c", "column_c", ValueMetaInterface.TYPE_STRING, 20, 0, null ) );
    fields.add( new DataSetField( "b", "column_b", ValueMetaInterface.TYPE_STRING, 20, 0, null ) );
    fields.add( new DataSetField( "a", "column_a", ValueMetaInterface.TYPE_STRING, 20, 0, null ) );

    List<Object[]> rows = new ArrayList<Object[]>();
    rows.add( new Object[] { 123456L, "c1", "b1", "a1",  });
    rows.add( new Object[] { 123456L, "c2", "b2", "a2",  });
    rows.add( new Object[] { 123456L, "c3", "b3", "a3",  });

    goldenDataSet = DataSetConst.writeDataSet( GOLDEN_SET_NAME, GOLDEN_SET_DESC, dataSetGroup, GOLDEN_SET_TABLE, fields, rows );
    setSize = rows.size();
  }

  
  private void createUnitTest() {
    
    List<TransUnitTestSetLocation> inputs = new ArrayList<TransUnitTestSetLocation>();
    inputs.add( new TransUnitTestSetLocation(INPUT_STEP_NAME, INPUT_SET_NAME, Arrays.asList( 
          new TransUnitTestFieldMapping( "a", "a" ),
          new TransUnitTestFieldMapping( "b", "b" ),
          new TransUnitTestFieldMapping( "c", "c" )), 
        Arrays.asList("a", "b", "c")) );
    
    List<TransUnitTestSetLocation> goldens = new ArrayList<TransUnitTestSetLocation>();
    goldens.add( new TransUnitTestSetLocation(OUTPUT_STEP_NAME, GOLDEN_SET_NAME, Arrays.asList( 
        new TransUnitTestFieldMapping( "a", "a" ),
        new TransUnitTestFieldMapping( "b", "b" ),
        new TransUnitTestFieldMapping( "c", "c" ),
        new TransUnitTestFieldMapping( "d", "d" )), 
        Arrays.asList("a", "b", "c")) );
    
    List<TransUnitTestTweak> tweaks = new ArrayList<TransUnitTestTweak>();
    tweaks.add( new TransUnitTestTweak(TransTweak.NONE, "step1") );
    tweaks.add( new TransUnitTestTweak(TransTweak.BYPASS_STEP, "step2") );
    tweaks.add( new TransUnitTestTweak(TransTweak.REMOVE_STEP, "step3") );
    
    unitTest = new TransUnitTest(UNIT_TEST_NAME, UNIT_TEST_DESCRIPTION, null, null, 
        "src/test/resources/simple-mapping.ktr", inputs, goldens, tweaks, TestType.UNIT_TEST, null, new ArrayList<TransUnitTestDatabaseReplacement>(), false);
  }

  public void testExecution() throws Exception {

    List<Class<? extends ExtensionPointInterface>> pluginClasses = Arrays.asList(
      ChangeTransMetaPriorToExecutionExtensionPoint.class,
      InjectDataSetIntoTransExtensionPoint.class,
      ValidateTransUnitTestExtensionPoint.class
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
    
    // Enable unit test validation
    //
    transMeta.setVariable( DataSetConst.VAR_RUN_UNIT_TEST, "Y" );
    transMeta.setVariable( DataSetConst.VAR_UNIT_TEST_NAME, unitTest.getName() );

    // pass our metastore reference
    //
    transMeta.setMetaStore( metaStore );

    // Create an in-memory data-set group and data sets to test with...
    //
    StepMeta stepMeta = transMeta.findStep( INPUT_STEP_NAME );

    Trans trans = new Trans( transMeta );
    trans.setPreview( true ); // data set only works in preview right now
    trans.execute( null );
    trans.waitUntilFinished();
    
    // All OK?  Did we read rows in the output step?
    //
    Result result = trans.getResult();
    assertTrue(result.getResult());
    assertEquals(0, result.getNrErrors());
    assertEquals(setSize, result.getNrLinesRead());
    
    @SuppressWarnings( "unchecked" )
    Map<String, RowCollection> collectionMap = (Map<String, RowCollection>) trans.getExtensionDataMap().get( DataSetConst.ROW_COLLECTION_MAP );
    assertNotNull(collectionMap);
    
    int rowNumber = 0;
    for (TransUnitTestSetLocation location : unitTest.getGoldenDataSets()) {
      RowCollection resultCollection = collectionMap.get( location.getStepname() );
      RowMetaInterface stepFieldsRowMeta = transMeta.getStepFields(stepMeta);

      DataSet goldenDataSet = unitTest.getGoldenDataSet( trans.getLogChannel(), factories, location);
      assertNotNull( "Golden data set not found!", goldenDataSet );

      List<Object[]> goldenRows = goldenDataSet.getAllRows( trans.getLogChannel(), location );
      RowMetaInterface goldenRowMeta = goldenDataSet.getMappedDataSetFieldsRowMeta( location );

      assertEquals(OUTPUT_STEP_NAME, location.getStepname());

      List<Object[]> resultRows = resultCollection.getRows();

      if ( resultRows.size() != goldenRows.size() ) {
        throw new KettleException( "Incorrect number of rows received from step, golden data set '" + goldenDataSet.getName() + "' has " + goldenRows.size() + " rows in it and we received "+resultRows.size() );
      }
      
      final int[] stepFieldIndices = new int[location.getFieldMappings().size()];
      final int[] goldenIndices = new int[location.getFieldMappings().size()];
      for ( int i = 0; i < location.getFieldMappings().size(); i++ ) {
        TransUnitTestFieldMapping fieldMapping = location.getFieldMappings().get( i );

        stepFieldIndices[i] = resultCollection.getRowMeta().indexOfValue( fieldMapping.getStepFieldName() );
        if (stepFieldIndices[i]<0) {
          throw new KettleException( "Unable to find field name '"+fieldMapping.getStepFieldName()+"' in step results rows output: "+Arrays.toString(resultCollection.getRowMeta().getFieldNames()) );
        }
        goldenIndices[i] = goldenRowMeta.indexOfValue( fieldMapping.getDataSetFieldName() );
        if (goldenIndices[i]<0) {
          throw new KettleException( "Unable to find data set field name '"+fieldMapping.getDataSetFieldName()+"' in golden data set rows : "+Arrays.toString(goldenRowMeta.getFieldNames()) );
        }
      }
      
      Object[] resultRow = resultRows.get( rowNumber );
      Object[] goldenRow = goldenRows.get( rowNumber );
      rowNumber++;
      
      // Now compare the input to the golden row
      //
      for ( int i = 0; i < location.getFieldMappings().size(); i++ ) {
        ValueMetaInterface stepValueMeta = resultCollection.getRowMeta().getValueMeta( stepFieldIndices[i] );
        Object stepValue = resultRow[stepFieldIndices[i]];

        ValueMetaInterface goldenValueMeta = goldenRowMeta.getValueMeta( goldenIndices[i] );
        Object goldenValue = goldenRow[goldenIndices[i]];
        try {
          int cmp = stepValueMeta.compare( stepValue, goldenValueMeta, goldenValue );
          if ( cmp != 0 ) {
            throw new KettleStepException( "Validation againt golden data failed for row number " + rowNumber
              + ": step value [" + stepValueMeta.getString( stepValue ) + "] does not correspond to data set value [" + goldenValueMeta.getString( goldenValue ) + "]" );
          }
        } catch ( KettleValueException e ) {
          throw new KettleStepException( "Unable to compare step data against golden data set '" + goldenDataSet.getName() + "'", e );
        }
      }
    }
  }
}

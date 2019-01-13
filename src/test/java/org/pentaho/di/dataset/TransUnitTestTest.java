package org.pentaho.di.dataset;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.pentaho.di.core.KettleClientEnvironment;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.stores.memory.MemoryMetaStore;
import org.pentaho.metastore.util.PentahoDefaults;

import junit.framework.TestCase;

/** 
 * Test serialization, constructor, ... 
 * */
public class TransUnitTestTest extends TestCase {

  private static final String NAME = "input1";
  private static final String DESCRIPTION = "input-set-1";

  protected IMetaStore metaStore;
  protected TransUnitTest test;

  protected List<TransUnitTestSetLocation> inputs;
  protected List<TransUnitTestSetLocation> goldens;
  
  @Override
  protected void setUp() throws Exception {
    KettleClientEnvironment.init();
    metaStore = new MemoryMetaStore();

    inputs = new ArrayList<TransUnitTestSetLocation>();
    inputs.add( new TransUnitTestSetLocation( "input-step1", "data-set-name1", 
        Arrays.asList( 
            new TransUnitTestFieldMapping( "fieldA", "setFieldA"),
            new TransUnitTestFieldMapping( "fieldB", "setFieldB"),
            new TransUnitTestFieldMapping( "fieldC", "setFieldC")            
          ), Arrays.asList("order1", "order2", "order3")));
    inputs.add( new TransUnitTestSetLocation( "input-step2", "data-set-name2", 
        Arrays.asList( 
            new TransUnitTestFieldMapping( "fieldX", "setFieldX"),
            new TransUnitTestFieldMapping( "fieldY", "setFieldY"),
            new TransUnitTestFieldMapping( "fieldW", "setFieldW"),
            new TransUnitTestFieldMapping( "fieldZ", "setFieldZ")           
           ), Arrays.asList("order1", "order2", "order3", "order4")));
    
    goldens = new ArrayList<TransUnitTestSetLocation>();
    
    List<TransUnitTestTweak> tweaks = new ArrayList<TransUnitTestTweak>();
    tweaks.add( new TransUnitTestTweak(TransTweak.NONE, "step1") );
    tweaks.add( new TransUnitTestTweak(TransTweak.BYPASS_STEP, "step2") );
    tweaks.add( new TransUnitTestTweak(TransTweak.REMOVE_STEP, "step3") );
    
    test = new TransUnitTest(NAME, DESCRIPTION, 
        null, null, "sometrans.ktr",
        inputs,
        goldens,
        tweaks,
        TestType.UNIT_TEST,
        null, new ArrayList<TransUnitTestDatabaseReplacement>(),
        false
        );
  }

  @Test
  public void testConstructor() {

    assertEquals( NAME, test.getName() );
    assertEquals( DESCRIPTION, test.getDescription() );
    
  }

  @Test
  public void testSerialisation() throws Exception {
    MetaStoreFactory<TransUnitTest> testFactory = new MetaStoreFactory<TransUnitTest>( TransUnitTest.class, metaStore, PentahoDefaults.NAMESPACE);

    // Save the test...
    //
    testFactory.saveElement( test );


    // Load it back up...
    //
    TransUnitTest verify = testFactory.loadElement( NAME );

    // Verify if everything is still the same...
    //
    assertEquals( NAME, verify.getName() );
    assertEquals( DESCRIPTION, verify.getDescription() );

    List<TransUnitTestSetLocation> verifyInputs = verify.getInputDataSets();
    List<TransUnitTestSetLocation> verifyGoldens = verify.getGoldenDataSets();
    
    assertEquals( inputs.size(), verifyInputs.size());
    for (int i=0;i<inputs.size();i++) {
      assertEquals(inputs.get(i).getStepname(), verifyInputs.get(i).getStepname());
      assertEquals(inputs.get(i).getDataSetName(), verifyInputs.get(i).getDataSetName());
      assertEquals(inputs.get(i).getFieldMappings().size(), verifyInputs.get(i).getFieldMappings().size());
    }
    assertEquals( goldens.size(), verifyGoldens.size());
    for (int i=0;i<goldens.size();i++) {
      assertEquals(goldens.get(i).getStepname(), verifyGoldens.get(i).getStepname());
      assertEquals(goldens.get(i).getDataSetName(), verifyGoldens.get(i).getDataSetName());
      assertEquals(goldens.get(i).getFieldMappings().size(), verifyGoldens.get(i).getFieldMappings().size());
    }
    
  }
}

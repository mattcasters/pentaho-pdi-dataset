package org.pentaho.di.dataset.trans;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.List;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.dataset.TransUnitTest;
import org.pentaho.di.dataset.TransUnitTestSetLocation;
import org.pentaho.di.dataset.util.DataSetConst;
import org.pentaho.di.dataset.util.FactoriesHierarchy;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.dummytrans.DummyTransMeta;
import org.pentaho.di.trans.steps.injector.InjectorMeta;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

@ExtensionPoint(
  extensionPointId = "TransformationPrepareExecution",
  id = "ChangeTransMetaPriorToExecutionExtensionPoint",
  description = "Change the transformation metadata in Trans prior to execution preperatio but only during execution in Spoon" )
public class ChangeTransMetaPriorToExecutionExtensionPoint implements ExtensionPointInterface {

  @Override
  public void callExtensionPoint( LogChannelInterface log, Object object ) throws KettleException {
    if ( !( object instanceof Trans ) ) {
      return;
    }

    Trans trans = (Trans) object;
    TransMeta transMeta = trans.getTransMeta();

    boolean runUnitTest = "Y".equalsIgnoreCase( transMeta.getVariable( DataSetConst.VAR_RUN_UNIT_TEST ) );
    if (!runUnitTest) {
      // No business here...
      return;
    }
    String unitTestName = transMeta.getAttribute( DataSetConst.ATTR_GROUP_DATASET, DataSetConst.ATTR_TRANS_SELECTED_UNIT_TEST_NAME );
    
    TransUnitTest unitTest = null;
    FactoriesHierarchy factoriesHierarchy = null;
    
    // TODO: The next 2 lines are very expensive: see how we can cache this or move it upstairs somewhere.
    //
    if (!Const.isEmpty( unitTestName )) {
      List<DatabaseMeta> databases = DataSetConst.getAvailableDatabases( transMeta.getRepository(), transMeta.getSharedObjects() );
      
      try {
        factoriesHierarchy = new FactoriesHierarchy( transMeta.getMetaStore(), databases );
        unitTest = factoriesHierarchy.getTestFactory().loadElement( unitTestName );
      } catch(MetaStoreException e) {
        throw new KettleException("Unable to load unit test '"+unitTestName+"'", e);
      }
    }

    // OK, so now replace an input step with a data set attached with an Injector step...
    // However, we don't want to have the user see this so we need to copy trans.transMeta first...
    //
    // Clone seems to has problems so we'll take the long (XML) way around...
    InputStream stream;
    try {
      stream = new ByteArrayInputStream( transMeta.getXML().getBytes(Const.XML_ENCODING) );
    } catch ( UnsupportedEncodingException e ) {
      throw new KettleException( "Encoding error", e );
    }
    TransMeta copyTransMeta = new TransMeta(stream, transMeta.getRepository(), true, transMeta, null);
    
    // Lets's simply replace the inputs with an Injector Step
    //
    for (int i=0;i<copyTransMeta.nrSteps();i++) {
      StepMeta stepMeta = copyTransMeta.getStep( i );
      String dataSetName = stepMeta.getAttribute( DataSetConst.ATTR_GROUP_DATASET, DataSetConst.ATTR_STEP_DATASET_INPUT );
      
      // See if there's a unit test if the step isn't flagged...
      //
      if ( unitTest!=null && Const.isEmpty( dataSetName ) ) {
        TransUnitTestSetLocation inputLocation = unitTest.findInputLocation( stepMeta.getName() );
        if (inputLocation!=null) {
          dataSetName = inputLocation.getDataSetName();
        }
      }
      
      if ( !Const.isEmpty( dataSetName ) ) {
        // OK, this step needs to be replaced by an Injector step...
        //
        RowMetaInterface stepFields = copyTransMeta.getStepFields( stepMeta );
        
        InjectorMeta injectorMeta = new InjectorMeta();
        injectorMeta.allocate( stepFields.size() );
        for (int x=0;x<stepFields.size();x++) {
          injectorMeta.getFieldname()[x] = stepFields.getValueMeta( x ).getName();
          injectorMeta.getType()[x] = stepFields.getValueMeta( x ).getType();
          injectorMeta.getLength()[x] = stepFields.getValueMeta( x ).getLength();
          injectorMeta.getPrecision()[x] = stepFields.getValueMeta( x ).getPrecision();
          
          // Only the step metadata, type...
          stepMeta.setStepMetaInterface( injectorMeta );
          stepMeta.setStepID( PluginRegistry.getInstance().getPluginId( StepPluginType.class, injectorMeta) );          
        }        
      }
      
      // Capture golden data in a dummy step instead of the regular one?
      //
      if ( unitTest!=null ) {
        TransUnitTestSetLocation goldenLocation = unitTest.findGoldenLocation( stepMeta.getName() );
        if (goldenLocation!=null) {
          String goldenDataSetName = goldenLocation.getDataSetName();
          if (!Const.isEmpty(goldenDataSetName)) {
            DummyTransMeta dummyTransMeta = new DummyTransMeta();
            stepMeta.setStepMetaInterface( dummyTransMeta );
            stepMeta.setStepID( PluginRegistry.getInstance().getPluginId( StepPluginType.class, dummyTransMeta) );
          }
        }
      }
      
    }
    
    // Pass metadata references...
    //
    copyTransMeta.setRepository( transMeta.getRepository() );
    copyTransMeta.setMetaStore( transMeta.getMetaStore() );
    copyTransMeta.setSharedObjects( transMeta.getSharedObjects() );
    
    // Now replace the metadata in the Trans object...
    //
    trans.setTransMeta( copyTransMeta );
    
    /*
    try {
      File ktr = new File("/tmp/unit-test-trans.ktr" );
      FileOutputStream fos = new FileOutputStream( ktr );
      fos.write( XMLHandler.getXMLHeader().getBytes() );
      fos.write( copyTransMeta.getXML().getBytes() );
      fos.close();    
    } catch(Exception e) {
      throw new KettleException("Error writing /tmp/unit-test-trans.ktr", e);
    }
    */
  }

}

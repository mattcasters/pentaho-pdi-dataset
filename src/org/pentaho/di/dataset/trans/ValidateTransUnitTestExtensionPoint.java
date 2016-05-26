package org.pentaho.di.dataset.trans;

import java.util.List;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.dataset.TransUnitTest;
import org.pentaho.di.dataset.util.DataSetConst;
import org.pentaho.di.dataset.util.FactoriesHierarchy;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.metastore.api.IMetaStore;

/**
 * 
 * @author matt
 *
 */
@ExtensionPoint(
  extensionPointId = "TransformationFinish",
  id = "ValidateTransUnitTestExtensionPoint",
  description = "Inject a bunch of rows into a step during preview" )
public class ValidateTransUnitTestExtensionPoint implements ExtensionPointInterface {

  @Override
  public void callExtensionPoint( LogChannelInterface log, Object object ) throws KettleException {
    if ( !( object instanceof Trans ) ) {
      return;
    }

    final Trans trans = (Trans) object;
    final TransMeta transMeta = trans.getTransMeta();
    boolean runUnitTest = "Y".equalsIgnoreCase( transMeta.getVariable( DataSetConst.VAR_RUN_UNIT_TEST ) );
    
    if ( !runUnitTest ) {
      return;
    }

    // We should always have a unit test name here...
    String unitTestName = transMeta.getAttribute( DataSetConst.ATTR_GROUP_DATASET, DataSetConst.ATTR_TRANS_SELECTED_UNIT_TEST_NAME );
    if (Const.isEmpty( unitTestName )) {
      return;
    }
    
    try {
      IMetaStore metaStore = transMeta.getMetaStore();
      Repository repository = transMeta.getRepository();

      if ( metaStore == null ) {
        return; // Nothing to do here, we can't reference data sets.
      }

      // TODO: The next 2 lines are very expensive: see how we can cache this or move it upstairs somewhere.
      //
      List<DatabaseMeta> databases = DataSetConst.getAvailableDatabases( repository, transMeta.getSharedObjects() );
      FactoriesHierarchy factoriesHierarchy = new FactoriesHierarchy( metaStore, databases );

      // If the transformation has a variable set with the unit test in it, we're dealing with a unit test situation.
      //
      TransUnitTest unitTest = factoriesHierarchy.getTestFactory().loadElement( unitTestName );
      
      // Validate execution results with what's in the data sets...
      //
      try {
        DataSetConst.validateTransResultAgainstUnitTest( trans, unitTest, factoriesHierarchy );
        trans.getLogChannel().logBasic( "Unit test '"+unitTest.getName()+"' passed succesfully" );
      } catch(Exception e) {
        throw new KettleException( "Unable to validate against golden data for unit test '"+unitTest.getName()+"'", e );
      }
      
    } catch ( Throwable e ) {
      throw new KettleException( "Unable to validate unit test/golden rows", e );
    }

  }
}

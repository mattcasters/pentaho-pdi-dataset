package org.pentaho.di.dataset.spoon.xtpoint;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.dataset.util.DataSetConst;
import org.pentaho.di.trans.TransMeta;

@ExtensionPoint(
  extensionPointId = "SpoonTransMetaExecutionStart",
  id = "SpoonFlagUnitTestExtensionPoint",
  description = "Change the transformation metadata in Trans prior to execution preperatio but only during execution in Spoon" )
public class SpoonFlagUnitTestExtensionPoint implements ExtensionPointInterface {

  @Override
  public void callExtensionPoint( LogChannelInterface log, Object object ) throws KettleException {
    if ( !( object instanceof TransMeta ) ) {
      return;
    }
    
    TransMeta transMeta = (TransMeta) object;
    
    String unitTestName = transMeta.getAttribute( DataSetConst.ATTR_GROUP_DATASET, DataSetConst.ATTR_TRANS_SELECTED_UNIT_TEST_NAME );
    
    if (!Const.isEmpty( unitTestName )) {
      // We're running in Spoon and there's a unit test selected : test it
      //
      System.out.println( "==== Running unit test on this transformation ====" );
      transMeta.setVariable( DataSetConst.VAR_RUN_UNIT_TEST, "Y" );
    }
  }

}

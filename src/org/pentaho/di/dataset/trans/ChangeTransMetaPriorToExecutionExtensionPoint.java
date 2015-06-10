package org.pentaho.di.dataset.trans;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.dataset.util.DataSetConst;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;

@ExtensionPoint(
  extensionPointId = "TransformationPrepareExecution",
  id = "ChangeTransMetaPriorToExecutionExtensionPoint",
  description = "Change the transformation metadata in Trans prior to execution preperation" )
public class ChangeTransMetaPriorToExecutionExtensionPoint implements ExtensionPointInterface {

  @Override
  public void callExtensionPoint( LogChannelInterface log, Object object ) throws KettleException {
    if ( !( object instanceof Trans ) ) {
      return;
    }

    Trans trans = (Trans) object;
    TransMeta transMeta = trans.getTransMeta();

    String strEnabled = transMeta.getVariable( DataSetConst.VAR_STEP_DATASET_ENABLED );
    boolean dataSetEnabled = "Y".equalsIgnoreCase( strEnabled );

    if ( !trans.isPreview() && !dataSetEnabled ) {
      return;
    }

    // OK, so now replace an input step with a data set attached with an Injector step...
    // However, we don't want to have the user see this so we need to copy trans.transMeta first...
    //
    transMeta.clone();

  }

}

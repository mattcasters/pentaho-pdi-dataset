package org.pentaho.di.dataset.trans;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.dataset.util.DataSetConst;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepInitThread;
import org.pentaho.di.trans.step.StepMetaDataCombi;

@ExtensionPoint(
  extensionPointId = "StepBeforeInitialize",
  id = "IgnoreInitOfDataSetInputStepExtensionPoint",
  description = "Ignore step initialization in case we want to replace the step output with a data set" )
public class IgnoreInitOfDataSetInputStepExtensionPoint implements ExtensionPointInterface {

  @Override
  public void callExtensionPoint( LogChannelInterface log, Object object ) throws KettleException {
    if ( !( object instanceof StepInitThread ) ) {
      return;
    }

    StepInitThread sit = (StepInitThread) object;

    final StepMetaDataCombi combi = sit.getCombi();
    final Trans trans = combi.step.getTrans();
    final TransMeta transMeta = trans.getTransMeta();
    String strEnabled = transMeta.getVariable( DataSetConst.VAR_STEP_DATASET_ENABLED );
    boolean dataSetEnabled = "Y".equalsIgnoreCase( strEnabled );

    if ( !trans.isPreview() && !dataSetEnabled ) {
      return;
    }

    // We don't want to initialize this step since we're throwing it away right after init anyway
    // The step might also not yet be configured for proper execution
    // For example if a database or a table doesn't exist yet.
    //
    sit.doIt = false;
    sit.ok = true;
  }

}

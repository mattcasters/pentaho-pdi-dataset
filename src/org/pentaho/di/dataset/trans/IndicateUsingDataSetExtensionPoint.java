package org.pentaho.di.dataset.trans;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.dataset.util.DataSetConst;
import org.pentaho.di.trans.TransExecutionConfiguration;

@ExtensionPoint(
  extensionPointId = "SpoonTransExecutionConfiguration",
  id = "IndicateUsingDataSetExtensionPoint",
  description = "Set a variable in the transformation to allow our other extension point" )
public class IndicateUsingDataSetExtensionPoint implements ExtensionPointInterface {

  @Override
  public void callExtensionPoint( LogChannelInterface log, Object object ) throws KettleException {
    if ( !( object instanceof TransExecutionConfiguration ) ) {
      return;
    }

    TransExecutionConfiguration transExecutionConfiguration = (TransExecutionConfiguration) object;

    transExecutionConfiguration.getVariables().put( DataSetConst.VAR_STEP_DATASET_ENABLED, "Y" );
  }

}

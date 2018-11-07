package org.pentaho.di.dataset.spoon.xtpoint;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.dataset.spoon.DataSetHelper;
import org.pentaho.di.trans.TransMeta;

@ExtensionPoint(
  extensionPointId = "TransAfterClose",
  id = "SpoonTransAfterClose",
  description = "Cleanup the active unit test for the closed transformation"
)
public class SpoonTransAfterClose implements ExtensionPointInterface {

  @Override public void callExtensionPoint( LogChannelInterface log, Object object ) throws KettleException {
    if (!(object instanceof TransMeta )) {
      return;
    }
    DataSetHelper.getInstance().getActiveTests().remove( (TransMeta)object );
  }
}

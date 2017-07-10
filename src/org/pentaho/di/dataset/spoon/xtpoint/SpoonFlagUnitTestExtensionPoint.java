/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

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

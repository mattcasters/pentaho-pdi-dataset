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

import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.dataset.TransUnitTest;
import org.pentaho.di.dataset.util.DataSetConst;
import org.pentaho.di.dataset.util.FactoriesHierarchy;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

import java.io.OutputStream;
import java.util.List;

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
    if ( !runUnitTest ) {
      // No business here...
      if ( log.isDetailed() ) {
        log.logDetailed( "Not running unit test..." );
      }
      return;
    }
    String unitTestName = trans.getVariable( DataSetConst.VAR_UNIT_TEST_NAME );

    // Do we have something to work with?
    // Unit test disabled?  Github issue #5
    //
    if ( StringUtils.isEmpty( unitTestName ) ) {
      if ( log.isDetailed() ) {
        log.logDetailed( "Unit test disabled." );
      }
      return;
    }

    TransUnitTest unitTest = null;
    FactoriesHierarchy factoriesHierarchy = null;


    // The next factory hierarchy initialization is very expensive. 
    // See how we can cache this or move it upstairs somewhere.
    //
    List<DatabaseMeta> databases = DataSetConst.getAvailableDatabases( transMeta.getRepository(), transMeta.getSharedObjects() );
    try {
      factoriesHierarchy = new FactoriesHierarchy( transMeta.getMetaStore(), databases );
      unitTest = factoriesHierarchy.getTestFactory().loadElement( unitTestName );
    } catch ( MetaStoreException e ) {
      throw new KettleException( "Unable to load unit test '" + unitTestName + "'", e );
    }

    if ( unitTest == null ) {
      throw new KettleException( "Unit test '" + unitTestName + "' was not found or could not be loaded" );
    }

    // Get a modified copy of the transformation using the unit test information
    //
    TransMetaModifier modifier = new TransMetaModifier( transMeta, unitTest );
    TransMeta copyTransMeta = modifier.getTestTransformation( log, trans, factoriesHierarchy );


    // Now replace the metadata in the Trans object...
    //
    trans.setTransMeta( copyTransMeta );

    String testFilename = trans.environmentSubstitute( unitTest.getFilename() );
    if ( !StringUtil.isEmpty( testFilename ) ) {
      try {
        OutputStream os = KettleVFS.getOutputStream( testFilename, false );
        os.write( XMLHandler.getXMLHeader().getBytes() );
        os.write( copyTransMeta.getXML().getBytes() );
        os.close();
      } catch ( Exception e ) {
        throw new KettleException( "Error writing test filename to '" + testFilename + "'", e );
      }
    }
  }
}

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

import java.util.ArrayList;
import java.util.List;

import org.eclipse.swt.SWT;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.dataset.TransUnitTest;
import org.pentaho.di.dataset.UnitTestResult;
import org.pentaho.di.dataset.util.DataSetConst;
import org.pentaho.di.dataset.util.FactoriesHierarchy;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.ui.core.dialog.PreviewRowsDialog;
import org.pentaho.di.ui.spoon.Spoon;
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
    String unitTestName = transMeta.getVariable( DataSetConst.VAR_UNIT_TEST_NAME );
    if (StringUtil.isEmpty( unitTestName )) {
      return;
    }
    
    try {
      IMetaStore metaStore = transMeta.getMetaStore();
      Repository repository = transMeta.getRepository();

      if ( metaStore == null ) {
        return; // Nothing to do here, we can't reference data sets.
      }

      List<DatabaseMeta> databases = DataSetConst.getAvailableDatabases( repository, transMeta.getSharedObjects() );
      FactoriesHierarchy factoriesHierarchy = new FactoriesHierarchy( metaStore, databases );

      // If the transformation has a variable set with the unit test in it, we're dealing with a unit test situation.
      //
      TransUnitTest unitTest = factoriesHierarchy.getTestFactory().loadElement( unitTestName );
      
      final List<UnitTestResult> results = new ArrayList<UnitTestResult>();
      trans.getExtensionDataMap().put(DataSetConst.UNIT_TEST_RESULTS, results);
      
      
      // Validate execution results with what's in the data sets...
      //
      int errors = DataSetConst.validateTransResultAgainstUnitTest( trans, unitTest, factoriesHierarchy, results );
      if (errors==0) {
        log.logBasic( "Unit test '"+unitTest.getName()+"' passed succesfully" );
      } else {
        log.logBasic( "Unit test '"+unitTest.getName()+"' failed, "+errors+" errors detected, "+results.size()+" comments to report." );
        
        String dontShowResults = transMeta.getVariable(DataSetConst.VAR_DO_NOT_SHOW_UNIT_TEST_ERRORS, "N");
        
        final Spoon spoon = Spoon.getInstance();
        if (spoon!=null && "N".equalsIgnoreCase(dontShowResults)) {
          spoon.getShell().getDisplay().asyncExec(new Runnable() {
            @Override
            public void run() {
              PreviewRowsDialog dialog = new PreviewRowsDialog(spoon.getShell(), trans, SWT.NONE, 
                  "Unit test results", 
                  UnitTestResult.getRowMeta(), 
                  UnitTestResult.getRowData(results));
              dialog.setDynamic(false);
              dialog.setProposingToGetMoreRows(false);
              dialog.setProposingToStop(false);
              dialog.setTitleMessage("Unit test results", "Here are the results of the unit test validations:");
              dialog.open();
            }
          });
        }
      }
      log.logBasic( "----------------------------------------------" );
      for (UnitTestResult result : results) {
        if (result.getDataSetName()!=null) {
          log.logBasic(result.getStepName()+" - "+result.getDataSetName()+" : "+result.getComment());
        } else {
          log.logBasic(result.getComment());
        }
      }
      log.logBasic( "----------------------------------------------" );
    } catch ( Throwable e ) {
      log.logError( "Unable to validate unit test/golden rows", e );
    }

  }
}

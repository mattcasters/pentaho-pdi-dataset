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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.dataset.DataSet;
import org.pentaho.di.dataset.TransTweak;
import org.pentaho.di.dataset.TransUnitTest;
import org.pentaho.di.dataset.TransUnitTestDatabaseReplacement;
import org.pentaho.di.dataset.TransUnitTestSetLocation;
import org.pentaho.di.dataset.util.DataSetConst;
import org.pentaho.di.dataset.util.FactoriesHierarchy;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransHopMeta;
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
      if (log.isDetailed()) {
        log.logDetailed("Not running unit test...");
      }
      return;
    }
    String unitTestName = transMeta.getAttribute( DataSetConst.ATTR_GROUP_DATASET, DataSetConst.ATTR_TRANS_SELECTED_UNIT_TEST_NAME );
    
    // Do we have something to work with?
    // Unit test disabled?  Github issue #5
    //
    if (StringUtils.isEmpty( unitTestName )) {
      if (log.isDetailed()) {
        log.logDetailed("Unit test disabled.");
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
    } catch(MetaStoreException e) {
      throw new KettleException("Unable to load unit test '"+unitTestName+"'", e);
    }
  
    
    if (unitTest==null) {
      throw new KettleException("Unit test '"+unitTestName+"' was not found or could not be loaded");
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
    // Pass the metadata references...
    //
    copyTransMeta.setRepository( transMeta.getRepository() );
    copyTransMeta.setMetaStore( transMeta.getMetaStore() );
    copyTransMeta.setSharedObjects( transMeta.getSharedObjects() );
    
    // Replace certain connections with another
    //
    for (TransUnitTestDatabaseReplacement dbReplacement : unitTest.getDatabaseReplacements()) {
      String sourceDatabaseName = transMeta.environmentSubstitute(dbReplacement.getOriginalDatabaseName());
      String replacementDatabaseName = transMeta.environmentSubstitute(dbReplacement.getReplacementDatabaseName());
      
      DatabaseMeta sourceDatabaseMeta = copyTransMeta.findDatabase(sourceDatabaseName);
      DatabaseMeta replacementDatabaseMeta = copyTransMeta.findDatabase(replacementDatabaseName);
      if (sourceDatabaseMeta==null) {
        throw new KettleException("Unable to find source database connection '"+sourceDatabaseName+"', can not be replaced");
      }
      if (replacementDatabaseMeta==null) {
        throw new KettleException("Unable to find replacement database connection '"+replacementDatabaseName+"', can not be used to replace");
      }
      
      if (log.isDetailed()) {
        log.logDetailed("Replaced database connection '"+sourceDatabaseName+"' with connection '"+replacementDatabaseName+"'");
      }
      sourceDatabaseMeta.replaceMeta(replacementDatabaseMeta);
    }
    
    // Replace all steps with an Input Data Set marker with an Injector
    // Replace all steps with a Golden Data Set marker with a Dummy
    // Apply the tweaks to the steps:
    //   - Bypass : replace with Dummy
    //   - Remove : remove step and all connected hops.
    //
    // Loop over the original transformation to allow us to safely modify the copy
    //
    List<StepMeta> steps = transMeta.getSteps();
    for (StepMeta step : steps) {
      StepMeta stepMeta = copyTransMeta.findStep(step.getName());
      String inputSetName = stepMeta.getAttribute( DataSetConst.ATTR_GROUP_DATASET, DataSetConst.ATTR_STEP_DATASET_INPUT );
      String goldenSetName = stepMeta.getAttribute( DataSetConst.ATTR_GROUP_DATASET, DataSetConst.ATTR_STEP_DATASET_GOLDEN );
      String tweakName = stepMeta.getAttribute( DataSetConst.ATTR_GROUP_DATASET, DataSetConst.ATTR_STEP_TWEAK );
      
      // See if there's a unit test if the step isn't flagged...
      //
      if ( !Const.isEmpty( inputSetName ) ) {
        handleInputDataSet(log, inputSetName, unitTest, transMeta, stepMeta, factoriesHierarchy);        
      }
      
      // Capture golden data in a dummy step instead of the regular one?
      //
      if ( !Const.isEmpty( goldenSetName )) {
        handleGoldenDataSet(log, goldenSetName, stepMeta);
      }
      
      if ( !Const.isEmpty(tweakName)) {
        TransTweak tweak;
        try {
          tweak = TransTweak.valueOf(tweakName);
        } catch(Exception e) {
          throw new KettleException("Unrecognized tweak '"+tweakName+"-", e);
        }
        switch(tweak) {
        case NONE : break;
        case REMOVE_STEP: handleTweakRemoveStep(log, copyTransMeta, stepMeta); break;
        case BYPASS_STEP: handleTweakBypassStep(log, stepMeta); break;
        default: break;
        }
      }
      
    }
    
    
    // Now replace the metadata in the Trans object...
    //
    trans.setTransMeta( copyTransMeta );
    
    String testFilename = trans.environmentSubstitute(unitTest.getFilename());
    if (!Const.isEmpty(testFilename)) {
      try {
        OutputStream os = KettleVFS.getOutputStream(testFilename, false);
        os.write( XMLHandler.getXMLHeader().getBytes() );
        os.write( copyTransMeta.getXML().getBytes() );
        os.close();    
      } catch(Exception e) {
        throw new KettleException("Error writing test filename to '"+testFilename+"'", e);
      }
    }
  }

  private void handleTweakBypassStep(LogChannelInterface log, StepMeta stepMeta) {
    if (log.isDetailed()) {
      log.logDetailed("Replacing step '"+stepMeta.getName()+"' with an Dummy for Bypass step tweak");
    }
    
    replaceStepWithDummy(log, stepMeta);
  }

  private void handleTweakRemoveStep(LogChannelInterface log, TransMeta copyTransMeta, StepMeta stepMeta) {
    if (log.isDetailed()) {
      log.logDetailed("Removing step '"+stepMeta.getName()+"' for Remove step tweak");
    }
    
    // Remove all hops connecting to the step to be removed...
    //
    List<StepMeta> prevSteps = copyTransMeta.findPreviousSteps(stepMeta);
    for (StepMeta prevStep : prevSteps) {
      TransHopMeta hop = copyTransMeta.findTransHop(prevStep, stepMeta);
      if (hop!=null) {
        int hopIndex = copyTransMeta.indexOfTransHop(hop);
        copyTransMeta.removeTransHop(hopIndex);
      }
    }
    List<StepMeta> nextSteps = copyTransMeta.findNextSteps(stepMeta);
    for (StepMeta nextStep : nextSteps) {
      TransHopMeta hop = copyTransMeta.findTransHop(stepMeta, nextStep);
      if (hop!=null) {
        int hopIndex = copyTransMeta.indexOfTransHop(hop);
        copyTransMeta.removeTransHop(hopIndex);
      }
    }
    
    int idx = copyTransMeta.indexOfStep(stepMeta);
    if (idx>=0) {
      copyTransMeta.removeStep(idx);
    }
  }

  private void handleGoldenDataSet(LogChannelInterface log, String goldenSetName, StepMeta stepMeta) {
    
    if (log.isDetailed()) {
      log.logDetailed("Replacing step '"+stepMeta.getName()+"' with an Dummy for golden dataset '"+goldenSetName+"'");
    }
    
    replaceStepWithDummy(log, stepMeta);
  }

  private void replaceStepWithDummy(LogChannelInterface log, StepMeta stepMeta) {
    DummyTransMeta dummyTransMeta = new DummyTransMeta();
    stepMeta.setStepMetaInterface( dummyTransMeta );
    stepMeta.setStepID( PluginRegistry.getInstance().getPluginId( StepPluginType.class, dummyTransMeta) );
  }

  private void handleInputDataSet(LogChannelInterface log, String inputSetName, TransUnitTest unitTest, TransMeta transMeta, StepMeta stepMeta, FactoriesHierarchy factoriesHierarchy) throws KettleException {
    TransUnitTestSetLocation inputLocation = unitTest.findInputLocation( stepMeta.getName() );
    if (inputLocation!=null) {
      inputSetName = inputLocation.getDataSetName();
    }
  
    if (log.isDetailed()) {
      log.logDetailed("Replacing step '"+stepMeta.getName()+"' with an Injector for dataset '"+inputSetName+"'");
    }
    
    DataSet dataSet;
    try {
      dataSet = factoriesHierarchy.getSetFactory().loadElement(inputSetName);
    } catch (MetaStoreException e) {
      throw new KettleException("Unable to load data set '"+inputSetName+"'");
    }
    
    // OK, this step needs to be replaced by an Injector step...
    // Which fields do we need to use?
    //
    final RowMetaInterface stepFields = DataSetConst.getStepOutputFields(log, transMeta, stepMeta, dataSet, inputLocation);
    
    if (log.isDetailed()) {
      log.logDetailed("Input Data Set '"+inputSetName+"' Injector fields : '"+stepFields.toString());
    }
    
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
}

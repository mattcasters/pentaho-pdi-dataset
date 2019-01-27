package org.pentaho.di.dataset.spoon.xtpoint;

import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.dataset.DataSet;
import org.pentaho.di.dataset.TransUnitTest;
import org.pentaho.di.dataset.TransUnitTestDatabaseReplacement;
import org.pentaho.di.dataset.TransUnitTestSetLocation;
import org.pentaho.di.dataset.TransUnitTestTweak;
import org.pentaho.di.dataset.VariableValue;
import org.pentaho.di.dataset.util.DataSetConst;
import org.pentaho.di.dataset.util.FactoriesHierarchy;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.dummytrans.DummyTransMeta;
import org.pentaho.di.trans.steps.injector.InjectorMeta;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.List;

public class TransMetaModifier {

  private TransMeta transMeta;
  private TransUnitTest unitTest;

  public TransMetaModifier() {
  }

  public TransMetaModifier( TransMeta transMeta, TransUnitTest unitTest ) {
    this.transMeta = transMeta;
    this.unitTest = unitTest;
  }

  public TransMeta getTestTransformation( LogChannelInterface log, VariableSpace space, FactoriesHierarchy factoriesHierarchy ) throws KettleException {
    // OK, so now replace an input step with a data set attached with an Injector step...
    // However, we don't want to have the user see this so we need to copy trans.transMeta first...
    //
    // Clone seems to has problems so we'll take the long (XML) way around...
    InputStream stream;
    try {
      stream = new ByteArrayInputStream( transMeta.getXML().getBytes( Const.XML_ENCODING ) );
    } catch ( UnsupportedEncodingException e ) {
      throw new KettleException( "Encoding error", e );
    }
    TransMeta copyTransMeta = new TransMeta( stream, transMeta.getRepository(), true, transMeta, null );
    // Pass the metadata references...
    //
    copyTransMeta.setRepository( transMeta.getRepository() );
    copyTransMeta.setMetaStore( transMeta.getMetaStore() );
    copyTransMeta.setSharedObjects( transMeta.getSharedObjects() );

    // Replace certain connections with another
    //
    for ( TransUnitTestDatabaseReplacement dbReplacement : unitTest.getDatabaseReplacements() ) {
      String sourceDatabaseName = transMeta.environmentSubstitute( dbReplacement.getOriginalDatabaseName() );
      String replacementDatabaseName = transMeta.environmentSubstitute( dbReplacement.getReplacementDatabaseName() );

      DatabaseMeta sourceDatabaseMeta = copyTransMeta.findDatabase( sourceDatabaseName );
      DatabaseMeta replacementDatabaseMeta = copyTransMeta.findDatabase( replacementDatabaseName );
      if ( sourceDatabaseMeta == null ) {
        throw new KettleException( "Unable to find source database connection '" + sourceDatabaseName + "', can not be replaced" );
      }
      if ( replacementDatabaseMeta == null ) {
        throw new KettleException( "Unable to find replacement database connection '" + replacementDatabaseName + "', can not be used to replace" );
      }

      if ( log.isDetailed() ) {
        log.logDetailed( "Replaced database connection '" + sourceDatabaseName + "' with connection '" + replacementDatabaseName + "'" );
      }
      sourceDatabaseMeta.replaceMeta( replacementDatabaseMeta );
    }

    // Set parameters and variables...
    //
    String[] parameters = copyTransMeta.listParameters();
    List<VariableValue> variableValues = unitTest.getVariableValues();
    for ( VariableValue variableValue : variableValues ) {
      String key = space.environmentSubstitute( variableValue.getKey() );
      String value = space.environmentSubstitute( variableValue.getValue() );

      if ( StringUtils.isEmpty( key ) ) {
        continue;
      }
      if ( Const.indexOfString( key, parameters ) < 0 ) {
        // set the variable in the transformation metadata...
        //
        copyTransMeta.setVariable( key, value );
      } else {
        // Set the parameter value...
        //
        copyTransMeta.setParameterValue( key, value );
      }
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
    for ( StepMeta step : steps ) {
      StepMeta stepMeta = copyTransMeta.findStep( step.getName() );
      TransUnitTestSetLocation inputLocation = unitTest.findInputLocation( stepMeta.getName() );
      TransUnitTestSetLocation goldenLocation = unitTest.findGoldenLocation( stepMeta.getName() );
      TransUnitTestTweak stepTweak = unitTest.findTweak( stepMeta.getName() );

      // See if there's a unit test if the step isn't flagged...
      //
      if ( inputLocation != null ) {
        handleInputDataSet( log, inputLocation, unitTest, transMeta, stepMeta, factoriesHierarchy );
      }

      // Capture golden data in a dummy step instead of the regular one?
      //
      if ( goldenLocation != null ) {
        handleGoldenDataSet( log, goldenLocation, stepMeta );
      }

      if ( stepTweak != null && stepTweak.getTweak() != null ) {
        switch ( stepTweak.getTweak() ) {
          case NONE:
            break;
          case REMOVE_STEP:
            handleTweakRemoveStep( log, copyTransMeta, stepMeta );
            break;
          case BYPASS_STEP:
            handleTweakBypassStep( log, stepMeta );
            break;
          default:
            break;
        }
      }
    }

    return copyTransMeta;
  }

  private void handleInputDataSet( LogChannelInterface log, TransUnitTestSetLocation inputLocation, TransUnitTest unitTest, TransMeta transMeta, StepMeta stepMeta,
                                   FactoriesHierarchy factoriesHierarchy ) throws KettleException {

    String inputSetName = inputLocation.getDataSetName();

    if ( log.isDetailed() ) {
      log.logDetailed( "Replacing step '" + stepMeta.getName() + "' with an Injector for dataset '" + inputSetName + "'" );
    }

    DataSet dataSet;
    try {
      dataSet = factoriesHierarchy.getSetFactory().loadElement( inputSetName );
    } catch ( MetaStoreException e ) {
      throw new KettleException( "Unable to load data set '" + inputSetName + "'" );
    }

    // OK, this step needs to be replaced by an Injector step...
    // Which fields do we need to use?
    //
    final RowMetaInterface stepFields = DataSetConst.getStepOutputFields( log, dataSet, inputLocation );

    if ( log.isDetailed() ) {
      log.logDetailed( "Input Data Set '" + inputSetName + "' Injector fields : '" + stepFields.toString() );
    }

    InjectorMeta injectorMeta = new InjectorMeta();
    injectorMeta.allocate( stepFields.size() );
    for ( int x = 0; x < stepFields.size(); x++ ) {
      injectorMeta.getFieldname()[ x ] = stepFields.getValueMeta( x ).getName();
      injectorMeta.getType()[ x ] = stepFields.getValueMeta( x ).getType();
      injectorMeta.getLength()[ x ] = stepFields.getValueMeta( x ).getLength();
      injectorMeta.getPrecision()[ x ] = stepFields.getValueMeta( x ).getPrecision();

      // Only the step metadata, type...
      stepMeta.setStepMetaInterface( injectorMeta );
      stepMeta.setStepID( PluginRegistry.getInstance().getPluginId( StepPluginType.class, injectorMeta ) );
    }
  }

  private void handleGoldenDataSet( LogChannelInterface log, TransUnitTestSetLocation goldenSetName, StepMeta stepMeta ) {

    if ( log.isDetailed() ) {
      log.logDetailed( "Replacing step '" + stepMeta.getName() + "' with an Dummy for golden dataset '" + goldenSetName + "'" );
    }

    replaceStepWithDummy( log, stepMeta );
  }

  private void replaceStepWithDummy( LogChannelInterface log, StepMeta stepMeta ) {
    DummyTransMeta dummyTransMeta = new DummyTransMeta();
    stepMeta.setStepMetaInterface( dummyTransMeta );
    stepMeta.setStepID( PluginRegistry.getInstance().getPluginId( StepPluginType.class, dummyTransMeta ) );
  }

  private void handleTweakBypassStep( LogChannelInterface log, StepMeta stepMeta ) {
    if ( log.isDetailed() ) {
      log.logDetailed( "Replacing step '" + stepMeta.getName() + "' with an Dummy for Bypass step tweak" );
    }

    replaceStepWithDummy( log, stepMeta );
  }

  private void handleTweakRemoveStep( LogChannelInterface log, TransMeta copyTransMeta, StepMeta stepMeta ) {
    if ( log.isDetailed() ) {
      log.logDetailed( "Removing step '" + stepMeta.getName() + "' for Remove step tweak" );
    }

    // Remove all hops connecting to the step to be removed...
    //
    List<StepMeta> prevSteps = copyTransMeta.findPreviousSteps( stepMeta );
    for ( StepMeta prevStep : prevSteps ) {
      TransHopMeta hop = copyTransMeta.findTransHop( prevStep, stepMeta );
      if ( hop != null ) {
        int hopIndex = copyTransMeta.indexOfTransHop( hop );
        copyTransMeta.removeTransHop( hopIndex );
      }
    }
    List<StepMeta> nextSteps = copyTransMeta.findNextSteps( stepMeta );
    for ( StepMeta nextStep : nextSteps ) {
      TransHopMeta hop = copyTransMeta.findTransHop( stepMeta, nextStep );
      if ( hop != null ) {
        int hopIndex = copyTransMeta.indexOfTransHop( hop );
        copyTransMeta.removeTransHop( hopIndex );
      }
    }

    int idx = copyTransMeta.indexOfStep( stepMeta );
    if ( idx >= 0 ) {
      copyTransMeta.removeStep( idx );
    }
  }


  /**
   * Gets transMeta
   *
   * @return value of transMeta
   */
  public TransMeta getTransMeta() {
    return transMeta;
  }

  /**
   * @param transMeta The transMeta to set
   */
  public void setTransMeta( TransMeta transMeta ) {
    this.transMeta = transMeta;
  }

  /**
   * Gets unitTest
   *
   * @return value of unitTest
   */
  public TransUnitTest getUnitTest() {
    return unitTest;
  }

  /**
   * @param unitTest The unitTest to set
   */
  public void setUnitTest( TransUnitTest unitTest ) {
    this.unitTest = unitTest;
  }
}

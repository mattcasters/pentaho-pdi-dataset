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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.SourceToTargetMapping;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.dataset.DataSet;
import org.pentaho.di.dataset.DataSetGroup;
import org.pentaho.di.dataset.TransUnitTest;
import org.pentaho.di.dataset.TransUnitTestFieldMapping;
import org.pentaho.di.dataset.TransUnitTestSetLocation;
import org.pentaho.di.dataset.util.DataSetConst;
import org.pentaho.di.dataset.util.FactoriesHierarchy;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.RowProducer;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransAdapter;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.RowAdapter;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaDataCombi;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.persist.MetaStoreFactory;

/**
 * 
 * @author matt
 *
 */
@ExtensionPoint(
  extensionPointId = "TransformationStartThreads",
  id = "InjectDataSetIntoTransExtensionPoint",
  description = "Inject a bunch of rows into a step during preview" )
public class InjectDataSetIntoTransExtensionPoint implements ExtensionPointInterface {

  @Override
  public void callExtensionPoint( LogChannelInterface log, Object object ) throws KettleException {
    if ( !( object instanceof Trans ) ) {
      return;
    }

    final Trans trans = (Trans) object;
    final TransMeta transMeta = trans.getTransMeta();
    boolean dataSetEnabled = "Y".equalsIgnoreCase( transMeta.getVariable( DataSetConst.VAR_RUN_UNIT_TEST ) );
    log.logBasic("Data Set enabled? "+dataSetEnabled);
    if ( !dataSetEnabled ) {
      return;
    }

    String unitTestName = trans.getVariable( DataSetConst.VAR_UNIT_TEST_NAME );
    log.logBasic("Unit test name: "+unitTestName);
    
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
      if (StringUtil.isEmpty( unitTestName )) {
        return;
      }
      TransUnitTest unitTest = factoriesHierarchy.getTestFactory().loadElement( unitTestName );
      if (unitTest==null) {
        log.logBasic("Unit test '"+unitTestName+"' could not be found");
        return;
      }

      // Replace all steps with input data sets with Injector steps.
      // Replace all steps with a golden data set, attached to a unit test, with a Dummy
      // Apply tweaks
      //
      for ( final StepMeta stepMeta : trans.getTransMeta().getSteps() ) {
        String stepname = stepMeta.getName();
        TransUnitTestSetLocation inputLocation = unitTest.findInputLocation( stepname );
        if (inputLocation!=null && StringUtils.isNotEmpty(inputLocation.getDataSetName())) {
          String inputDataSetName = inputLocation.getDataSetName();
          log.logDetailed("Data Set location found for step '"+stepname+"' and data set  "+inputDataSetName);

          // We need to inject data from the data set with the specified name into the step
          //
          injectDataSetIntoStep( trans, transMeta, inputDataSetName, factoriesHierarchy.getSetFactory(), repository, metaStore, stepMeta, inputLocation );
        }

        // How about capturing rows for golden data review?
        //
        TransUnitTestSetLocation goldenLocation = unitTest.findGoldenLocation( stepname );
        if (goldenLocation!=null) {
          String goldenDataSetName = goldenLocation.getDataSetName();
          if (!StringUtil.isEmpty( goldenDataSetName )) {
            
            log.logBasic("Capturing rows for validation at transformation end, step='"+stepMeta.getName()+"', golden set '"+goldenDataSetName);
            
            final RowCollection rowCollection = new RowCollection();
            
            // Create a row collection map if it's missing...
            //
            @SuppressWarnings( "unchecked" )
            Map<String, RowCollection> collectionMap = (Map<String, RowCollection>) trans.getExtensionDataMap().get(DataSetConst.ROW_COLLECTION_MAP);
            if (collectionMap==null) {
              collectionMap = new HashMap<String, RowCollection>();
              trans.getExtensionDataMap().put(DataSetConst.ROW_COLLECTION_MAP, collectionMap);
            }
            
            // Keep the map for safe keeping...
            //
            collectionMap.put( stepMeta.getName(), rowCollection );
            
            // We'll capture the rows from this one and then evaluate them after execution...
            //
            StepInterface stepInterface = trans.findStepInterface( stepMeta.getName(), 0 );
            stepInterface.addRowListener( new RowAdapter() {
              @Override
              public void rowReadEvent( RowMetaInterface rowMeta, Object[] row ) throws KettleStepException {
                if (rowCollection.getRowMeta()==null) {
                  rowCollection.setRowMeta( rowMeta ); 
                }
                rowCollection.getRows().add( row );
              }
            });
          }
        }
      
      }
    } catch ( Throwable e ) {
      throw new KettleException( "Unable to inject data set rows", e );
    }

  }

  private void injectDataSetIntoStep( final Trans trans, final TransMeta transMeta,
    final String dataSetName, final MetaStoreFactory<DataSet> dataSetFactory,
    final Repository repository, final IMetaStore metaStore, final StepMeta stepMeta, 
    TransUnitTestSetLocation inputLocation ) throws MetaStoreException, KettleException {

    final DataSet dataSet = dataSetFactory.loadElement( dataSetName );
    final DataSetGroup group = dataSet.getGroup();
    final Database database = new Database( trans, group.getDatabaseMeta() );
    final LogChannelInterface log = trans.getLogChannel();
    
    final RowProducer rowProducer = trans.addRowProducer( stepMeta.getName(), 0 );

    // Look for the step into which we'll inject rows...
    //
    StepMetaDataCombi combi = null;
    for ( StepMetaDataCombi step : trans.getSteps() ) {
      if ( step.stepname.equals( stepMeta.getName() ) ) {
        combi = step;
        break;
      }
    }

    if ( combi != null ) {

      final List<Object[]> dataSetRows = dataSet.getAllRows(log, inputLocation);
      RowMetaInterface dataSetRowMeta = dataSet.getSetRowMeta( false );

      // The rows to inject are always driven by the dataset, NOT the step it replaces (!) for simplicity
      //
      RowMetaInterface injectRowMeta = new RowMeta();

      // Figure out which fields to pass
      // Only inject those mentioned in the field mappings...
      //
      int[] fieldIndexes = new int[inputLocation.getFieldMappings().size()];
      for (int i=0;i<inputLocation.getFieldMappings().size();i++) {
        TransUnitTestFieldMapping fieldMapping = inputLocation.getFieldMappings().get(i);
        fieldIndexes[i] = dataSetRowMeta.indexOfValue( fieldMapping.getDataSetFieldName() );
        if (fieldIndexes[i]<0) {
          throw new KettleException( "Unable to find mapped field '"+fieldMapping.getDataSetFieldName()+"' in data set '"+dataSet.getName()+"'" );
        }
        ValueMetaInterface injectValueMeta = dataSetRowMeta.getValueMeta( fieldIndexes[ i ] ).clone();
        // Rename to the step output names though...
        //
        injectValueMeta.setName( fieldMapping.getStepFieldName() );
        injectRowMeta.addValueMeta( injectValueMeta );
      }

      log.logBasic( "Injecting data set '" + dataSetName + "' into step '" + stepMeta.getName() + "', fields: "+ Arrays.toString(injectRowMeta.getFieldNames()) );

      // Pass rows
      try {
        Runnable runnable = new Runnable() {
          @Override
          public void run() {
            try {
              
              for( Object[] dataSetRow : dataSetRows ) {
                // pass the row with the external names, in the right order and with the selected columns from the data set
                //
                Object[] row = RowDataUtil.allocateRowData( injectRowMeta.size() );
                for (int i=0;i<fieldIndexes.length;i++) {
                  row[i] = dataSetRow[fieldIndexes[i]];
                }
                rowProducer.putRow( injectRowMeta, row );                
              }
              rowProducer.finished();
              
            } catch ( Exception e ) {
              throw new RuntimeException( "Problem injecting data set '" + dataSetName + "' row into step '" + stepMeta.getName() + "'", e );
            }
          }
        };
        Thread thread = new Thread( runnable );
        thread.start();

      } finally {
        database.disconnect();
      }
    }
  }
}

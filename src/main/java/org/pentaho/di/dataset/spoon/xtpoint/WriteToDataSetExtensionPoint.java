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
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.SourceToTargetMapping;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleValueException;
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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 
 * @author matt
 *
 */
@ExtensionPoint(
  extensionPointId = "TransformationStartThreads",
  id = "WriteToDataSetExtensionPoint",
  description = "Writes rows of data from a step into a data set" )
public class WriteToDataSetExtensionPoint implements ExtensionPointInterface {

  public static Map<String, StepMeta> stepsMap = new HashMap<String, StepMeta>();
  public static Map<String, List<SourceToTargetMapping>> mappingsMap = new HashMap<String, List<SourceToTargetMapping>>();
  public static Map<String, DataSet> setsMap = new HashMap<String, DataSet>();

  @Override
  public void callExtensionPoint( LogChannelInterface log, Object object ) throws KettleException {
    if ( !( object instanceof Trans ) ) {
      return;
    }

    final Trans trans = (Trans) object;
    final TransMeta transMeta = trans.getTransMeta();
    boolean writeToDataSet = "Y".equalsIgnoreCase( transMeta.getVariable( DataSetConst.VAR_WRITE_TO_DATASET ) );
    log.logBasic("Writing to dataSet? "+writeToDataSet);
    if ( !writeToDataSet ) {
      return;
    }

    trans.addTransListener( new TransAdapter() {
      @Override public void transFinished( Trans trans ) throws KettleException {
        // Remove the flag when done.
        // We don't want to write to the data set every time we run
        //
        transMeta.setVariable(DataSetConst.VAR_WRITE_TO_DATASET, null );

        // Prevent memory leaking as well
        //
        WriteToDataSetExtensionPoint.stepsMap.remove( transMeta.getName() );
        WriteToDataSetExtensionPoint.mappingsMap.remove( transMeta.getName() );
        WriteToDataSetExtensionPoint.setsMap.remove( transMeta.getName() );
      }
    } );
    
    try {
      IMetaStore metaStore = transMeta.getMetaStore();
      Repository repository = transMeta.getRepository();

      if ( metaStore == null ) {
        return; // Nothing to do here, we can't reference data sets.
      }

      List<DatabaseMeta> databases = DataSetConst.getAvailableDatabases( repository, transMeta.getSharedObjects() );
      FactoriesHierarchy factoriesHierarchy = new FactoriesHierarchy( metaStore, databases );

      // Replace all steps with input data sets with Injector steps.
      // Replace all steps with a golden data set, attached to a unit test, with a Dummy
      // Apply tweaks
      //
      for ( final StepMeta stepMeta : trans.getTransMeta().getSteps() ) {
        String stepname = stepMeta.getName();

        // We might want to pass the data from this step into a data set all by itself...
        // For this we want to attach a row listener which writes the data.
        //
        StepMeta injectMeta = stepsMap.get( transMeta.getName() );
        if ( injectMeta != null && injectMeta.equals( stepMeta ) ) {
          final List<SourceToTargetMapping> mappings = mappingsMap.get( transMeta.getName() );
          final DataSet dataSet = setsMap.get( transMeta.getName() );
          if ( mappings != null && dataSet != null ) {
            passStepRowsToDataSet( trans, transMeta, stepMeta, mappings, dataSet );
          }
        }
      }
    } catch ( Throwable e ) {
      throw new KettleException( "Unable to pass rows to data set", e );
    }

  }



  private void passStepRowsToDataSet( final Trans trans, final TransMeta transMeta, final StepMeta stepMeta, final List<SourceToTargetMapping> mappings, final DataSet dataSet ) throws KettleException {
    final DatabaseMeta databaseMeta = dataSet.getGroup().getDatabaseMeta();
    final Database database = new Database( trans, databaseMeta );

    database.connect();
    database.setCommit( Integer.MAX_VALUE );
    String schemaTable = databaseMeta.getQuotedSchemaTableCombination( dataSet.getGroup().getSchemaName(), dataSet.getTableName() );

    // Remove it all first. Don't truncate to keep the whole upload transactional
    //
    database.execStatement( "DELETE FROM " + schemaTable );
    final RowMetaInterface columnsRowMeta = dataSet.getSetRowMeta( true );
    database.prepareInsert( columnsRowMeta, dataSet.getGroup().getSchemaName(), dataSet.getTableName() );
    final RowMetaAndData rmad = new RowMetaAndData();
    rmad.setRowMeta( columnsRowMeta );

    // This is the step to inject into the specified data set
    //
    StepInterface stepInterface = trans.findStepInterface( stepMeta.getName(), 0 );
    stepInterface.addRowListener( new RowAdapter() {
      public void rowWrittenEvent( RowMetaInterface rowMeta, Object[] row ) throws KettleStepException {
        try {
          Object[] dbRow = RowDataUtil.allocateRowData( columnsRowMeta.size() );
          for ( SourceToTargetMapping mapping : mappings ) {
            dbRow[mapping.getTargetPosition()] = row[mapping.getSourcePosition()];
          }
          rmad.setData( dbRow );
          database.setValuesInsert( rmad );
          database.insertRow( false );
        } catch ( Exception e ) {
          try {
            trans.getLogChannel().logError("Error writing row to data set '"+dataSet.getName()+"' : "+rowMeta.getString( row ));
          } catch ( KettleValueException e1 ) {
            trans.getLogChannel().logError("Data conversion error on row metadata : "+rowMeta.toString()+" data : "+Arrays.toString(row));
          }

          throw new KettleStepException( e );
        }
      }
    } );

    // we also need to clean up shop at the end of the transformation...
    //
    trans.addTransListener( new TransAdapter() {
      @Override
      public void transFinished( Trans trans ) throws KettleException {
        try {
          if ( trans.isStopped() || trans.getErrors() > 0 ) {
            database.rollback();
          } else {
            database.commit();
          }
          database.disconnect();

          // Also clear out the mappings...
          setsMap.remove( transMeta.getName() );
          mappingsMap.remove( transMeta.getName() );
          setsMap.remove( transMeta.getName() );
        } catch ( Exception e ) {
          throw new KettleException( "Unable to close write operation to dataset", e );
        }
      }
    } );

  }
}

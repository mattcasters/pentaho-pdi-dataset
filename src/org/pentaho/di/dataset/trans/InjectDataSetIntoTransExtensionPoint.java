package org.pentaho.di.dataset.trans;

import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.dataset.DataSet;
import org.pentaho.di.dataset.DataSetGroup;
import org.pentaho.di.dataset.TransUnitTest;
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
    boolean dataSetEnabled = "Y".equalsIgnoreCase( transMeta.getVariable( DataSetConst.VAR_RUN_UNIT_TEST ) );

    if ( !dataSetEnabled ) {
      return;
    }

    String unitTestName = transMeta.getAttribute( DataSetConst.ATTR_GROUP_DATASET, DataSetConst.ATTR_TRANS_SELECTED_UNIT_TEST_NAME );
    
    try {
      IMetaStore metaStore = transMeta.getMetaStore();
      Repository repository = transMeta.getRepository();

      if ( metaStore == null ) {
        return; // Nothing to do here, we can't reference data sets.
      }

      // TODO: The next 2 lines are very expensive: see how we can cache this or move it upstairs somewhere.
      //
      List<DatabaseMeta> databases = DataSetConst.getAvailableDatabases( repository, transMeta.getSharedObjects() );
      FactoriesHierarchy factoriesHierarchy = new FactoriesHierarchy( metaStore, databases );

      // If the transformation has a variable set with the unit test in it, we're dealing with a unit test situation.
      //
      TransUnitTest unitTest = null;
      if (!Const.isEmpty( unitTestName )) {
        unitTest = factoriesHierarchy.getTestFactory().loadElement( unitTestName );
      }

      // Replace all steps with input data sets with Injector steps.
      // Replace all steps with a golden data set, attached to a unit test, with a Dummy
      //
      for ( final StepMeta stepMeta : trans.getTransMeta().getSteps() ) {
        String dataSetName = stepMeta.getAttribute( DataSetConst.ATTR_GROUP_DATASET, DataSetConst.ATTR_STEP_DATASET_INPUT );
        
        // See if there's a unit test if the step isn't flagged...
        //
        if ( unitTest!=null && Const.isEmpty( dataSetName ) ) {
          TransUnitTestSetLocation inputLocation = unitTest.findInputLocation( stepMeta.getName() );
          if (inputLocation!=null) {
            dataSetName = inputLocation.getDataSetName();
          }
        }
        
        if ( !Const.isEmpty( dataSetName ) ) {

          // We need to inject data from the data set with the specified name into the step
          //
          injectDataSetIntoStep( trans, transMeta, dataSetName, factoriesHierarchy.getSetFactory(), repository, metaStore, stepMeta );
        }

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

        // How about capturing rows for golden data review?
        //
        if ( unitTest!=null) {
          TransUnitTestSetLocation goldenLocation = unitTest.findGoldenLocation( stepMeta.getName() );
          if (goldenLocation!=null) {
            String goldenDataSetName = goldenLocation.getDataSetName();
            if (!Const.isEmpty( goldenDataSetName )) {
              
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
      }
    } catch ( Throwable e ) {
      throw new KettleException( "Unable to inject data set rows", e );
    }

  }



  private void passStepRowsToDataSet( final Trans trans, final TransMeta transMeta, final StepMeta stepMeta, final List<SourceToTargetMapping> mappings, final DataSet dataSet ) throws KettleException {
    final DatabaseMeta databaseMeta = dataSet.getGroup().getDatabaseMeta();
    final Database database = new Database( trans, databaseMeta );

    database.connect();
    database.setCommit( Integer.MAX_VALUE );
    String schemaTable = databaseMeta.getQuotedSchemaTableCombination( dataSet.getGroup().getSchemaName(), dataSet.getTableName() );
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

  private void injectDataSetIntoStep( final Trans trans, final TransMeta transMeta,
    final String dataSetName, final MetaStoreFactory<DataSet> dataSetFactory,
    final Repository repository, final IMetaStore metaStore, final StepMeta stepMeta ) throws MetaStoreException, KettleException {

    final DataSet dataSet = dataSetFactory.loadElement( dataSetName );
    final DataSetGroup group = dataSet.getGroup();
    final Database database = new Database( trans, group.getDatabaseMeta() );
    final String schemaTable = group.getDatabaseMeta().getQuotedSchemaTableCombination( group.getSchemaName(), dataSet.getTableName() );
    final RowMetaInterface columnRowMeta = dataSet.getSetRowMeta( true );
    final RowMetaInterface fieldRowMeta = dataSet.getSetRowMeta( false );

    final RowProducer rowProducer = trans.addRowProducer( stepMeta.getName(), 0 );

    // This step needs to be replaced...
    // This is somewhat hack-ish, exceptional work...
    //
    StepMetaDataCombi combi = null;
    for ( StepMetaDataCombi step : trans.getSteps() ) {
      if ( step.stepname.equals( stepMeta.getName() ) ) {
        combi = step;
        break;
      }
    }

    if ( combi != null ) {

      trans.getLogChannel().logBasic( "Injecting data set '" + dataSetName + "' into step '" + stepMeta.getName() + "'" );
      
      String query = "SELECT ";
      for ( int i = 0; i < columnRowMeta.size(); i++ ) {
        ValueMetaInterface colValueMeta = columnRowMeta.getValueMeta( i );
        if ( i > 0 ) {
          query += ", ";
        }
        query += group.getDatabaseMeta().quoteField( colValueMeta.getName() );
      }
      query += " FROM " + schemaTable;

      // Pass rows
      try {
        database.connect();
        final ResultSet resultSet = database.openQuery( query );

        Runnable runnable = new Runnable() {
          @Override
          public void run() {
            try {
              Object[] row = database.getRow( resultSet );
              while ( row != null ) {
                // pass the row with the external names
                //
                rowProducer.putRow( fieldRowMeta, row );

                row = database.getRow( resultSet );
              }
              rowProducer.finished();
              
              resultSet.close();
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

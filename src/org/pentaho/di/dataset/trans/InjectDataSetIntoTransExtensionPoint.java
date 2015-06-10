package org.pentaho.di.dataset.trans;

import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.RowSet;
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
import org.pentaho.di.dataset.TransUnitTestFieldMapping;
import org.pentaho.di.dataset.util.DataSetConst;
import org.pentaho.di.dataset.util.FactoriesHierarchy;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransAdapter;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.RowAdapter;
import org.pentaho.di.trans.step.RowListener;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaDataCombi;
import org.pentaho.di.trans.steps.injector.Injector;
import org.pentaho.di.trans.steps.injector.InjectorMeta;
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
    String strEnabled = transMeta.getVariable( DataSetConst.VAR_STEP_DATASET_ENABLED );
    boolean dataSetEnabled = "Y".equalsIgnoreCase( strEnabled );

    if ( !trans.isPreview() && !dataSetEnabled ) {
      return;
    }

    try {
      IMetaStore metaStore = transMeta.getMetaStore();
      Repository repository = transMeta.getRepository();

      if ( metaStore == null ) {
        return; // Nothing to do here, we can't reference data sets.
      }

      FactoriesHierarchy factoriesHierarchy = null;

      for ( final StepMeta stepMeta : trans.getTransMeta().getSteps() ) {
        final String dataSetName = stepMeta.getAttribute( DataSetConst.ATTR_GROUP_DATASET, DataSetConst.ATTR_STEP_DATASET_INPUT );
        if ( !Const.isEmpty( dataSetName ) ) {

          // We need to inject data from the data set with the specified name into the step
          //
          if ( factoriesHierarchy == null ) {
            // Lazy loading...
            //
            factoriesHierarchy = new FactoriesHierarchy( metaStore, DataSetConst.getAvailableDatabases( repository ) );
          }

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

        // We might want to verify if a unit test definition is passing
        //
        final String testName = stepMeta.getAttribute( DataSetConst.ATTR_GROUP_DATASET, DataSetConst.ATTR_STEP_UNIT_TEST );
        if ( !Const.isEmpty( testName ) ) {
          if ( factoriesHierarchy == null ) {
            // Lazy loading...
            //
            factoriesHierarchy = new FactoriesHierarchy( metaStore, DataSetConst.getAvailableDatabases( repository ) );
          }
          validateStepRowsAgainstGoldenDataSet( trans, transMeta, testName, factoriesHierarchy.getTestFactory(), repository, metaStore, stepMeta );
        }

      }
    } catch ( Throwable e ) {
      throw new KettleException( "Unable to inject data set rows", e );
    }

  }

  /**
   * Validate the rows from the step in the order specified in the unit test.
   * 
   * @param trans
   * @param transMeta
   * @param testName
   * @param testFactory
   * @param repository
   * @param metaStore
   * @param stepMeta
   * @throws MetaStoreException 
   * @throws KettleException 
   */
  private void validateStepRowsAgainstGoldenDataSet( Trans trans, TransMeta transMeta, final String testName, MetaStoreFactory<TransUnitTest> testFactory, Repository repository, IMetaStore metaStore, final StepMeta stepMeta ) throws MetaStoreException, KettleException {
    final TransUnitTest unitTest = testFactory.loadElement( testName );
    final DataSet goldenDataSet = unitTest.getGoldenDataSet();
    if ( goldenDataSet == null ) {
      throw new KettleException( "No golden data specified for test " + testName );
    }
    final RowMetaInterface goldenRowMeta = goldenDataSet.getSetRowMeta( false );
    final List<Object[]> goldenRows = unitTest.getGoldenRows();

    final RowMetaInterface stepRowMeta = transMeta.getStepFields( stepMeta );

    final int[] stepFieldIndices = new int[unitTest.getFieldMappings().size()];
    final int[] goldenIndices = new int[unitTest.getFieldMappings().size()];
    for ( int i = 0; i < unitTest.getFieldMappings().size(); i++ ) {
      TransUnitTestFieldMapping fieldMapping = unitTest.getFieldMappings().get( i );

      stepFieldIndices[i] = stepRowMeta.indexOfValue( fieldMapping.getStepFieldName() );
      goldenIndices[i] = goldenRowMeta.indexOfValue( fieldMapping.getDataSetFieldName() );
    }

    // This is the step to inject into the specified data set
    //
    StepInterface stepInterface = trans.findStepInterface( stepMeta.getName(), 0 );
    final ValidatingRowListener rowListener = new ValidatingRowListener( goldenDataSet, unitTest, goldenRows, goldenRowMeta, stepRowMeta, stepFieldIndices, goldenIndices );
    stepInterface.addRowListener( rowListener );

    trans.addTransListener( new TransAdapter() {
      @Override
      public void transFinished( Trans trans ) throws KettleException {
        // Just verify that we received enough rows
        //
        if ( rowListener.getRowNumber() < goldenRows.size() ) {
          throw new KettleException( "Transformation unit test '" + unitTest.getName()
            + "' : we expected " + goldenRows.size() + " rows from step " + stepMeta.getName()
            + " but only reveived " + rowListener.getRowNumber() );
        } else {
          trans.getLogChannel().logBasic( "Transformation unit test '" + unitTest.getName()
            + "' : Verified " + rowListener.getRowNumber() + " rows against golden data set '" + goldenDataSet.getName() + "'" );
        }
      }
    } );

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

    final StepInterface stepInterface = trans.findStepInterface( stepMeta.getName(), 0 );

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

      // Keep the input and output row sets...
      //
      List<RowSet> outputRowSets = combi.step.getOutputRowSets();
      List<RowSet> inputRowSets = combi.step.getInputRowSets();
      List<RowListener> rowListeners = ( (BaseStep) combi.step ).getRowListeners();

      // Replace the old step with a new Injector step with the same name...
      //
      combi.meta = getInjectorMeta( fieldRowMeta );
      StepMeta originalStepMeta = combi.stepMeta;
      combi.stepMeta = new StepMeta( stepMeta.getName(), combi.meta );
      combi.stepMeta.setLocation( stepMeta.getLocation() );
      combi.stepMeta.setDraw( true );
      combi.stepMeta.setTargetStepPartitioningMeta( originalStepMeta.getTargetStepPartitioningMeta() );
      combi.data = combi.meta.getStepData();
      // StepInterface originalStep = combi.step;
      combi.step = new Injector( combi.stepMeta, combi.data, combi.copy, transMeta, trans );
      combi.step.init( combi.meta, combi.data );
      combi.step.getOutputRowSets().clear();
      combi.step.getOutputRowSets().addAll( outputRowSets );
      combi.step.getInputRowSets().clear();
      combi.step.getInputRowSets().addAll( inputRowSets );
      combi.stepname = stepMeta.getName();
      ( (BaseStep) combi.step ).getRowListeners().clear();
      ( (BaseStep) combi.step ).getRowListeners().addAll( rowListeners );

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
                stepInterface.putRow( fieldRowMeta, row );

                row = database.getRow( resultSet );
              }
              stepInterface.setOutputDone();
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

  private InjectorMeta getInjectorMeta( RowMetaInterface rowMeta ) {
    InjectorMeta meta = new InjectorMeta();
    meta.allocate( rowMeta.size() );
    for ( int i = 0; i < rowMeta.size(); i++ ) {
      ValueMetaInterface valueMeta = rowMeta.getValueMeta( i );
      meta.getFieldname()[i] = valueMeta.getName();
      meta.getType()[i] = valueMeta.getType();
      meta.getLength()[i] = valueMeta.getLength();
      meta.getPrecision()[i] = valueMeta.getPrecision();
    }
    return meta;
  }
}

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

package org.pentaho.di.dataset.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LoggingObjectType;
import org.pentaho.di.core.logging.SimpleLoggingObject;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.dataset.DataSet;
import org.pentaho.di.dataset.DataSetField;
import org.pentaho.di.dataset.DataSetGroup;
import org.pentaho.di.dataset.TestType;
import org.pentaho.di.dataset.TransTweak;
import org.pentaho.di.dataset.TransUnitTest;
import org.pentaho.di.dataset.TransUnitTestFieldMapping;
import org.pentaho.di.dataset.TransUnitTestSetLocation;
import org.pentaho.di.dataset.TransUnitTestTweak;
import org.pentaho.di.dataset.UnitTestResult;
import org.pentaho.di.dataset.spoon.xtpoint.RowCollection;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.shared.SharedObjectInterface;
import org.pentaho.di.shared.SharedObjects;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;

public class DataSetConst {
  private static Class<?> PKG = DataSetConst.class; // for i18n purposes, needed by Translator2!!

  public static String DATA_SET_GROUP_TYPE_NAME = "Data Set Group";
  public static String DATA_SET_GROUP_TYPE_DESCRIPTION = "A collection of data sets and unit tests";

  public static String DATA_SET_GROUP_DESCRIPTION = "description";
  public static String DATA_SET_GROUP_DATABASE_NAME = "connection";

  public static String DATA_SET_TYPE_NAME = "Data Set";
  public static String DATA_SET_TYPE_DESCRIPTION = "A data set";

  public static String DATA_SET_DESCRIPTION = "description";
  public static String DATA_SET_SCHEMA_NAME = "schema";
  public static String DATA_SET_TABLE_NAME = "table";
  public static String DATA_SET_ROWMETA_XML = "rowmeta-xml";
  public static String DATA_SET_GROUP_NAME = "group";

  public static final String DATABASE_LIST_KEY = "Databases";
  public static final String GROUP_LIST_KEY = "DataSetGroups";
  public static final String SET_LIST_KEY = "DataSets";

  public static final String ATTR_GROUP_DATASET = "DataSet";
  public static final String ATTR_STEP_DATASET_INPUT = "InputDataSet";
  public static final String ATTR_STEP_DATASET_GOLDEN = "GoldenDataSet";
  public static final String ATTR_STEP_TWEAK= "UnitTestStepTweak";
  
  public static final String VAR_RUN_UNIT_TEST = "__UnitTest__";
  public static final String VAR_DO_NOT_SHOW_UNIT_TEST_ERRORS = "__DontShowUnitTestErrors__";
  public static final String ATTR_TRANS_SELECTED_UNIT_TEST_NAME = "SelectedUnitTest";
  
  public static final String AREA_DRAWN_UNIT_ICON = "DrawnUnitTestIcon";
  public static final String ROW_COLLECTION_MAP = "RowCollectionMap";
  public static final String UNIT_TEST_RESULTS = "UnitTestResults";

  private static final String[] tweakDesc = new String[] {
      BaseMessages.getString(PKG, "DataSetConst.Tweak.NONE.Desc"),
      BaseMessages.getString(PKG, "DataSetConst.Tweak.BYPASS_STEP.Desc"),
      BaseMessages.getString(PKG, "DataSetConst.Tweak.REMOVE_STEP.Desc"),      
      };

  private static final String[] testTypeDesc = new String[] {
      BaseMessages.getString(PKG, "DataSetConst.TestType.NONE.Desc"),
      BaseMessages.getString(PKG, "DataSetConst.TestType.CONCEPTUAL.Desc"),
      BaseMessages.getString(PKG, "DataSetConst.TestType.DEVELOPMENT.Desc"),
      BaseMessages.getString(PKG, "DataSetConst.TestType.UNIT_TEST.Desc"),
      };
  
  public static final DataSet findDataSet( List<DataSet> list, String dataSetName ) {
    if ( Const.isEmpty( dataSetName ) ) {
      return null;
    }
    for ( DataSet dataSet : list ) {
      if ( dataSetName.equals( dataSet.getName() ) ) {
        return dataSet;
      }
    }
    return null;
  }

  public static final DataSetGroup findDataSetGroup( List<DataSetGroup> list, String dataSetGroupName ) {
    if ( Const.isEmpty( dataSetGroupName ) ) {
      return null;
    }
    for ( DataSetGroup dataSetGroup : list ) {
      if ( dataSetGroupName.equals( dataSetGroup.getName() ) ) {
        return dataSetGroup;
      }
    }
    return null;
  }

  public static List<DatabaseMeta> getAvailableDatabases( Repository repository, SharedObjects sharedObjects ) throws KettleException {
    List<DatabaseMeta> list = new ArrayList<DatabaseMeta>();

    // Load database connections from the central repository if we're connected to one
    //
    if ( repository != null ) {
      ObjectId[] databaseIDs = repository.getDatabaseIDs( false );
      for ( ObjectId databaseId : databaseIDs ) {
        list.add( repository.loadDatabaseMeta( databaseId, null ) );
      }
    }

    // Also load from the shared objects file of the transformation
    //
    Collection<SharedObjectInterface> localSharedObjects = sharedObjects.getObjectsMap().values();

    for ( SharedObjectInterface localSharedObject : localSharedObjects ) {
      if ( localSharedObject instanceof DatabaseMeta ) {
        DatabaseMeta databaseMeta = (DatabaseMeta) localSharedObject;
        // Only add a local database if it doesn't exist in the central repository
        //
        if ( !list.contains( databaseMeta ) ) {
          list.add( databaseMeta );
        }
      }
    }

    return list;
  }
  
  public static final DataSet writeDataSet(String name, String description, DataSetGroup dataSetGroup, String tableName, List<DataSetField> fields, List<Object[]> dataRows) throws KettleException {
    DataSet dataSet = new DataSet( name, description, dataSetGroup, tableName, fields );
    RowMetaInterface rowMeta = dataSet.getSetRowMeta( true );
    List<RowMetaAndData> rows = new ArrayList<RowMetaAndData>();
    for (Object[] dataRow : dataRows) {
      RowMetaAndData row = new RowMetaAndData();
      row.setRowMeta( rowMeta );
      row.setData( dataRow );
      rows.add( row );
    }
    
    Database database = new Database( new SimpleLoggingObject( "Writing Data Set", LoggingObjectType.TRANS, null ), dataSetGroup.getDatabaseMeta());
    try {
      database.connect();
      String sql;
      if ( database.checkTableExists( tableName) ) {
        sql = database.getAlterTableStatement( tableName, rowMeta, null, false, null, true );
      } else {
        sql = database.getCreateTableStatement( tableName, rowMeta, null, false, null, true );
      }
      if ( !Const.isEmpty( sql ) ) {
        database.execStatements( sql );
      }
      database.prepareInsert( rowMeta, tableName );
      for ( RowMetaAndData row : rows ) {
        database.setValuesInsert( row );
        database.insertRow();
      }
      database.commit();
    } finally {
      database.disconnect();
    }

    return dataSet;
  }
  
  public static final void clearStepDataSetIndicators(TransMeta transMeta) {
    for (StepMeta stepMeta : transMeta.getSteps()) {
      Map<String, String> attributes = stepMeta.getAttributes( DataSetConst.ATTR_GROUP_DATASET );
      if (attributes!=null) {
        attributes.remove( DataSetConst.ATTR_STEP_DATASET_INPUT );
        attributes.remove( DataSetConst.ATTR_STEP_DATASET_GOLDEN);
        attributes.remove( DataSetConst.ATTR_STEP_TWEAK);
      }
    }
  }  

  
  public static final void loadStepDataSetIndicators(TransMeta transMeta, TransUnitTest test) {
    
    // First clear 'm all
    //
    clearStepDataSetIndicators( transMeta );
    
    for (TransUnitTestSetLocation location : test.getInputDataSets()) {
      StepMeta stepMeta = transMeta.findStep( location.getStepname() );
      if (stepMeta!=null) {
        stepMeta.setAttribute( DataSetConst.ATTR_GROUP_DATASET, DataSetConst.ATTR_STEP_DATASET_INPUT, location.getDataSetName());
      }
    }
    
    for (TransUnitTestSetLocation location : test.getGoldenDataSets()) {
      StepMeta stepMeta = transMeta.findStep( location.getStepname() );
      if (stepMeta!=null) {
        stepMeta.setAttribute( DataSetConst.ATTR_GROUP_DATASET, DataSetConst.ATTR_STEP_DATASET_GOLDEN, location.getDataSetName());
      }
    }
    
    // Load the tweak indicators?
    //
    List<TransUnitTestTweak> tweaks = test.getTweaks();
    for (TransUnitTestTweak tweak : tweaks) {
      StepMeta stepMeta = transMeta.findStep(tweak.getStepName());
      if (stepMeta!=null && tweak.getTweak()!=null) {
        stepMeta.setAttribute(DataSetConst.ATTR_GROUP_DATASET, DataSetConst.ATTR_STEP_TWEAK, tweak.getTweak().name());
      } 
    }
  }
  
  /**
   * Validate the execution results of a transformation against the golden data sets of a unit test.
   * @param trans The transformation after execution
   * @param unitTest The unit test
   * @param hierarchy The factories to load unit test and data set information
   * @param results The results list to add comments to
   * @return The nr of errors, 0 if no errors found
   * @throws KettleException In case there was an error loading data or metadata.
   */
  public static final int validateTransResultAgainstUnitTest(Trans trans, TransUnitTest unitTest, FactoriesHierarchy hierarchy, List<UnitTestResult> results) throws KettleException {
    int nrErrors = 0;
    
    LogChannelInterface log = trans.getLogChannel();
    
    @SuppressWarnings( "unchecked" )
    Map<String, RowCollection> collectionMap = (Map<String, RowCollection>) trans.getExtensionDataMap().get( DataSetConst.ROW_COLLECTION_MAP );
    if (collectionMap==null) {
      
      String comment = "No step output result data found to validate against";
      results.add(new UnitTestResult( trans.getName(), unitTest.getName(), null, null, false, comment));
      return nrErrors;
    }
    
    for (TransUnitTestSetLocation location : unitTest.getGoldenDataSets()) {
      int nrLocationErrors = 0;
      RowCollection resultCollection = collectionMap.get( location.getStepname() );
      if (resultCollection==null || resultCollection.getRows()==null || resultCollection.getRowMeta()==null) {
        // error occurred somewhere, we don't have results, provide dummy values to avoid exceptions, flag error
        //
        resultCollection = new RowCollection();
        resultCollection.setRowMeta(new RowMeta());
        resultCollection.setRows(new ArrayList<Object[]>());    
        
        String comment = "WARNING: no test results found for step '" + location.getStepname() + "' : check disabled hops, input and so on.";
        results.add(new UnitTestResult(
            trans.getName(), unitTest.getName(), location.getDataSetName(), location.getStepname(),
            false, comment));
      }
      RowMetaInterface resultRowMeta = resultCollection.getRowMeta();

      // Only retain the mapped fields for the unit test...
      //
      RowMetaInterface goldenRowMeta = new RowMeta();
      for (TransUnitTestFieldMapping mapping : location.getFieldMappings()) {
         ValueMetaInterface resultValueMeta = resultRowMeta.searchValueMeta(mapping.getStepFieldName());
         ValueMetaInterface goldenValueMeta;
         if (resultValueMeta!=null) {
           goldenValueMeta = resultValueMeta.clone();
         } else {
           // case where we have no results.
           //
           goldenValueMeta = new ValueMetaString(mapping.getStepFieldName());
         }
         goldenValueMeta.setName(mapping.getDataSetFieldName());
         goldenRowMeta.addValueMeta(goldenValueMeta);
      }
      
      log.logBasic("Found "+resultCollection.getRows().size()+" results for comparrison in step '"+location.getStepname()+"', fields: "+resultRowMeta.toString());
      
      RowCollection goldenCollection = unitTest.getGoldenRows( log, hierarchy, location, goldenRowMeta );
      
      log.logBasic("Found "+goldenCollection.getRows().size()+" golden rows '"+location.getStepname()+"', fields: "+goldenRowMeta.toString());
      
      List<Object[]> resultRows = resultCollection.getRows();
      List<Object[]> goldenRows = goldenCollection.getRows();

      if ( resultRows.size() != goldenRows.size() ) {
        String comment = "Incorrect number of rows received from step, golden data set '" + location.getDataSetName() + "' has " + goldenRows.size() + " rows in it and we received "+resultRows.size();
        results.add(new UnitTestResult(
            trans.getName(), unitTest.getName(), location.getDataSetName(), location.getStepname(),
            true, comment));
        nrLocationErrors++;
      }
      
      if (nrLocationErrors==0) {
        final int[] stepFieldIndices = new int[location.getFieldMappings().size()];
        final int[] goldenIndices = new int[location.getFieldMappings().size()];
        for ( int i = 0; i < location.getFieldMappings().size(); i++ ) {
          TransUnitTestFieldMapping fieldMapping = location.getFieldMappings().get( i );
  
          stepFieldIndices[i] = resultCollection.getRowMeta().indexOfValue( fieldMapping.getStepFieldName() );
          goldenIndices[i] = goldenCollection.getRowMeta().indexOfValue( fieldMapping.getDataSetFieldName() );
        }
        
        for (int rowNumber=0 ; rowNumber<resultRows.size() ; rowNumber++) {
          Object[] resultRow = resultRows.get( rowNumber );
          Object[] goldenRow = goldenRows.get( rowNumber );
        
          // Now compare the input to the golden row
          //
          for ( int i = 0; i < location.getFieldMappings().size(); i++ ) {
            ValueMetaInterface stepValueMeta = resultCollection.getRowMeta().getValueMeta( stepFieldIndices[i] );
            Object stepValue = resultRow[stepFieldIndices[i]];
    
            ValueMetaInterface goldenValueMeta = goldenCollection.getRowMeta().getValueMeta( goldenIndices[i] );
            Object goldenValue = goldenRow[goldenIndices[i]];
            
            if (log.isDebug()) {
              log.logDebug("Comparing Meta '"+stepValueMeta.toString()+"' with '"+goldenValueMeta.toString()+"'");
              log.logDebug("Comparing Value '"+stepValue+"' with '"+goldenValue+"'");
            }
            
            Object goldenValueConverted;
            
            // sometimes there are data conversion issues because of the the database...
            //
            if (goldenValueMeta.getType()==stepValueMeta.getType()) {
              goldenValueConverted = goldenValue;
            } else {
              goldenValueConverted = stepValueMeta.convertData( goldenValueMeta, goldenValue );
            }
            
            try {
              int cmp = stepValueMeta.compare( stepValue, stepValueMeta, goldenValueConverted );
              if ( cmp != 0 ) {
                if (log.isDebug()) {
                  log.logDebug("Unit test failure: '"+stepValue+"' <> '"+goldenValue+"'");
                }
                String comment = "Validation againt golden data failed for row number " + (rowNumber+1)
                  + ": step value [" + stepValueMeta.getString( stepValue )
                  + "] does not correspond to data set value [" + goldenValueMeta.getString( goldenValue ) + "]";
                results.add(new UnitTestResult(
                    trans.getName(), unitTest.getName(), location.getDataSetName(), location.getStepname(),
                    true, comment));
                nrLocationErrors++;
              }
            } catch ( KettleValueException e ) {
              throw new KettleException( "Unable to compare step data against golden data set '" + location.getDataSetName() + "'", e );
            }
          }
        }
      }

      if (nrLocationErrors==0) {
        String comment = "Test passed succesfully against golden data set";
        results.add(new UnitTestResult(
            trans.getName(), unitTest.getName(), location.getDataSetName(), location.getStepname(),
            false, comment));
      } else {
        nrErrors+=nrLocationErrors;
      }
    }
    
    if (nrErrors==0) {
      String comment = "Test passed succesfully against unit test";
      results.add(new UnitTestResult(
          trans.getName(), unitTest.getName(), null, null,
          false, comment));
    
    }
    return nrErrors;
  }
  
  public static final RowMetaInterface getStepOutputFields(LogChannelInterface log, TransMeta transMeta, StepMeta stepMeta, DataSet dataSet, TransUnitTestSetLocation inputLocation ) throws KettleException {
    final RowMetaInterface outputRowMeta = new RowMeta();
    RowMetaInterface addFields;
    try {
      addFields = transMeta.getStepFields(stepMeta);
      log.logBasic("The natural output fields of step '"+stepMeta.getName()+"' are : " +addFields.toString() );
    } catch(Exception e) {
      addFields = new RowMeta();
      log.logError("Error getting step output fields: "+e.getMessage());
    }
    if (addFields.isEmpty()) {
      addFields= dataSet.getSetRowMeta(false);
      log.logBasic("No output fields found for step '"+stepMeta.getName()+"', taking mapped fields from input data set." );
    }
    if (inputLocation.getFieldMappings().isEmpty()) {
      // If we have no field mappings, just add all fields that are found...
      //
      log.logBasic("No mappings found, adding all fields : " );
      outputRowMeta.addRowMeta(addFields);
    } else {
      // Limit the fields to those which are mapped
      for (TransUnitTestFieldMapping mapping : inputLocation.getFieldMappings()) {
        ValueMetaInterface setValueMeta = addFields.searchValueMeta(mapping.getDataSetFieldName());
        if (setValueMeta!=null) {
          outputRowMeta.addValueMeta(setValueMeta);
        }
      }
    }

    return outputRowMeta;
  }
  
  public static final String getDirectoryFromPath(String path) {
    int lastSlashIndex = path.lastIndexOf('/');
    if (lastSlashIndex>=0) {
      return path.substring(0, lastSlashIndex);
    } else {
      return "/";
    }
  }
  
  public static final String getNameFromPath(String path) {
    int lastSlashIndex = path.lastIndexOf('/');
    if (lastSlashIndex>=0) {
      return path.substring(lastSlashIndex+1);
    } else {
      return path;
    }
  }
  
  /**
   * Get the TransTweak for a tweak description (from the dialog)
   * @param tweakDescription The description to look for
   * @return the tweak or NONE if nothing matched
   */
  public TransTweak getTweakForDescription(String tweakDescription) {
    if (StringUtils.isEmpty(tweakDescription)) {
      return TransTweak.NONE;
    }
    int index = Const.indexOfString(tweakDescription, tweakDesc);
    if (index<0) {
      return TransTweak.NONE;
    }
    return TransTweak.values()[index];
  }
  
  public static final String getTestTypeDescription(TestType testType) {
    int index = 0; // NONE
    if (testType!=null) {
      TestType[] testTypes = TestType.values();
      for (int i=0;i<testTypes.length;i++) {
        if (testTypes[i]==testType) {
          index=i;
          break;
        }
      }
    }
    
    return testTypeDesc[index];
  }

  /**
   * Get the TestType for a tweak description (from the dialog)
   * @param testTypeDescription The description to look for
   * @return the test type or NONE if nothing matched
   */
  public static final TestType getTestTypeForDescription(String testTypeDescription) {
    if (StringUtils.isEmpty(testTypeDescription)) {
      return TestType.NONE;
    }
    int index = Const.indexOfString(testTypeDescription, testTypeDesc);
    if (index<0) {
      return TestType.NONE;
    }
    return TestType.values()[index];
  }
  
  public static final String[] getTestTypeDescriptions() {
    return testTypeDesc;
  }
  
  public static final String[] getTweakDescriptions() {
    return tweakDesc;
  }
}

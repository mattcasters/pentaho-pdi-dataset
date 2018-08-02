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

package org.pentaho.di.dataset.spoon;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.MessageBox;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.SourceToTargetMapping;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.gui.SpoonFactory;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.dataset.DataSet;
import org.pentaho.di.dataset.DataSetField;
import org.pentaho.di.dataset.DataSetGroup;
import org.pentaho.di.dataset.TransTweak;
import org.pentaho.di.dataset.TransUnitTest;
import org.pentaho.di.dataset.TransUnitTestFieldMapping;
import org.pentaho.di.dataset.TransUnitTestSetLocation;
import org.pentaho.di.dataset.TransUnitTestTweak;
import org.pentaho.di.dataset.spoon.dialog.DataSetDialog;
import org.pentaho.di.dataset.spoon.dialog.DataSetGroupDialog;
import org.pentaho.di.dataset.spoon.dialog.EditRowsDialog;
import org.pentaho.di.dataset.spoon.xtpoint.InjectDataSetIntoTransExtensionPoint;
import org.pentaho.di.dataset.util.DataSetConst;
import org.pentaho.di.dataset.util.FactoriesHierarchy;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryDirectory;
import org.pentaho.di.shared.SharedObjectInterface;
import org.pentaho.di.shared.SharedObjects;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.ui.core.dialog.EnterMappingDialog;
import org.pentaho.di.ui.core.dialog.EnterSelectionDialog;
import org.pentaho.di.ui.core.dialog.EnterStringDialog;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.spoon.ISpoonMenuController;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.spoon.trans.TransGraph;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.util.PentahoDefaults;
import org.pentaho.ui.xul.dom.Document;
import org.pentaho.ui.xul.impl.AbstractXulEventHandler;

public class DataSetHelper extends AbstractXulEventHandler implements ISpoonMenuController {
  protected static Class<?> PKG = DataSetHelper.class; // for i18n

  private static DataSetHelper instance = null;
    
  private DataSetHelper() {
  }

  public static DataSetHelper getInstance() {
    if ( instance == null ) {
      instance = new DataSetHelper();
      Spoon spoon = ( (Spoon) SpoonFactory.getInstance() );
      spoon.addSpoonMenuController( instance );
    }
    return instance;
  }

  public String getName() {
    return "dataSetHelper";
  }

  public void updateMenu( Document doc ) {
    // Nothing so far.
  }

  public void manage() {
  }

  public List<DatabaseMeta> getAvailableDatabases( Repository repository ) throws KettleException {
    List<DatabaseMeta> list = new ArrayList<DatabaseMeta>();

    // Load database connections from the central repository if we're connected to one
    //
    if ( repository != null ) {
      ObjectId[] databaseIDs = repository.getDatabaseIDs( false );
      for ( ObjectId databaseId : databaseIDs ) {
        list.add( repository.loadDatabaseMeta( databaseId, null ) );
      }
    }

    // Also load from the standard shared objects file
    //
    SharedObjects sharedObjects = new SharedObjects( Const.getSharedObjectsFile() );
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

  public void editDataSetGroup() {

    Spoon spoon = ( (Spoon) SpoonFactory.getInstance() );

    IMetaStore metaStore = spoon.getMetaStore();
    MetaStoreFactory<DataSetGroup> groupFactory = new MetaStoreFactory<DataSetGroup>( DataSetGroup.class, metaStore, PentahoDefaults.NAMESPACE );
    try {

      List<String> groupNames = groupFactory.getElementNames();
      Collections.sort( groupNames );
      EnterSelectionDialog esd = new EnterSelectionDialog( spoon.getShell(), groupNames.toArray( new String[groupNames.size()] ), "Select the group", "Select the group to edit..." );
      String groupName = esd.open();
      if ( groupName != null ) {
        List<DatabaseMeta> databases = getAvailableDatabases( spoon.getRepository() );
        groupFactory.addNameList( DataSetConst.DATABASE_LIST_KEY, databases );
        DataSetGroup dataSetGroup = groupFactory.loadElement( groupName );

        DataSetGroupDialog groupDialog = new DataSetGroupDialog( spoon.getShell(), dataSetGroup, databases );
        while ( groupDialog.open() ) {
          String message = validateDataSetGroup( dataSetGroup, groupName, groupFactory.getElementNames() );

          // Save the group ...
          //
          if ( message == null ) {
            groupFactory.saveElement( dataSetGroup );
            break;
          } else {
            MessageBox box = new MessageBox( spoon.getShell(), SWT.OK );
            box.setText( "Error" );
            box.setMessage( message );
            box.open();
          }
        }
      }
    } catch ( Exception e ) {
      new ErrorDialog( spoon.getShell(), "Error", "Error retrieving the list of data set groups", e );
    }
  }

  public void addDataSetGroup() {

    Spoon spoon = ( (Spoon) SpoonFactory.getInstance() );

    IMetaStore metaStore = spoon.getMetaStore();
    MetaStoreFactory<DataSetGroup> groupFactory = new MetaStoreFactory<DataSetGroup>( DataSetGroup.class, metaStore, PentahoDefaults.NAMESPACE );

    try {

      DataSetGroup dataSetGroup = new DataSetGroup();
      List<DatabaseMeta> databases = getAvailableDatabases( spoon.getRepository() );

      DataSetGroupDialog groupDialog = new DataSetGroupDialog( spoon.getShell(), dataSetGroup, databases );
      while ( groupDialog.open() ) {
        // Verify empty name, existing name...
        //
        String message = validateDataSetGroup( dataSetGroup, null, groupFactory.getElementNames() );

        // Save the group again...
        //
        if ( message == null ) {
          groupFactory.saveElement( dataSetGroup );
          break;
        } else {
          MessageBox box = new MessageBox( spoon.getShell(), SWT.OK );
          box.setText( "Error" );
          box.setMessage( message );
          box.open();
        }
      }
    } catch ( Exception e ) {
      new ErrorDialog( spoon.getShell(), "Error", "Error creating a new data set group", e );
    }
  }
  
  public void removeDataSetGroup() {

    Spoon spoon = ( (Spoon) SpoonFactory.getInstance() );

    IMetaStore metaStore = spoon.getMetaStore();
    MetaStoreFactory<DataSetGroup> groupFactory = new MetaStoreFactory<DataSetGroup>( DataSetGroup.class, metaStore, PentahoDefaults.NAMESPACE );
    
    try {

      List<String> groupNames = groupFactory.getElementNames();
      Collections.sort( groupNames );
      EnterSelectionDialog esd = new EnterSelectionDialog( spoon.getShell(), groupNames.toArray( new String[groupNames.size()] ), "Select the group", "Select the group to edit..." );
      String groupName = esd.open();
      if ( groupName != null ) {
        
        // TODO: Find the unit tests for this group, if there are any, we can't remove the group
        //
        groupFactory.deleteElement(groupName);
      }
    } catch ( Exception e ) {
      new ErrorDialog( spoon.getShell(), "Error", "Error retrieving the list of data set groups or deleting a group", e );
    }
  }


  private String validateDataSetGroup( DataSetGroup dataSetGroup, String previousName, List<String> groupNames ) {

    String message = null;

    String newName = dataSetGroup.getName();
    if ( StringUtil.isEmpty( newName ) ) {
      message = BaseMessages.getString( PKG, "DataSetHelper.DataSetGroup.NoNameSpecified.Message" );
    } else if ( !StringUtil.isEmpty( previousName ) && !previousName.equals( newName ) ) {
      message = BaseMessages.getString( PKG, "DataSetHelper.DataSetGroup.RenamingOfADataSetNotSupported.Message" );
    } else if ( dataSetGroup.getDatabaseMeta() == null ) {
      message = BaseMessages.getString( PKG, "DataSetHelper.DataSetGroup.NoDatabaseSpecified.Message" );
    } else {
      if ( StringUtil.isEmpty( previousName ) && Const.indexOfString( newName, groupNames ) >= 0 ) {
        message = BaseMessages.getString( PKG, "DataSetHelper.DataSetGroup.AGroupWithNameExists.Message", newName );
      }
    }

    return message;
  }

  private String validateDataSet( DataSet dataSet, String previousName, List<String> setNames ) {

    String message = null;

    String newName = dataSet.getName();
    if ( StringUtil.isEmpty( newName ) ) {
      message = BaseMessages.getString( PKG, "DataSetHelper.DataSet.NoNameSpecified.Message" );
    } else if ( !StringUtil.isEmpty( previousName ) && !previousName.equals( newName ) ) {
      message = BaseMessages.getString( PKG, "DataSetHelper.DataSet.RenamingOfADataSetsNotSupported.Message" );
    } else if ( dataSet.getGroup() == null ) {
      message = BaseMessages.getString( PKG, "DataSetHelper.DataSet.NoGroupSpecified.Message" );
    } else {
      if ( StringUtil.isEmpty( previousName ) && Const.indexOfString( newName, setNames ) >= 0 ) {
        message = BaseMessages.getString( PKG, "DataSetHelper.DataSet.ADataSetWithNameExists.Message", newName );
      }
    }

    return message;
  }

  public void addDataSet() {

    Spoon spoon = ( (Spoon) SpoonFactory.getInstance() );

    IMetaStore metaStore = spoon.getMetaStore();

    try {
      MetaStoreFactory<DataSetGroup> groupFactory = new MetaStoreFactory<DataSetGroup>( DataSetGroup.class, metaStore, PentahoDefaults.NAMESPACE );
      List<DatabaseMeta> databases = getAvailableDatabases( spoon.getRepository() );
      groupFactory.addNameList( DataSetConst.DATABASE_LIST_KEY, databases );
      List<DataSetGroup> groups = groupFactory.getElements();

      MetaStoreFactory<DataSet> setFactory = new MetaStoreFactory<DataSet>( DataSet.class, metaStore, PentahoDefaults.NAMESPACE );
      setFactory.addNameList( DataSetConst.GROUP_LIST_KEY, groups );

      DataSet dataSet = new DataSet();

      editDataSet( spoon, dataSet, groups, setFactory, null );

    } catch ( Exception e ) {
      new ErrorDialog( spoon.getShell(), "Error", "Error creating a new data set", e );
    }
  }

  public void editDataSet() {

    Spoon spoon = ( (Spoon) SpoonFactory.getInstance() );
    IMetaStore metaStore = spoon.getMetaStore();

    try {
      MetaStoreFactory<DataSetGroup> groupFactory = new MetaStoreFactory<DataSetGroup>( DataSetGroup.class, metaStore, PentahoDefaults.NAMESPACE );
      List<DatabaseMeta> databases = getAvailableDatabases( spoon.getRepository() );
      groupFactory.addNameList( DataSetConst.DATABASE_LIST_KEY, databases );
      List<DataSetGroup> groups = groupFactory.getElements();

      MetaStoreFactory<DataSet> setFactory = new MetaStoreFactory<DataSet>( DataSet.class, metaStore, PentahoDefaults.NAMESPACE );
      setFactory.addNameList( DataSetConst.GROUP_LIST_KEY, groups );

      List<String> setNames = setFactory.getElementNames();
      Collections.sort( setNames );
      EnterSelectionDialog esd = new EnterSelectionDialog( spoon.getShell(), setNames.toArray( new String[setNames.size()] ), "Select the set", "Select the data set to edit..." );
      String setName = esd.open();
      if ( setName != null ) {
        DataSet dataSet = setFactory.loadElement( setName );

        editDataSet( spoon, dataSet, groups, setFactory, setName );
      }
    } catch ( Exception e ) {
      new ErrorDialog( spoon.getShell(), "Error", "Error retrieving the list of data set groups", e );
    }
  }

  private void editDataSet( Spoon spoon, DataSet dataSet, List<DataSetGroup> groups, MetaStoreFactory<DataSet> setFactory, String setName ) throws MetaStoreException {
    
    try {
      DataSetDialog setDialog = new DataSetDialog( spoon.getShell(), setFactory.getMetaStore(), dataSet, groups, getAvailableDatabases(spoon.getRepository()) );
      while ( setDialog.open() ) {
        String message = validateDataSet( dataSet, setName, setFactory.getElementNames() );
  
        // Save the data set...
        //
        if ( message == null ) {
          setFactory.saveElement( dataSet );
          break;
        } else {
          MessageBox box = new MessageBox( spoon.getShell(), SWT.OK );
          box.setText( "Error" );
          box.setMessage( message );
          box.open();
        }
      }
    } catch(Exception e) {
      new ErrorDialog(spoon.getShell(), "Error", "Unable to edit data set", e);
    }

  }

  /**
   * We set an input data set
   */
  public void setInputDataSet() {
    Spoon spoon = ( (Spoon) SpoonFactory.getInstance() );
    TransGraph transGraph = spoon.getActiveTransGraph();
    TransMeta transMeta = spoon.getActiveTransformation();
    StepMeta stepMeta = transGraph.getCurrentStep();
    if ( transGraph == null || transMeta == null || stepMeta == null ) {
      return;
    }
    IMetaStore metaStore = spoon.getMetaStore();

    if (checkTestPresent(spoon, transMeta)) {
      return;
    }
    
    try {
      
      List<DatabaseMeta> databases = getAvailableDatabases( spoon.getRepository() );
      FactoriesHierarchy hierarchy = new FactoriesHierarchy( metaStore, databases );
      
      MetaStoreFactory<DataSet> setFactory = hierarchy.getSetFactory();
      List<String> setNames = setFactory.getElementNames();
      Collections.sort( setNames );
      EnterSelectionDialog esd = new EnterSelectionDialog( spoon.getShell(), setNames.toArray( new String[setNames.size()] ), "Select the set", "Select the data set to edit..." );
      String setName = esd.open();
      if ( setName != null ) {
        DataSet dataSet = setFactory.loadElement( setName );
        
        stepMeta.setAttribute( DataSetConst.ATTR_GROUP_DATASET, DataSetConst.ATTR_STEP_DATASET_GOLDEN, null);
        stepMeta.setAttribute( DataSetConst.ATTR_GROUP_DATASET, DataSetConst.ATTR_STEP_DATASET_INPUT, dataSet.getName() );
        
        // Now we need to map the fields from the input data set to the step...
        //
        RowMetaInterface setFields = dataSet.getSetRowMeta( false );
        RowMetaInterface stepFields = transMeta.getStepFields( stepMeta );
        if (stepFields.isEmpty()) {
          stepFields = setFields.clone();
        }
        
        String[] stepFieldNames = stepFields.getFieldNames();
        String[] setFieldNames = setFields.getFieldNames();
        
        EnterMappingDialog mappingDialog = new EnterMappingDialog(spoon.getShell(), setFieldNames, stepFieldNames);
        List<SourceToTargetMapping> mappings = mappingDialog.open();
        if (mappings==null) {
          return;
        }
        
        // Ask about the sort order...
        // Show the mapping as well as an order column
        //
        RowMetaInterface sortMeta = new RowMeta();
        sortMeta.addValueMeta(new ValueMetaString(BaseMessages.getString(PKG, "DataSetHelper.SortOrder.Column.SetField")));
        List<Object[]> sortData = new ArrayList<Object[]>();
        for (String setFieldName : setFieldNames) {
          sortData.add(new Object[] { setFieldName });
        }
        EditRowsDialog orderDialog = new EditRowsDialog(spoon.getShell(), SWT.NONE, 
            BaseMessages.getString(PKG, "DataSetHelper.SortOrder.Title"),
            BaseMessages.getString(PKG, "DataSetHelper.SortOrder.Message"),
            sortMeta, sortData            
            );
        List<Object[]> orderMappings = orderDialog.open();
        if (orderMappings==null) {
          return;
        }
        
        // What is the unit test we are using?
        //
        String testName = transMeta.getAttribute( DataSetConst.ATTR_GROUP_DATASET, DataSetConst.ATTR_TRANS_SELECTED_UNIT_TEST_NAME );
        if (StringUtil.isEmpty( testName )) {
          return;
        }
        TransUnitTest unitTest = hierarchy.getTestFactory().loadElement( testName );
        if (unitTest==null) {
          // Show a message box later
          //
          return;
        }
        
        // Modify the test
        //
        
        // Remove other crap on the step...
        //
        unitTest.removeInputAndGoldenDataSets(stepMeta.getName());
        
        TransUnitTestSetLocation inputLocation = new TransUnitTestSetLocation();
        unitTest.getInputDataSets().add( inputLocation );
        
        inputLocation.setStepname( stepMeta.getName() );
        inputLocation.setDataSetName( dataSet.getName() );
        List<TransUnitTestFieldMapping> fieldMappings = inputLocation.getFieldMappings();
        fieldMappings.clear();
        
        for (SourceToTargetMapping mapping : mappings) {
          String stepFieldName = mapping.getTargetString(stepFieldNames);
          String setFieldName = mapping.getSourceString(setFieldNames);
          fieldMappings.add( new TransUnitTestFieldMapping(stepFieldName, setFieldName) );
        }
        
        List<String> setFieldOrder = new ArrayList<String>();
        for (Object[] orderMapping : orderMappings) {
          setFieldOrder.add(sortMeta.getString(orderMapping, 0));
        }
        inputLocation.setFieldOrder(setFieldOrder);
        
        // Save the unit test...
        //
        hierarchy.getTestFactory().saveElement( unitTest );
        
        stepMeta.setChanged();

        spoon.refreshGraph();        
      }
    } catch ( Exception e ) {
      new ErrorDialog( spoon.getShell(), "Error", "Error retrieving the list of data set groups", e );
    }
  }
  
  private boolean checkTestPresent(Spoon spoon, TransMeta transMeta) {
    
    spoon.getLog().logBasic("Check test present...");
    
    String testName = transMeta.getAttribute( DataSetConst.ATTR_GROUP_DATASET, DataSetConst.ATTR_TRANS_SELECTED_UNIT_TEST_NAME );
    if (!StringUtil.isEmpty( testName )) {
      return false;
    }
    
    // there is no test defined of selected in the transformation.
    // Show a warning
    //
    MessageBox box = new MessageBox(spoon.getShell(), SWT.OK | SWT.ICON_INFORMATION );
    box.setMessage("Please create a test-case first by left clicking on the test icon.");
    box.setText("First create a test-case");
    box.open();
    
    return true;
  }

  /**
   * We set an golden data set on the selected unit test
   */
  public void setGoldenDataSet() {
    Spoon spoon = ( (Spoon) SpoonFactory.getInstance() );
    TransGraph transGraph = spoon.getActiveTransGraph();
    TransMeta transMeta = spoon.getActiveTransformation();
    StepMeta stepMeta = transGraph.getCurrentStep();
    if ( transGraph == null || transMeta == null || stepMeta == null ) {
      return;
    }
    IMetaStore metaStore = spoon.getMetaStore();

    try {
      
      List<DatabaseMeta> databases = getAvailableDatabases( spoon.getRepository() );
      FactoriesHierarchy hierarchy = new FactoriesHierarchy( metaStore, databases );
      
      MetaStoreFactory<DataSet> setFactory = hierarchy.getSetFactory();
      List<String> setNames = setFactory.getElementNames();
      Collections.sort( setNames );
      EnterSelectionDialog esd = new EnterSelectionDialog( spoon.getShell(), setNames.toArray( new String[setNames.size()] ), "Select the golden data set", "Select the golden data set..." );
      String setName = esd.open();
      if ( setName != null ) {
        DataSet dataSet = setFactory.loadElement( setName );

        // Clear possible input data set name
        stepMeta.setAttribute(DataSetConst.ATTR_GROUP_DATASET, DataSetConst.ATTR_STEP_DATASET_INPUT, null);
        // Set golden data set name
        stepMeta.setAttribute( DataSetConst.ATTR_GROUP_DATASET, DataSetConst.ATTR_STEP_DATASET_GOLDEN, dataSet.getName() );
        
        // Now we need to map the fields from the step to golden data set fields...
        //
        RowMetaInterface stepFields = transMeta.getPrevStepFields( stepMeta );
        RowMetaInterface setFields = dataSet.getSetRowMeta( false );
        
        String[] stepFieldNames = stepFields.getFieldNames();
        String[] setFieldNames = setFields.getFieldNames();
        
        EnterMappingDialog mappingDialog = new EnterMappingDialog(spoon.getShell(), stepFieldNames, setFieldNames);
        List<SourceToTargetMapping> mappings = mappingDialog.open();
        if (mappings==null) {
          return;
        }
        
        // Ask about the sort order...
        // Show the mapping as well as an order column
        //
        RowMetaInterface sortMeta = new RowMeta();
        sortMeta.addValueMeta(new ValueMetaString(BaseMessages.getString(PKG, "DataSetHelper.SortOrder.Column.SetField")));
        List<Object[]> sortData = new ArrayList<Object[]>();
        for (String setFieldName : setFieldNames) {
          sortData.add(new Object[] { setFieldName });
        }
        EditRowsDialog orderDialog = new EditRowsDialog(spoon.getShell(), SWT.NONE, 
            BaseMessages.getString(PKG, "DataSetHelper.SortOrder.Title"),
            BaseMessages.getString(PKG, "DataSetHelper.SortOrder.Message"),
            sortMeta, sortData            
            );
        List<Object[]> orderMappings = orderDialog.open();
        if (orderMappings==null) {
          return;
        }
        
        // What is the unit test we are using?
        //
        String testName = transMeta.getAttribute( DataSetConst.ATTR_GROUP_DATASET, DataSetConst.ATTR_TRANS_SELECTED_UNIT_TEST_NAME );
        if (StringUtil.isEmpty( testName )) {
          return;
        }
        TransUnitTest goldenTest = hierarchy.getTestFactory().loadElement( testName );
        if (goldenTest==null) {
          // Show a message box later
          //
          return;
        }
        
        // Modify the test
        //
        
        // Remove golden locations and input locations on the step to avoid duplicates
        //
        goldenTest.removeInputAndGoldenDataSets(stepMeta.getName());
        
        TransUnitTestSetLocation goldenLocation = new TransUnitTestSetLocation();
        goldenTest.getGoldenDataSets().add( goldenLocation );
        
        goldenLocation.setStepname( stepMeta.getName() );
        goldenLocation.setDataSetName( dataSet.getName() );
        List<TransUnitTestFieldMapping> fieldMappings = goldenLocation.getFieldMappings();
        fieldMappings.clear();

        for (SourceToTargetMapping mapping : mappings) {
          fieldMappings.add( new TransUnitTestFieldMapping(
              mapping.getSourceString( stepFieldNames ),
              mapping.getTargetString( setFieldNames )) );
        }
        
        List<String> setFieldOrder = new ArrayList<String>();
        for (Object[] orderMapping : orderMappings) {
          setFieldOrder.add(sortMeta.getString(orderMapping, 0));
        }
        goldenLocation.setFieldOrder(setFieldOrder);
        
        System.out.println("###### golden data sort order: "+goldenTest.getGoldenDataSets().get(0).getFieldOrder()+" ######");
        
        // Save the unit test...
        //
        hierarchy.getTestFactory().saveElement( goldenTest );
        
        stepMeta.setChanged();

        spoon.refreshGraph();       
      }
    } catch ( Exception e ) {
      new ErrorDialog( spoon.getShell(), "Error", "Error retrieving the list of data set groups", e );
    }
  }


  public void clearInputDataSet() {
    Spoon spoon = ( (Spoon) SpoonFactory.getInstance() );
    TransGraph transGraph = spoon.getActiveTransGraph();
    TransMeta transMeta = spoon.getActiveTransformation();
    StepMeta stepMeta = transGraph.getCurrentStep();
    if ( transGraph == null || transMeta == null || stepMeta == null ) {
      return;
    }

    stepMeta.setAttribute( DataSetConst.ATTR_GROUP_DATASET, DataSetConst.ATTR_STEP_DATASET_INPUT, null );
    transGraph.redraw();
  }
  
  public void clearGoldenDataSet() {
    Spoon spoon = ( (Spoon) SpoonFactory.getInstance() );
    TransGraph transGraph = spoon.getActiveTransGraph();
    TransMeta transMeta = spoon.getActiveTransformation();
    StepMeta stepMeta = transGraph.getCurrentStep();
    if ( transGraph == null || transMeta == null || stepMeta == null ) {
      return;
    }

    stepMeta.setAttribute( DataSetConst.ATTR_GROUP_DATASET, DataSetConst.ATTR_STEP_DATASET_GOLDEN, null );
    transGraph.redraw();
  }

  /**
   * Create a new data set with the output from 
   */
  public void createDataSetFromStep() {
    Spoon spoon = ( (Spoon) SpoonFactory.getInstance() );
    TransGraph transGraph = spoon.getActiveTransGraph();
    IMetaStore metaStore = spoon.getMetaStore();
    if ( transGraph == null ) {
      return;
    }
    StepMeta stepMeta = transGraph.getCurrentStep();
    TransMeta transMeta = spoon.getActiveTransformation();
    if ( stepMeta == null || transMeta == null ) {
      return;
    }

    try {
      MetaStoreFactory<DataSetGroup> groupFactory = new MetaStoreFactory<DataSetGroup>( DataSetGroup.class, metaStore, PentahoDefaults.NAMESPACE );
      List<DatabaseMeta> databases = getAvailableDatabases( spoon.getRepository() );
      groupFactory.addNameList( DataSetConst.DATABASE_LIST_KEY, databases );
      List<DataSetGroup> groups = groupFactory.getElements();

      MetaStoreFactory<DataSet> setFactory = new MetaStoreFactory<DataSet>( DataSet.class, metaStore, PentahoDefaults.NAMESPACE );
      setFactory.addNameList( DataSetConst.GROUP_LIST_KEY, groups );

      DataSet dataSet = new DataSet();
      RowMetaInterface rowMeta = transMeta.getStepFields( stepMeta );
      for ( int i = 0; i < rowMeta.size(); i++ ) {
        ValueMetaInterface valueMeta = rowMeta.getValueMeta( i );
        DataSetField field = new DataSetField( valueMeta.getName(), "field" + i, valueMeta.getType(), valueMeta.getLength(), valueMeta.getPrecision(), valueMeta.getComments() );
        dataSet.getFields().add( field );
      }

      editDataSet( spoon, dataSet, groups, setFactory, null );

    } catch ( Exception e ) {
      new ErrorDialog( spoon.getShell(), "Error", "Error creating a new data set", e );
    }
  }

  /**
   * Ask which data set to write to
   * Ask for the mapping between the output row and the data set field
   * Start the transformation and capture the output of the step, write to the database table backing the data set.
   * 
   */
  public void writeStepDataToDataSet() {
    Spoon spoon = ( (Spoon) SpoonFactory.getInstance() );
    TransGraph transGraph = spoon.getActiveTransGraph();
    IMetaStore metaStore = spoon.getMetaStore();
    if ( transGraph == null ) {
      return;
    }
    StepMeta stepMeta = transGraph.getCurrentStep();
    TransMeta transMeta = spoon.getActiveTransformation();
    if ( stepMeta == null || transMeta == null ) {
      return;
    }

    if ( transMeta.hasChanged() ) {
      MessageBox box = new MessageBox( spoon.getShell(), SWT.OK | SWT.ICON_INFORMATION );
      box.setText( "Save transformation" );
      box.setMessage( "Please save your transformation first." );
      box.open();
      return;
    }

    try {

      MetaStoreFactory<DataSetGroup> groupFactory = new MetaStoreFactory<DataSetGroup>( DataSetGroup.class, metaStore, PentahoDefaults.NAMESPACE );
      List<DatabaseMeta> databases = getAvailableDatabases( spoon.getRepository() );
      groupFactory.addNameList( DataSetConst.DATABASE_LIST_KEY, databases );
      List<DataSetGroup> groups = groupFactory.getElements();

      MetaStoreFactory<DataSet> setFactory = new MetaStoreFactory<DataSet>( DataSet.class, metaStore, PentahoDefaults.NAMESPACE );
      setFactory.addNameList( DataSetConst.GROUP_LIST_KEY, groups );

      // Ask which data set to write to
      //
      List<String> setNames = setFactory.getElementNames();
      Collections.sort( setNames );
      EnterSelectionDialog esd = new EnterSelectionDialog( spoon.getShell(), setNames.toArray( new String[setNames.size()] ), "Select the set", "Select the data set to edit..." );
      String setName = esd.open();
      if ( setName == null ) {
        return;
      }

      DataSet dataSet = setFactory.loadElement( setName );
      String[] setFields = new String[dataSet.getFields().size()];
      for ( int i = 0; i < setFields.length; i++ ) {
        setFields[i] = dataSet.getFields().get( i ).getFieldName();
      }

      RowMetaInterface rowMeta = transMeta.getStepFields( stepMeta );
      String[] stepFields = new String[rowMeta.size()];
      for ( int i = 0; i < rowMeta.size(); i++ ) {
        ValueMetaInterface valueMeta = rowMeta.getValueMeta( i );
        stepFields[i] = valueMeta.getName();
      }

      // Ask for the mapping between the output row and the data set field
      //
      EnterMappingDialog mappingDialog = new EnterMappingDialog( spoon.getShell(), stepFields, setFields );
      List<SourceToTargetMapping> mapping = mappingDialog.open();
      if ( mapping == null ) {
        return;
      }

      // Run the transformation.  We want to use the standard Spoon runFile() method
      // So we need to leave the source to target mapping list somewhere so it can be picked up later.
      // For now we'll leave it where we need it.
      //
      InjectDataSetIntoTransExtensionPoint.stepsMap.put( transMeta.getName(), stepMeta );
      InjectDataSetIntoTransExtensionPoint.mappingsMap.put( transMeta.getName(), mapping );
      InjectDataSetIntoTransExtensionPoint.setsMap.put( transMeta.getName(), dataSet );

      spoon.runFile();

    } catch ( Exception e ) {
      new ErrorDialog( spoon.getShell(), "Error", "Error creating a new data set", e );
    }
  }


  public void createUnitTest() {
    Spoon spoon = ( (Spoon) SpoonFactory.getInstance() );
    Repository repository = spoon.getRepository();
    try {
      TransGraph transGraph = spoon.getActiveTransGraph();
      IMetaStore metaStore = spoon.getMetaStore();
      if ( transGraph == null ) {
        return;
      }
      TransMeta transMeta = transGraph.getTransMeta();
      
      EnterStringDialog stringDialog = new EnterStringDialog( spoon.getShell(), "", "Enter unit test name", "Unit test name: " );
      String testName = stringDialog.open();
      if (testName==null) {
        return;
      }
      
      MetaStoreFactory<TransUnitTest> testFactory = new MetaStoreFactory<TransUnitTest>( TransUnitTest.class, metaStore, PentahoDefaults.NAMESPACE);
      if (testFactory.loadElement( testName )!=null) {
        MessageBox box = new MessageBox( spoon.getShell(), SWT.YES | SWT.NO  );
        box.setText( "A test with that name exists" );
        box.setMessage( "A test with that name already exists.  Would you like to use and edit this test in this transformation?" );
        int answer = box.open();
        if ((answer&SWT.YES)!=0) {
          transMeta.setAttribute( DataSetConst.ATTR_GROUP_DATASET, DataSetConst.ATTR_TRANS_SELECTED_UNIT_TEST_NAME, testName );
          transGraph.redraw();
        }
        return;
      }
      
      TransUnitTest test = new TransUnitTest();
      test.setName( testName );
      
      if ( repository != null ) {
        test.setTransRepositoryPath( transMeta.getRepositoryDirectory().getPath() + RepositoryDirectory.DIRECTORY_SEPARATOR + transMeta.getName() );
        if ( repository.getRepositoryMeta().getRepositoryCapabilities().supportsReferences() ) {
          test.setTransObjectId( transMeta.getObjectId().toString() );
        } else {
          test.setTransRepositoryPath( transMeta.getRepositoryDirectory().getPath() + "/" + transMeta.getName() );
        }
      } else {
        test.setTransFilename( transMeta.getFilename() );
      }

      testFactory.saveElement( test );
      transMeta.setAttribute( DataSetConst.ATTR_GROUP_DATASET, DataSetConst.ATTR_TRANS_SELECTED_UNIT_TEST_NAME, testName );
      
      // Don't carry on old indicators...
      //
      DataSetConst.clearStepDataSetIndicators( transMeta );
      
      spoon.refreshGraph();
      
    } catch ( Exception e ) {
      new ErrorDialog( spoon.getShell(), "Error", "Error creating a new transformation unit test", e );
    }
  }
  
  public void detachUnitTest() {
    Spoon spoon = ( (Spoon) SpoonFactory.getInstance() );
    try {
      TransGraph transGraph = spoon.getActiveTransGraph();
      if ( transGraph == null ) {
        return;
      }
      TransMeta transMeta = spoon.getActiveTransformation();
      if (transMeta == null ) {
        return;
      }

      transMeta.setAttribute( DataSetConst.ATTR_GROUP_DATASET, DataSetConst.ATTR_TRANS_SELECTED_UNIT_TEST_NAME, null );
      transMeta.setChanged();
      
      DataSetConst.clearStepDataSetIndicators( transMeta );
      
      spoon.refreshGraph();
    } catch ( Exception e ) {
      new ErrorDialog( spoon.getShell(), "Error", "Error detaching unit test", e );
    }
  }

  public void selectUnitTest() {
    
    Spoon spoon = ( (Spoon) SpoonFactory.getInstance() );
    try {
      TransGraph transGraph = spoon.getActiveTransGraph();
      IMetaStore metaStore = spoon.getMetaStore();
      if ( transGraph == null ) {
        return;
      }
      TransMeta transMeta = spoon.getActiveTransformation();
      if ( transMeta == null ) {
        return;
      }

      MetaStoreFactory<TransUnitTest> testFactory = new MetaStoreFactory<TransUnitTest>( TransUnitTest.class, metaStore, PentahoDefaults.NAMESPACE);
      List<String> testNames = testFactory.getElementNames();
      String[] names = testNames.toArray( new String[testNames.size()] );
      Arrays.sort( names );
      EnterSelectionDialog esd = new EnterSelectionDialog( spoon.getShell(), names, "Select a unit test", "Select the unit test to use" );
      String testName = esd.open();
      if ( testName != null ) {
        
        TransUnitTest unitTest = testFactory.loadElement( testName );
        if (unitTest==null) {
          throw new KettleException( "Unit test '"+testName+"' could not be found (deleted)?" );
        }

        selectUnitTest(transMeta, unitTest);
        Spoon.getInstance().refreshGraph();
      }
    } catch ( Exception e ) {
      new ErrorDialog( spoon.getShell(), "Error", "Error selecting a new transformation unit test", e );
    }
  }
  
  public static final void selectUnitTest(TransMeta transMeta, TransUnitTest unitTest) throws KettleException {
    transMeta.setAttribute( DataSetConst.ATTR_GROUP_DATASET, DataSetConst.ATTR_TRANS_SELECTED_UNIT_TEST_NAME, unitTest.getName() );
    
    DataSetConst.loadStepDataSetIndicators( transMeta, unitTest);

    transMeta.setChanged();
  }

  public TransUnitTest getCurrentUnitTest(TransMeta transMeta) throws MetaStoreException, KettleException {
    // What is the unit test we are using?
    //
    String testName = transMeta.getAttribute( DataSetConst.ATTR_GROUP_DATASET, DataSetConst.ATTR_TRANS_SELECTED_UNIT_TEST_NAME );
    if (StringUtil.isEmpty( testName )) {
      return null;
    }
    Spoon spoon = Spoon.getInstance();
    List<DatabaseMeta> databases = getAvailableDatabases( spoon.getRepository() );
    FactoriesHierarchy hierarchy = new FactoriesHierarchy( spoon.getMetaStore(), databases );
    TransUnitTest unitTest = hierarchy.getTestFactory().loadElement( testName );
    return unitTest;
  }
  
  public void enableTweakRemoveStepInUnitTest() {
    tweakRemoveStepInUnitTest(true);
  }
  public void disableTweakRemoveStepInUnitTest() {
    tweakRemoveStepInUnitTest(false);
  }
  public void tweakRemoveStepInUnitTest(boolean enable) {
    tweakUnitTestStep(TransTweak.REMOVE_STEP, enable);
  }

  private void tweakUnitTestStep(TransTweak stepTweak, boolean enable) {
    Spoon spoon = ( (Spoon) SpoonFactory.getInstance() );
    TransGraph transGraph = spoon.getActiveTransGraph();
    IMetaStore metaStore = spoon.getMetaStore();
    if ( transGraph == null ) {
      return;
    }
    StepMeta stepMeta = transGraph.getCurrentStep();
    TransMeta transMeta = spoon.getActiveTransformation();
    if ( stepMeta == null || transMeta == null ) {
      return;
    }

    try {
      TransUnitTest unitTest = getCurrentUnitTest(transMeta);
      if (unitTest==null) {
        return;
      }
      TransUnitTestTweak unitTestTweak = unitTest.findTweak(stepMeta.getName());
      if (unitTestTweak!=null) {
        unitTest.getTweaks().remove(unitTestTweak);
        stepMeta.setAttribute(DataSetConst.ATTR_GROUP_DATASET, DataSetConst.ATTR_STEP_TWEAK, null);
      }
      if (enable) {
        unitTest.getTweaks().add(new TransUnitTestTweak(stepTweak, stepMeta.getName()));
        stepMeta.setAttribute(DataSetConst.ATTR_GROUP_DATASET, DataSetConst.ATTR_STEP_TWEAK, stepTweak.name());
      }
      
      new MetaStoreFactory<TransUnitTest>(TransUnitTest.class, metaStore, PentahoDefaults.NAMESPACE)
        .saveElement(unitTest);
      
      spoon.refreshGraph();
      
    } catch(Exception exception) {
      new ErrorDialog( spoon.getShell(), "Error", "Error tweaking transformation unit test on step '"+stepMeta.getName()+"' with operation "+stepTweak.name(), exception );
    }
  }

  public void enableTweakBypassStepInUnitTest() {
    tweakBypassStepInUnitTest(true);
  }
  public void disableTweakBypassStepInUnitTest() {
    tweakBypassStepInUnitTest(false);
  }
  public void tweakBypassStepInUnitTest(boolean enable) {
    tweakUnitTestStep(TransTweak.BYPASS_STEP, enable);
  }

}

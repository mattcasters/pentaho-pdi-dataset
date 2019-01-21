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

import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.MessageBox;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.SourceToTargetMapping;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.gui.SpoonFactory;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.core.vfs.KettleVFS;
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
import org.pentaho.di.dataset.spoon.dialog.TransUnitTestDialog;
import org.pentaho.di.dataset.spoon.xtpoint.TransMetaModifier;
import org.pentaho.di.dataset.spoon.xtpoint.WriteToDataSetExtensionPoint;
import org.pentaho.di.dataset.util.DataSetConst;
import org.pentaho.di.dataset.util.FactoriesHierarchy;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.shared.SharedObjectInterface;
import org.pentaho.di.shared.SharedObjects;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.ui.core.dialog.EnterMappingDialog;
import org.pentaho.di.ui.core.dialog.EnterSelectionDialog;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.dialog.SelectRowDialog;
import org.pentaho.di.ui.spoon.ISpoonMenuController;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.spoon.trans.TransGraph;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.stores.delegate.DelegatingMetaStore;
import org.pentaho.metastore.util.PentahoDefaults;
import org.pentaho.ui.xul.dom.Document;
import org.pentaho.ui.xul.impl.AbstractXulEventHandler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataSetHelper extends AbstractXulEventHandler implements ISpoonMenuController {
  protected static Class<?> PKG = DataSetHelper.class; // for i18n

  private static DataSetHelper instance = null;

  private Map<TransMeta, TransUnitTest> activeTests;

  private DataSetHelper() {
    activeTests = new HashMap<>();
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

  public static List<DatabaseMeta> getAvailableDatabases( Repository repository ) throws KettleException {
    List<DatabaseMeta> list = new ArrayList<DatabaseMeta>();

    // Load database connections from the central repository if we're connected to one
    //
    if ( repository != null ) {
      ObjectId[] databaseIDs = repository.getDatabaseIDs( false );
      for ( ObjectId databaseId : databaseIDs ) {
        list.add( repository.loadDatabaseMeta( databaseId, null ) );
      }
    }

    // We need to share a default VariableSpace (env vars) with these objects...
    //
    VariableSpace space = Variables.getADefaultVariableSpace();

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

          // To allow these connections to be parameterized
          //
          databaseMeta.initializeVariablesFrom( space );
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
      EnterSelectionDialog esd = new EnterSelectionDialog( spoon.getShell(), groupNames.toArray( new String[ groupNames.size() ] ), "Select the group", "Select the group to edit..." );
      String groupName = esd.open();
      if ( groupName != null ) {
        editDataSetGroup( groupName );
      }
    } catch ( Exception e ) {
      new ErrorDialog( spoon.getShell(), "Error", "Error retrieving the list of data set groups", e );
    }
  }

  public void editDataSetGroup( String groupName ) throws MetaStoreException, KettleException {

    Spoon spoon = Spoon.getInstance();
    DelegatingMetaStore metaStore = spoon.getMetaStore();
    List<DatabaseMeta> databases = getAvailableDatabases( spoon.getRepository() );
    MetaStoreFactory<DataSetGroup> groupFactory = new MetaStoreFactory<DataSetGroup>( DataSetGroup.class, metaStore, PentahoDefaults.NAMESPACE );

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
      EnterSelectionDialog esd = new EnterSelectionDialog( spoon.getShell(), groupNames.toArray( new String[ groupNames.size() ] ), "Select the group", "Select the group to edit..." );
      String groupName = esd.open();
      if ( groupName != null ) {

        // TODO: Find the unit tests for this group, if there are any, we can't remove the group
        //
        groupFactory.deleteElement( groupName );
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
    } else if ( StringUtil.isEmpty( previousName ) && Const.indexOfString( newName, groupNames ) >= 0 ) {
      message = BaseMessages.getString( PKG, "DataSetHelper.DataSetGroup.AGroupWithNameExists.Message", newName );
    } else if ( dataSetGroup.getType() == null ) {
      message = BaseMessages.getString( PKG, "DataSetHelper.DataSetGroup.NoGroupTypeSpecified.Message" );
    } else {
      switch ( dataSetGroup.getType() ) {
        case Database:
          if ( dataSetGroup.getDatabaseMeta() == null ) {
            message = BaseMessages.getString( PKG, "DataSetHelper.DataSetGroup.NoDatabaseSpecified.Message" );
          }
          break;
        case CSV:
          break;
      }
    }

    return message;
  }

  public static String validateDataSet( DataSet dataSet, String previousName, List<String> setNames ) {

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
      EnterSelectionDialog esd = new EnterSelectionDialog( spoon.getShell(), setNames.toArray( new String[ setNames.size() ] ), "Select the set", "Select the data set to edit..." );
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
      DataSetDialog setDialog = new DataSetDialog( spoon.getShell(), setFactory.getMetaStore(), dataSet, groups, getAvailableDatabases( spoon.getRepository() ) );
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
    } catch ( Exception e ) {
      new ErrorDialog( spoon.getShell(), "Error", "Unable to edit data set", e );
    }
  }


  public void deleteDataSet() {

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
      EnterSelectionDialog esd = new EnterSelectionDialog( spoon.getShell(), setNames.toArray( new String[ setNames.size() ] ), "Select the data set", "Select the data set to delete..." );
      String setName = esd.open();
      if ( setName != null ) {
        DataSet dataSet = setFactory.loadElement( setName );

        deleteDataSet( spoon, dataSet, groups, setFactory, setName );
      }
    } catch ( Exception e ) {
      new ErrorDialog( spoon.getShell(), "Error", "Error retrieving the list of data set groups", e );
    }
  }

  private void deleteDataSet( Spoon spoon, DataSet dataSet, List<DataSetGroup> groups, MetaStoreFactory<DataSet> setFactory, String setName ) throws MetaStoreException {

    MessageBox box = new MessageBox( Spoon.getInstance().getShell(), SWT.YES | SWT.NO );
    box.setText( BaseMessages.getString( PKG, "DataSetHelper.YouSureToDeleteDataSet.Title" ) );
    box.setMessage( BaseMessages.getString( PKG, "DataSetHelper.YouSureToDeleteDataSet.Message", setName ) );
    int answer = box.open();
    if ( ( answer & SWT.YES ) != 0 ) {
      try {
        FactoriesHierarchy hierarchy = getHierarchy();
        hierarchy.getSetFactory().deleteElement( setName );
      } catch ( Exception exception ) {
        new ErrorDialog( Spoon.getInstance().getShell(),
          BaseMessages.getString( PKG, "DataSetHelper.ErrorDeletingDataSet.Title" ),
          BaseMessages.getString( PKG, "DataSetHelper.ErrorDeletingDataSet.Message", setName ),
          exception );
      }
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

    if ( checkTestPresent( spoon, transMeta ) ) {
      return;
    }
    TransUnitTest unitTest = activeTests.get( transMeta );

    try {

      List<DatabaseMeta> databases = getAvailableDatabases( spoon.getRepository() );
      FactoriesHierarchy hierarchy = new FactoriesHierarchy( metaStore, databases );

      MetaStoreFactory<DataSet> setFactory = hierarchy.getSetFactory();
      List<String> setNames = setFactory.getElementNames();
      Collections.sort( setNames );
      EnterSelectionDialog esd = new EnterSelectionDialog( spoon.getShell(), setNames.toArray( new String[ setNames.size() ] ), "Select the set", "Select the data set to edit..." );
      String setName = esd.open();
      if ( setName != null ) {
        DataSet dataSet = setFactory.loadElement( setName );

        // Now we need to map the fields from the input data set to the step...
        //
        RowMetaInterface setFields = dataSet.getSetRowMeta( false );
        RowMetaInterface stepFields;
        try {
          stepFields = transMeta.getStepFields( stepMeta );
        } catch( KettleStepException e) {
          // Driver or input problems...
          //
          stepFields = new RowMeta();
        }
        if ( stepFields.isEmpty() ) {
          stepFields = setFields.clone();
        }

        String[] stepFieldNames = stepFields.getFieldNames();
        String[] setFieldNames = setFields.getFieldNames();

        EnterMappingDialog mappingDialog = new EnterMappingDialog( spoon.getShell(), setFieldNames, stepFieldNames );
        List<SourceToTargetMapping> mappings = mappingDialog.open();
        if ( mappings == null ) {
          return;
        }

        // Ask about the sort order...
        // Show the mapping as well as an order column
        //
        RowMetaInterface sortMeta = new RowMeta();
        sortMeta.addValueMeta( new ValueMetaString( BaseMessages.getString( PKG, "DataSetHelper.SortOrder.Column.SetField" ) ) );
        List<Object[]> sortData = new ArrayList<Object[]>();
        for ( String setFieldName : setFieldNames ) {
          sortData.add( new Object[] { setFieldName } );
        }
        EditRowsDialog orderDialog = new EditRowsDialog( spoon.getShell(), SWT.NONE,
          BaseMessages.getString( PKG, "DataSetHelper.SortOrder.Title" ),
          BaseMessages.getString( PKG, "DataSetHelper.SortOrder.Message" ),
          sortMeta, sortData
        );
        List<Object[]> orderMappings = orderDialog.open();
        if ( orderMappings == null ) {
          return;
        }

        // Modify the test
        //

        // Remove other crap on the step...
        //
        unitTest.removeInputAndGoldenDataSets( stepMeta.getName() );

        TransUnitTestSetLocation inputLocation = new TransUnitTestSetLocation();
        unitTest.getInputDataSets().add( inputLocation );

        inputLocation.setStepname( stepMeta.getName() );
        inputLocation.setDataSetName( dataSet.getName() );
        List<TransUnitTestFieldMapping> fieldMappings = inputLocation.getFieldMappings();
        fieldMappings.clear();

        for ( SourceToTargetMapping mapping : mappings ) {
          String stepFieldName = mapping.getTargetString( stepFieldNames );
          String setFieldName = mapping.getSourceString( setFieldNames );
          fieldMappings.add( new TransUnitTestFieldMapping( stepFieldName, setFieldName ) );
        }

        List<String> setFieldOrder = new ArrayList<String>();
        for ( Object[] orderMapping : orderMappings ) {
          String setFieldName = sortMeta.getString( orderMapping, 0 );
          setFieldOrder.add( setFieldName );
        }
        inputLocation.setFieldOrder( setFieldOrder );

        // Save the unit test...
        //
        saveUnitTest( getHierarchy().getTestFactory(), unitTest, transMeta );

        stepMeta.setChanged();

        spoon.refreshGraph();
      }
    } catch ( Exception e ) {
      new ErrorDialog( spoon.getShell(), "Error", "Error retrieving the list of data set groups", e );
    }
  }

  private boolean checkTestPresent( Spoon spoon, TransMeta transMeta ) {

    TransUnitTest activeTest = activeTests.get( transMeta );
    if ( activeTest != null ) {
      return false;
    }

    // there is no test defined of selected in the transformation.
    // Show a warning
    //
    MessageBox box = new MessageBox( spoon.getShell(), SWT.OK | SWT.ICON_INFORMATION );
    box.setMessage( "Please create a test-case first by left clicking on the test icon." );
    box.setText( "First create a test-case" );
    box.open();

    return true;
  }

  /**
   * We set an golden data set on the selected unit test
   */
  public void setGoldenDataSet() {
    Spoon spoon = ( (Spoon) SpoonFactory.getInstance() );
    TransGraph transGraph = spoon.getActiveTransGraph();
    TransMeta sourceTransMeta = spoon.getActiveTransformation();
    StepMeta stepMeta = transGraph.getCurrentStep();
    if ( transGraph == null || sourceTransMeta == null || stepMeta == null ) {
      return;
    }
    IMetaStore metaStore = spoon.getMetaStore();

    if ( checkTestPresent( spoon, sourceTransMeta ) ) {
      return;
    }
    TransUnitTest unitTest = activeTests.get( sourceTransMeta );

    try {
      FactoriesHierarchy hierarchy = getHierarchy();

      // Create a copy and modify the transformation
      // This way we have
      TransMetaModifier modifier = new TransMetaModifier( sourceTransMeta, unitTest );
      TransMeta transMeta = modifier.getTestTransformation( LogChannel.UI, sourceTransMeta, hierarchy );


      MetaStoreFactory<DataSet> setFactory = hierarchy.getSetFactory();
      List<String> setNames = setFactory.getElementNames();
      Collections.sort( setNames );
      EnterSelectionDialog esd = new EnterSelectionDialog( spoon.getShell(), setNames.toArray( new String[ setNames.size() ] ), "Select the golden data set", "Select the golden data set..." );
      String setName = esd.open();
      if ( setName != null ) {
        DataSet dataSet = setFactory.loadElement( setName );

        // Now we need to map the fields from the step to golden data set fields...
        //
        RowMetaInterface stepFields;
        try {
          stepFields = transMeta.getPrevStepFields( stepMeta );
        } catch(KettleStepException e) {
          // Ignore error: issues with not being able to get fields because of the unit test
          // running in a different environment.
          //
          stepFields = new RowMeta();
        }
        RowMetaInterface setFields = dataSet.getSetRowMeta( false );

        String[] stepFieldNames = stepFields.getFieldNames();
        String[] setFieldNames = setFields.getFieldNames();

        EnterMappingDialog mappingDialog = new EnterMappingDialog( spoon.getShell(), stepFieldNames, setFieldNames );
        List<SourceToTargetMapping> mappings = mappingDialog.open();
        if ( mappings == null ) {
          return;
        }

        // Ask about the sort order...
        // Show the mapping as well as an order column
        //
        RowMetaInterface sortMeta = new RowMeta();
        sortMeta.addValueMeta( new ValueMetaString( BaseMessages.getString( PKG, "DataSetHelper.SortOrder.Column.SetField" ) ) );
        List<Object[]> sortData = new ArrayList<Object[]>();
        for ( String setFieldName : setFieldNames ) {
          sortData.add( new Object[] { setFieldName } );
        }
        EditRowsDialog orderDialog = new EditRowsDialog( spoon.getShell(), SWT.NONE,
          BaseMessages.getString( PKG, "DataSetHelper.SortOrder.Title" ),
          BaseMessages.getString( PKG, "DataSetHelper.SortOrder.Message" ),
          sortMeta, sortData
        );
        List<Object[]> orderMappings = orderDialog.open();
        if ( orderMappings == null ) {
          return;
        }

        // Modify the test
        //

        // Remove golden locations and input locations on the step to avoid duplicates
        //
        unitTest.removeInputAndGoldenDataSets( stepMeta.getName() );

        TransUnitTestSetLocation goldenLocation = new TransUnitTestSetLocation();
        unitTest.getGoldenDataSets().add( goldenLocation );

        goldenLocation.setStepname( stepMeta.getName() );
        goldenLocation.setDataSetName( dataSet.getName() );
        List<TransUnitTestFieldMapping> fieldMappings = goldenLocation.getFieldMappings();
        fieldMappings.clear();

        for ( SourceToTargetMapping mapping : mappings ) {
          fieldMappings.add( new TransUnitTestFieldMapping(
            mapping.getSourceString( stepFieldNames ),
            mapping.getTargetString( setFieldNames ) ) );
        }

        List<String> setFieldOrder = new ArrayList<String>();
        for ( Object[] orderMapping : orderMappings ) {
          setFieldOrder.add( sortMeta.getString( orderMapping, 0 ) );
        }
        goldenLocation.setFieldOrder( setFieldOrder );

        // Save the unit test...
        //
        saveUnitTest( getHierarchy().getTestFactory(), unitTest, transMeta );

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
    if ( checkTestPresent( spoon, transMeta ) ) {
      return;
    }

    try {
      TransUnitTest currentUnitTest = getCurrentUnitTest( transMeta );

      TransUnitTestSetLocation inputLocation = currentUnitTest.findInputLocation( stepMeta.getName() );
      if ( inputLocation != null ) {
        currentUnitTest.getInputDataSets().remove( inputLocation );
      }

      saveUnitTest( getHierarchy().getTestFactory(), currentUnitTest, transMeta );

      transGraph.redraw();
    } catch ( Exception e ) {
      new ErrorDialog( spoon.getShell(), "Error", "Error saving unit test", e );
    }
  }

  public void clearGoldenDataSet() {
    Spoon spoon = ( (Spoon) SpoonFactory.getInstance() );
    TransGraph transGraph = spoon.getActiveTransGraph();
    TransMeta transMeta = spoon.getActiveTransformation();
    StepMeta stepMeta = transGraph.getCurrentStep();
    if ( transGraph == null || transMeta == null || stepMeta == null ) {
      return;
    }
    if ( checkTestPresent( spoon, transMeta ) ) {
      return;
    }

    try {
      TransUnitTest currentUnitTest = getCurrentUnitTest( transMeta );

      TransUnitTestSetLocation goldenLocation = currentUnitTest.findGoldenLocation( stepMeta.getName() );
      if ( goldenLocation != null ) {
        currentUnitTest.getGoldenDataSets().remove( goldenLocation );
      }

      saveUnitTest( getHierarchy().getTestFactory(), currentUnitTest, transMeta );
    } catch ( Exception e ) {
      new ErrorDialog( spoon.getShell(), "Error", "Error saving unit test", e );
    }
    transMeta.setChanged();
    transGraph.redraw();
  }

  public static FactoriesHierarchy getHierarchy() throws KettleException {

    try {
      Spoon spoon = Spoon.getInstance();

      List<DatabaseMeta> databases = getAvailableDatabases( spoon.getRepository() );
      FactoriesHierarchy hierarchy = new FactoriesHierarchy( spoon.getMetaStore(), databases );
      return hierarchy;
    } catch ( Exception e ) {
      throw new KettleException( "Unable to get MetaStore factories hierarchy", e );
    }
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
      FactoriesHierarchy hierarchy = getHierarchy();

      MetaStoreFactory<DataSetGroup> groupFactory = hierarchy.getGroupFactory();
      List<DatabaseMeta> databases = getAvailableDatabases( spoon.getRepository() );
      groupFactory.addNameList( DataSetConst.DATABASE_LIST_KEY, databases );
      List<DataSetGroup> groups = groupFactory.getElements();

      MetaStoreFactory<DataSet> setFactory = hierarchy.getSetFactory();
      setFactory.addNameList( DataSetConst.GROUP_LIST_KEY, groups );

      DataSet dataSet = new DataSet();
      RowMetaInterface rowMeta = transMeta.getStepFields( stepMeta );
      for ( int i = 0; i < rowMeta.size(); i++ ) {
        ValueMetaInterface valueMeta = rowMeta.getValueMeta( i );
        String setFieldname = valueMeta.getName();
        String columnName = "field" + i;
        DataSetField field = new DataSetField( setFieldname, columnName, valueMeta.getType(), valueMeta.getLength(), valueMeta.getPrecision(), valueMeta.getComments() );
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

      FactoriesHierarchy hierarchy = getHierarchy();

      MetaStoreFactory<DataSetGroup> groupFactory = hierarchy.getGroupFactory();
      List<DatabaseMeta> databases = getAvailableDatabases( spoon.getRepository() );
      groupFactory.addNameList( DataSetConst.DATABASE_LIST_KEY, databases );
      List<DataSetGroup> groups = groupFactory.getElements();

      MetaStoreFactory<DataSet> setFactory = hierarchy.getSetFactory();
      setFactory.addNameList( DataSetConst.GROUP_LIST_KEY, groups );

      // Ask which data set to write to
      //
      List<String> setNames = setFactory.getElementNames();
      Collections.sort( setNames );
      EnterSelectionDialog esd = new EnterSelectionDialog( spoon.getShell(), setNames.toArray( new String[ setNames.size() ] ), "Select the set", "Select the data set to edit..." );
      String setName = esd.open();
      if ( setName == null ) {
        return;
      }

      DataSet dataSet = setFactory.loadElement( setName );
      String[] setFields = new String[ dataSet.getFields().size() ];
      for ( int i = 0; i < setFields.length; i++ ) {
        setFields[ i ] = dataSet.getFields().get( i ).getFieldName();
      }

      RowMetaInterface rowMeta = transMeta.getStepFields( stepMeta );
      String[] stepFields = new String[ rowMeta.size() ];
      for ( int i = 0; i < rowMeta.size(); i++ ) {
        ValueMetaInterface valueMeta = rowMeta.getValueMeta( i );
        stepFields[ i ] = valueMeta.getName();
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
      WriteToDataSetExtensionPoint.stepsMap.put( transMeta.getName(), stepMeta );
      WriteToDataSetExtensionPoint.mappingsMap.put( transMeta.getName(), mapping );
      WriteToDataSetExtensionPoint.setsMap.put( transMeta.getName(), dataSet );

      // Signal to the transformation xp plugin to inject data into some data set
      //
      transMeta.setVariable( DataSetConst.VAR_WRITE_TO_DATASET, "Y" );
      spoon.runFile();

    } catch ( Exception e ) {
      new ErrorDialog( spoon.getShell(), "Error", "Error creating a new data set", e );
    }
  }


  public void createUnitTest() {
    Spoon spoon = ( (Spoon) SpoonFactory.getInstance() );
    TransGraph transGraph = spoon.getActiveTransGraph();
    if ( transGraph == null ) {
      return;
    }
    TransMeta transMeta = transGraph.getTransMeta();

    createUnitTest( spoon, transMeta );
  }

  public void createUnitTest( Spoon spoon, TransMeta transMeta ) {
    try {
      TransUnitTest unitTest = new TransUnitTest();
      unitTest.setName( transMeta.getName() + " Test" );
      unitTest.setTransFilename( transMeta.getFilename() );
      if ( spoon.getRepository() != null ) {
        unitTest.setTransObjectId( transMeta.getObjectId() == null ? null : transMeta.getObjectId().getId() );
        String path = transMeta.getRepositoryDirectory().getPath();
        if ( !path.endsWith( "/" ) ) {
          path += "/";
        }
        path += transMeta.getName();
        unitTest.setTransRepositoryPath( path );
      }

      FactoriesHierarchy hierarchy = getHierarchy();

      TransUnitTestDialog dialog = new TransUnitTestDialog( spoon.getShell(), transMeta, spoon.getMetaStore(), unitTest );
      if ( dialog.open() ) {
        saveUnitTest( hierarchy.getTestFactory(), unitTest, transMeta );

        activeTests.put( transMeta, unitTest );

        spoon.refreshGraph();
      }

    } catch ( Exception e ) {
      new ErrorDialog( spoon.getShell(), "Error", "Error creating a new transformation unit test", e );
    }
  }

  private void saveUnitTest( MetaStoreFactory<TransUnitTest> testFactory, TransUnitTest unitTest, TransMeta transMeta ) throws MetaStoreException {

    // Build relative path whenever a transformation is saved
    //
    if ( StringUtils.isNotEmpty( transMeta.getFilename() ) ) {
      // Set the filename to be safe
      //
      unitTest.setTransFilename( transMeta.getFilename() );

      String basePath = unitTest.getBasePath();
      if ( StringUtils.isEmpty( basePath ) ) {
        basePath = transMeta.getVariable( DataSetConst.VARIABLE_UNIT_TESTS_BASE_PATH );
      }
      basePath = transMeta.environmentSubstitute( basePath );
      if ( StringUtils.isNotEmpty( basePath ) ) {
        // See if the basePath is present in the filename
        // Then replace the filename
        //
        try {
          FileObject baseFolder = KettleVFS.getFileObject( basePath );
          FileObject transFile = KettleVFS.getFileObject( transMeta.getFilename() );
          FileObject parent = transFile.getParent();
          while ( parent != null ) {
            if ( parent.equals( baseFolder ) ) {
              // Here we are, we found the base folder in the transformation file
              //
              String transFilename = transFile.toString();
              String baseFoldername = parent.toString();

              // Final validation & unit test filename correction
              //
              if ( transFilename.startsWith( baseFoldername ) ) {
                String relativeFile = transFilename.substring( baseFoldername.length() );
                String filename;
                if ( relativeFile.startsWith( "/" ) ) {
                  filename = "." + relativeFile;
                } else {
                  filename = "./" + relativeFile;
                }
                // Set the transformation filename to the relative path
                //
                unitTest.setTransFilename( filename );
                LogChannel.GENERAL.logBasic( "Unit test '" + unitTest.getName() + "' : Saved relative path to transformation: " + filename );
              }
            }
            parent = parent.getParent();
          }
        } catch ( Exception e ) {
          throw new MetaStoreException( "Error calculating relative unit test file path", e );
        }
      }
    }

    testFactory.saveElement( unitTest );
  }

  public void editUnitTest( Spoon spoon, TransMeta transMeta, String unitTestName ) {

    IMetaStore metaStore = spoon.getMetaStore();
    try {
      FactoriesHierarchy hierarchy = getHierarchy();

      TransUnitTest unitTest = hierarchy.getTestFactory().loadElement( unitTestName );
      if ( unitTest == null ) {
        throw new KettleException( BaseMessages.getString( PKG, "DataSetHelper.ErrorEditingUnitTest.Message", unitTestName ) );
      }
      TransUnitTestDialog dialog = new TransUnitTestDialog( spoon.getShell(), transMeta, metaStore, unitTest );
      if ( dialog.open() ) {
        saveUnitTest( hierarchy.getTestFactory(), unitTest, transMeta );
      }
    } catch ( Exception exception ) {
      new ErrorDialog( Spoon.getInstance().getShell(),
        BaseMessages.getString( PKG, "DataSetHelper.ErrorEditingUnitTest.Title" ),
        BaseMessages.getString( PKG, "DataSetHelper.ErrorEditingUnitTest.Message", unitTestName ),
        exception );
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
      if ( transMeta == null ) {
        return;
      }


      // Remove
      //
      activeTests.remove( transMeta );
      transMeta.setVariable( DataSetConst.VAR_RUN_UNIT_TEST, "N" );

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

      FactoriesHierarchy hierarchy = getHierarchy();

      List<String> testNames = hierarchy.getTestFactory().getElementNames();
      String[] names = testNames.toArray( new String[ testNames.size() ] );
      Arrays.sort( names );
      EnterSelectionDialog esd = new EnterSelectionDialog( spoon.getShell(), names, "Select a unit test", "Select the unit test to use" );
      String testName = esd.open();
      if ( testName != null ) {

        TransUnitTest unitTest = hierarchy.getTestFactory().loadElement( testName );
        if ( unitTest == null ) {
          throw new KettleException( "Unit test '" + testName + "' could not be found (deleted)?" );
        }

        selectUnitTest( transMeta, unitTest );
        Spoon.getInstance().refreshGraph();
      }
    } catch ( Exception e ) {
      new ErrorDialog( spoon.getShell(), "Error", "Error selecting a new transformation unit test", e );
    }
  }

  public static final void selectUnitTest( TransMeta transMeta, TransUnitTest unitTest ) {
    getInstance().getActiveTests().put( transMeta, unitTest );
  }

  public static final TransUnitTest getCurrentUnitTest( TransMeta transMeta ) {
    return getInstance().getActiveTests().get( transMeta );
  }

  public void enableTweakRemoveStepInUnitTest() {
    tweakRemoveStepInUnitTest( true );
  }

  public void disableTweakRemoveStepInUnitTest() {
    tweakRemoveStepInUnitTest( false );
  }

  public void tweakRemoveStepInUnitTest( boolean enable ) {
    tweakUnitTestStep( TransTweak.REMOVE_STEP, enable );
  }

  private void tweakUnitTestStep( TransTweak stepTweak, boolean enable ) {
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
    if ( checkTestPresent( spoon, transMeta ) ) {
      return;
    }

    try {
      TransUnitTest unitTest = getCurrentUnitTest( transMeta );
      TransUnitTestTweak unitTestTweak = unitTest.findTweak( stepMeta.getName() );
      if ( unitTestTweak != null ) {
        unitTest.getTweaks().remove( unitTestTweak );
      }
      if ( enable ) {
        unitTest.getTweaks().add( new TransUnitTestTweak( stepTweak, stepMeta.getName() ) );
      }

      saveUnitTest( getHierarchy().getTestFactory(), unitTest, transMeta );

      spoon.refreshGraph();

    } catch ( Exception exception ) {
      new ErrorDialog( spoon.getShell(), "Error", "Error tweaking transformation unit test on step '" + stepMeta.getName() + "' with operation " + stepTweak.name(), exception );
    }
  }

  public void enableTweakBypassStepInUnitTest() {
    tweakBypassStepInUnitTest( true );
  }

  public void disableTweakBypassStepInUnitTest() {
    tweakBypassStepInUnitTest( false );
  }

  public void tweakBypassStepInUnitTest( boolean enable ) {
    tweakUnitTestStep( TransTweak.BYPASS_STEP, enable );
  }

  /**
   * List all unit tests which are defined
   * And allow the user to select one
   */
  public RowMetaAndData selectUnitTestFromAllTests() {
    Spoon spoon = Spoon.getInstance();

    RowMetaInterface rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaString( "Unit test" ) );
    rowMeta.addValueMeta( new ValueMetaString( "Description" ) );
    rowMeta.addValueMeta( new ValueMetaString( "Filename" ) );

    List<RowMetaAndData> rows = new ArrayList<>();

    try {
      FactoriesHierarchy hierarchy = getHierarchy();

      List<String> testNames = hierarchy.getTestFactory().getElementNames();
      for ( String testName : testNames ) {
        TransUnitTest unitTest = hierarchy.getTestFactory().loadElement( testName );
        Object[] row = RowDataUtil.allocateRowData( rowMeta.size() );
        row[ 0 ] = testName;
        row[ 1 ] = unitTest.getDescription();
        row[ 2 ] = unitTest.getTransFilename();

        rows.add( new RowMetaAndData( rowMeta, row ) );
      }

      // Now show a selection dialog...
      //
      SelectRowDialog dialog = new SelectRowDialog( spoon.getShell(), new Variables(), SWT.DIALOG_TRIM | SWT.MAX | SWT.RESIZE, rows );
      RowMetaAndData selection = dialog.open();
      if ( selection != null ) {
        return selection;
      }
      return null;
    } catch ( Exception e ) {
      new ErrorDialog( spoon.getShell(), "Error", "Error listing/deleting unit test(s)", e );
      return null;
    }
  }


  /**
   * List all unit tests which are defined
   * And allow the user to select one to delete
   */
  public void deleteUnitTest() {
    try {

      RowMetaAndData selection = selectUnitTestFromAllTests();
      if ( selection != null ) {
        String unitTestName = selection.getString( 0, null );

        if ( StringUtils.isNotEmpty( unitTestName ) ) {
          deleteUnitTest( unitTestName );
        }
      }
    } catch ( Exception e ) {
      new ErrorDialog( Spoon.getInstance().getShell(), "Error", "Error deleting unit test", e );
    }
  }

  public void deleteUnitTest( String unitTestName ) {
    MessageBox box = new MessageBox( Spoon.getInstance().getShell(), SWT.YES | SWT.NO );
    box.setText( BaseMessages.getString( PKG, "DataSetHelper.YouSureToDelete.Title" ) );
    box.setMessage( BaseMessages.getString( PKG, "DataSetHelper.YouSureToDelete.Message", unitTestName ) );
    int answer = box.open();
    if ( ( answer & SWT.YES ) != 0 ) {
      try {
        FactoriesHierarchy hierarchy = getHierarchy();
        hierarchy.getTestFactory().deleteElement( unitTestName );
        DataSetHelper.getInstance().detachUnitTest();
      } catch ( Exception exception ) {
        new ErrorDialog( Spoon.getInstance().getShell(),
          BaseMessages.getString( PKG, "DataSetHelper.ErrorDeletingUnitTest.Title" ),
          BaseMessages.getString( PKG, "DataSetHelper.ErrorDeletingUnitTest.Message", unitTestName ),
          exception );

      }
    }
  }

  public void openUnitTestTransformation() {
    try {
      Spoon spoon = Spoon.getInstance();
      FactoriesHierarchy hierarchy = getHierarchy();
      RowMetaAndData selection = selectUnitTestFromAllTests();
      if ( selection != null ) {
        String filename = selection.getString( 2, null );
        if ( StringUtils.isNotEmpty( filename ) ) {
          // Load the unit test...
          //
          String unitTestName = selection.getString( 0, null );
          TransUnitTest targetTest = hierarchy.getTestFactory().loadElement( unitTestName );

          if ( targetTest != null ) {

            String completeFilename = targetTest.calculateCompleteFilename( Variables.getADefaultVariableSpace() );
            spoon.openFile( completeFilename, false );

            TransMeta transMeta = spoon.getActiveTransformation();
            if (transMeta!=null) {
              switchUnitTest( targetTest, transMeta );
            }
          }
        } else {
          throw new KettleException( "No filename found: repositories not supported yet for this feature" );
        }
      }
    } catch ( Exception e ) {
      new ErrorDialog( Spoon.getInstance().getShell(), "Error", "Error opening unit test transformation", e );
    }
  }

  public void switchUnitTest( TransUnitTest targetTest, TransMeta transMeta ) {
    try {
      DataSetHelper.getInstance().detachUnitTest();
      DataSetHelper.selectUnitTest( transMeta, targetTest );
    } catch ( Exception exception ) {
      new ErrorDialog( Spoon.getInstance().getShell(),
        BaseMessages.getString( PKG, "ShowUnitTestMenuExtensionPoint.ErrorSwitchingUnitTest.Title" ),
        BaseMessages.getString( PKG, "ShowUnitTestMenuExtensionPoint.ErrorSwitchingUnitTest.Message", targetTest.getName() ),
        exception );
    }
    Spoon.getInstance().refreshGraph();
  }

  public static List<TransUnitTest> findTransformationUnitTest( TransMeta transMeta, IMetaStore metaStore ) {
    MetaStoreFactory<TransUnitTest> factory = new MetaStoreFactory<TransUnitTest>( TransUnitTest.class, metaStore, PentahoDefaults.NAMESPACE );
    List<TransUnitTest> tests = new ArrayList<TransUnitTest>();

    try {

      List<TransUnitTest> allTests = factory.getElements();
      for ( TransUnitTest test : allTests ) {
        // Match the filename
        //
        if ( StringUtils.isNotEmpty( transMeta.getFilename() ) ) {

          // What's the transformation absolute URI
          //
          FileObject transFile = KettleVFS.getFileObject( transMeta.getFilename() );
          String transUri = transFile.getName().getURI();

          // What's the filename referenced in the test?
          //
          FileObject testTransFile = KettleVFS.getFileObject( test.calculateCompleteFilename(transMeta) );
          if (testTransFile.exists()) {
            String testTransUri = testTransFile.getName().getURI();

            if ( transUri.equals( testTransUri ) ) {
              tests.add( test );
            }
          }
        } else {
          if ( transMeta.getRepository() != null ) {
            // No filename, check the object_id ...
            //
            if ( transMeta.getObjectId() != null && transMeta.getObjectId().getId().equals( test.getTransObjectId() ) ) {
              tests.add( test );
            } else {
              // Try the repository path..
              //
              // What is the repsository path?
              String repositoryPath = transMeta.getRepositoryDirectory().getPath() + "/" + transMeta.getName();
              if ( repositoryPath.equals( test.getTransRepositoryPath() ) ) {
                tests.add( test );
              }
            }
          }
        }
      }

    } catch ( Exception exception ) {
      new ErrorDialog( Spoon.getInstance().getShell(),
        BaseMessages.getString( PKG, "ShowUnitTestMenuExtensionPoint.ErrorFindingUnitTestsForTransformation.Title" ),
        BaseMessages.getString( PKG, "ShowUnitTestMenuExtensionPoint.ErrorFindingUnitTestsForTransformation.Message" ),
        exception );
    }
    return tests;
  }

  /**
   * Gets activeTests
   *
   * @return value of activeTests
   */
  public Map<TransMeta, TransUnitTest> getActiveTests() {
    return activeTests;
  }

  /**
   * @param activeTests The activeTests to set
   */
  public void setActiveTests( Map<TransMeta, TransUnitTest> activeTests ) {
    this.activeTests = activeTests;
  }
}

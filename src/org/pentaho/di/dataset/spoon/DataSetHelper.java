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
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.dataset.DataSet;
import org.pentaho.di.dataset.DataSetField;
import org.pentaho.di.dataset.DataSetGroup;
import org.pentaho.di.dataset.TransUnitTest;
import org.pentaho.di.dataset.spoon.dialog.DataSetDialog;
import org.pentaho.di.dataset.spoon.dialog.DataSetGroupDialog;
import org.pentaho.di.dataset.spoon.dialog.TransUnitTestDialog;
import org.pentaho.di.dataset.trans.InjectDataSetIntoTransExtensionPoint;
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

  private String validateDataSetGroup( DataSetGroup dataSetGroup, String previousName, List<String> groupNames ) {

    String message = null;

    String newName = dataSetGroup.getName();
    if ( Const.isEmpty( newName ) ) {
      message = BaseMessages.getString( PKG, "DataSetHelper.DataSetGroup.NoNameSpecified.Message" );
    } else if ( !Const.isEmpty( previousName ) && !previousName.equals( newName ) ) {
      message = BaseMessages.getString( PKG, "DataSetHelper.DataSetGroup.RenamingOfADataSetNotSupported.Message" );
    } else if ( dataSetGroup.getDatabaseMeta() == null ) {
      message = BaseMessages.getString( PKG, "DataSetHelper.DataSetGroup.NoDatabaseSpecified.Message" );
    } else {
      if ( Const.isEmpty( previousName ) && Const.indexOfString( newName, groupNames ) >= 0 ) {
        message = BaseMessages.getString( PKG, "DataSetHelper.DataSetGroup.AGroupWithNameExists.Message", newName );
      }
    }

    return message;
  }

  private String validateDataSet( DataSet dataSet, String previousName, List<String> setNames ) {

    String message = null;

    String newName = dataSet.getName();
    if ( Const.isEmpty( newName ) ) {
      message = BaseMessages.getString( PKG, "DataSetHelper.DataSet.NoNameSpecified.Message" );
    } else if ( !Const.isEmpty( previousName ) && !previousName.equals( newName ) ) {
      message = BaseMessages.getString( PKG, "DataSetHelper.DataSet.RenamingOfADataSetsNotSupported.Message" );
    } else if ( dataSet.getGroup() == null ) {
      message = BaseMessages.getString( PKG, "DataSetHelper.DataSet.NoGroupSpecified.Message" );
    } else {
      if ( Const.isEmpty( previousName ) && Const.indexOfString( newName, setNames ) >= 0 ) {
        message = BaseMessages.getString( PKG, "DataSetHelper.DataSet.ADataSetWithNameExists.Message", newName );
      }
    }

    return message;
  }

  private String validateTransUnitTest( TransUnitTest test, String previousName, List<String> testNames ) {

    String message = null;

    String newName = test.getName();
    if ( Const.isEmpty( newName ) ) {
      message = BaseMessages.getString( PKG, "DataSetHelper.TransUnitTest.NoNameSpecified.Message" );
    } else if ( !Const.isEmpty( previousName ) && !previousName.equals( newName ) ) {
      message = BaseMessages.getString( PKG, "DataSetHelper.TransUnitTest.RenamingOfATransUnitTestNotSupported.Message" );
    } /* else if ( Const.isEmpty( test.getStepname() ) ) {
      message = BaseMessages.getString( PKG, "DataSetHelper.TransUnitTest.NoStepNameSpecified.Message" );
    } else if ( test.getGoldenDataSet() == null ) {
      message = BaseMessages.getString( PKG, "DataSetHelper.TransUnitTest.NoGoldenDataSetSpecified.Message" );
    } */ else {
      if ( Const.isEmpty( previousName ) && Const.indexOfString( newName, testNames ) >= 0 ) {
        message = BaseMessages.getString( PKG, "DataSetHelper.TransUnitTest.ATransUnitTestSetWithNameExists.Message", newName );
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
    DataSetDialog setDialog = new DataSetDialog( spoon.getShell(), dataSet, groups );
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

  }

  public void setInputDataSet() {
    Spoon spoon = ( (Spoon) SpoonFactory.getInstance() );
    TransGraph transGraph = spoon.getActiveTransGraph();
    TransMeta transMeta = spoon.getActiveTransformation();
    StepMeta stepMeta = transGraph.getCurrentStep();
    if ( transGraph == null || transMeta == null || stepMeta == null ) {
      return;
    }
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
        stepMeta.setAttribute( DataSetConst.ATTR_GROUP_DATASET, DataSetConst.ATTR_STEP_DATASET_INPUT, dataSet.getName() );
        stepMeta.setChanged();
        spoon.refreshGraph();
      }
    } catch ( Exception e ) {
      new ErrorDialog( spoon.getShell(), "Error", "Error retrieving the list of data set groups", e );
    }
  }

  public void clearDataSet() {
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

  public void addUnitTest() {
    Spoon spoon = ( (Spoon) SpoonFactory.getInstance() );
    Repository repository = spoon.getRepository();
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

      TransUnitTest test = new TransUnitTest();

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

      TransUnitTestDialog dialog = new TransUnitTestDialog( spoon.getShell(), repository, metaStore, test, transMeta.getSharedObjects() );
      while ( dialog.open() ) {
        MetaStoreFactory<TransUnitTest> testFactory = dialog.getFactoriesHierarchy().getTestFactory();

        // Verify empty name, existing name...
        //
        String message = validateTransUnitTest( test, null, testFactory.getElementNames() );

        // Save the test case ...
        //
        if ( message == null ) {
          testFactory.saveElement( test );
          break;
        } else {
          MessageBox box = new MessageBox( spoon.getShell(), SWT.OK );
          box.setText( "Error" );
          box.setMessage( message );
          box.open();
        }
      }
    } catch ( Exception e ) {
      new ErrorDialog( spoon.getShell(), "Error", "Error creating a new transformation unit test", e );
    }

  }

  public void editUnitTest() {

    Spoon spoon = ( (Spoon) SpoonFactory.getInstance() );
    Repository repository = spoon.getRepository();

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

      FactoriesHierarchy fh = new FactoriesHierarchy( metaStore, DataSetConst.getAvailableDatabases( repository, transMeta.getSharedObjects() ) );
      List<String> testNames = fh.getTestFactory().getElementNames();
      Collections.sort( testNames );
      EnterSelectionDialog esd = new EnterSelectionDialog( spoon.getShell(), testNames.toArray( new String[testNames.size()] ), "Select the test", "Select the transformation unit test to edit..." );
      String testName = esd.open();
      if ( testName == null ) {
        return;
      }
      TransUnitTest test = fh.getTestFactory().loadElement( testName );

      TransUnitTestDialog dialog = new TransUnitTestDialog( spoon.getShell(), repository, metaStore, test, transMeta.getSharedObjects() );
      while ( dialog.open() ) {
        MetaStoreFactory<TransUnitTest> testFactory = dialog.getFactoriesHierarchy().getTestFactory();

        // Verify empty name, existing name...
        //
        String message = validateTransUnitTest( test, testName, testFactory.getElementNames() );

        // Save the test case ...
        //
        if ( message == null ) {
          testFactory.saveElement( test );
          break;
        } else {
          MessageBox box = new MessageBox( spoon.getShell(), SWT.OK );
          box.setText( "Error" );
          box.setMessage( message );
          box.open();
        }
      }
    } catch ( Exception e ) {
      new ErrorDialog( spoon.getShell(), "Error", "Error editing a transformation unit test", e );
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
      StepMeta stepMeta = transGraph.getCurrentStep();
      TransMeta transMeta = spoon.getActiveTransformation();
      if ( stepMeta == null || transMeta == null ) {
        return;
      }

      TransUnitTest test = new TransUnitTest();
      // TODO FIX : test.setStepname( stepMeta.getName() );

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

      TransUnitTestDialog dialog = new TransUnitTestDialog( spoon.getShell(), repository, metaStore, test, transMeta.getSharedObjects() );
      while ( dialog.open() ) {
        MetaStoreFactory<TransUnitTest> testFactory = dialog.getFactoriesHierarchy().getTestFactory();

        // Verify empty name, existing name...
        //
        String message = validateTransUnitTest( test, null, testFactory.getElementNames() );

        // Save the test case ...
        //
        if ( message == null ) {
          testFactory.saveElement( test );

          // Also leave a trace in the step metadata so that we know what to draw.
          //
          stepMeta.setAttribute( DataSetConst.ATTR_GROUP_DATASET, DataSetConst.ATTR_STEP_UNIT_TEST, test.getName() );
          stepMeta.setChanged();
          spoon.refreshGraph();
          break;
        } else {
          MessageBox box = new MessageBox( spoon.getShell(), SWT.OK );
          box.setText( "Error" );
          box.setMessage( message );
          box.open();
        }
      }
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
      StepMeta stepMeta = transGraph.getCurrentStep();
      TransMeta transMeta = spoon.getActiveTransformation();
      if ( stepMeta == null || transMeta == null ) {
        return;
      }

      stepMeta.setAttribute( DataSetConst.ATTR_GROUP_DATASET, DataSetConst.ATTR_STEP_UNIT_TEST, null );
      transGraph.redraw();
    } catch ( Exception e ) {
      new ErrorDialog( spoon.getShell(), "Error", "Error detaching a new transformation unit test", e );
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
      StepMeta stepMeta = transGraph.getCurrentStep();
      TransMeta transMeta = spoon.getActiveTransformation();
      if ( stepMeta == null || transMeta == null ) {
        return;
      }

      List<DatabaseMeta> databases = getAvailableDatabases( spoon.getRepository() );
      FactoriesHierarchy fh = new FactoriesHierarchy( metaStore, databases );
      List<String> testNames = fh.getTestFactory().getElementNames();
      String[] names = testNames.toArray( new String[testNames.size()] );
      Arrays.sort( names );
      EnterSelectionDialog esd = new EnterSelectionDialog( spoon.getShell(), names, "Select a unit test", "Select the unit test to use" );
      String testName = esd.open();
      if ( testName != null ) {
        stepMeta.setAttribute( DataSetConst.ATTR_GROUP_DATASET, DataSetConst.ATTR_STEP_UNIT_TEST, testName );
        stepMeta.setChanged();
        spoon.refreshGraph();
      }
    } catch ( Exception e ) {
      new ErrorDialog( spoon.getShell(), "Error", "Error selecting a new transformation unit test", e );
    }
  }
}

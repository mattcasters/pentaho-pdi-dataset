package org.pentaho.di.dataset.spoon.dialog;

import java.util.List;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.dataset.DataSet;
import org.pentaho.di.dataset.DataSetGroup;
import org.pentaho.di.dataset.TransUnitTest;
import org.pentaho.di.dataset.util.DataSetConst;
import org.pentaho.di.dataset.util.FactoriesHierarchy;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.di.shared.SharedObjects;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.gui.WindowProperty;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

public class TransUnitTestDialog extends Dialog {
  private static Class<?> PKG = TransUnitTestDialog.class; // for i18n purposes, needed by Translator2!!

  private TransUnitTest transUnitTest;

  private List<DataSet> sets;

  private Shell shell;

  private Text wName;
  private Text wDescription;
  
  protected Group ioGroup;
  protected Button wInputs;
  protected Button wGolden;
  
  private CCombo wStepname;
  private CCombo wDataSet;
  private TableView wFieldMapping;

  private Text wTransformation;

  private Button wOK;
  private Button wEditSet;
  private Button wCancel;

  private PropsUI props;

  private int middle;
  private int margin;

  private boolean ok;

  protected Repository repository;

  protected IMetaStore metaStore;

  private List<DataSetGroup> groups;

  private FactoriesHierarchy factoriesHierarchy;

  private SharedObjects sharedObjects;

  public TransUnitTestDialog( Shell parent, Repository repository, IMetaStore metaStore, TransUnitTest transUnitTest, SharedObjects sharedObjects ) throws KettleException, MetaStoreException {
    super( parent, SWT.NONE );
    this.repository = repository;
    this.metaStore = metaStore;
    this.transUnitTest = transUnitTest;
    this.setSharedObjects(sharedObjects);
    props = PropsUI.getInstance();
    ok = false;

    List<DatabaseMeta> databases = DataSetConst.getAvailableDatabases( repository, sharedObjects );
    factoriesHierarchy = new FactoriesHierarchy( metaStore, databases );
    sets = factoriesHierarchy.getSetFactory().getElements();
    groups = factoriesHierarchy.getGroupFactory().getElements();
  }

  public boolean open() {
    Shell parent = getParent();
    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    shell.setImage( GUIResource.getInstance().getImageTable() );

    middle = props.getMiddlePct();
    margin = Const.MARGIN;

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setText( BaseMessages.getString( PKG, "TransUnitTestDialog.Shell.Title" ) );
    shell.setLayout( formLayout );

    // The name of the group...
    //
    Label wlName = new Label( shell, SWT.RIGHT );
    props.setLook( wlName );
    wlName.setText( BaseMessages.getString( PKG, "TransUnitTestDialog.Name.Label" ) );
    FormData fdlName = new FormData();
    fdlName.top = new FormAttachment( 0, 0 );
    fdlName.left = new FormAttachment( 0, 0 );
    fdlName.right = new FormAttachment( middle, -margin );
    wlName.setLayoutData( fdlName );
    wName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wName );
    FormData fdName = new FormData();
    fdName.top = new FormAttachment( 0, 0 );
    fdName.left = new FormAttachment( middle, 0 );
    fdName.right = new FormAttachment( 100, 0 );
    wName.setLayoutData( fdName );
    Control lastControl = wName;

    // The description of the group...
    //
    Label wlDescription = new Label( shell, SWT.RIGHT );
    props.setLook( wlDescription );
    wlDescription.setText( BaseMessages.getString( PKG, "TransUnitTestDialog.Description.Label" ) );
    FormData fdlDescription = new FormData();
    fdlDescription.top = new FormAttachment( lastControl, margin );
    fdlDescription.left = new FormAttachment( 0, 0 );
    fdlDescription.right = new FormAttachment( middle, -margin );
    wlDescription.setLayoutData( fdlDescription );
    wDescription = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wDescription );
    FormData fdDescription = new FormData();
    fdDescription.top = new FormAttachment( lastControl, margin );
    fdDescription.left = new FormAttachment( middle, 0 );
    fdDescription.right = new FormAttachment( 100, 0 );
    wDescription.setLayoutData( fdDescription );
    lastControl = wDescription;

    // The transformation informational...
    //
    Label wlTransformation = new Label( shell, SWT.RIGHT );
    props.setLook( wlTransformation );
    wlTransformation.setText( BaseMessages.getString( PKG, "TransUnitTestDialog.Transformation.Label" ) );
    FormData fdlTransformation = new FormData();
    fdlTransformation.top = new FormAttachment( lastControl, margin );
    fdlTransformation.left = new FormAttachment( 0, 0 );
    fdlTransformation.right = new FormAttachment( middle, -margin );
    wlTransformation.setLayoutData( fdlTransformation );
    wTransformation = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wTransformation.setEditable( false ); // READ ONLY!
    props.setLook( wTransformation );
    FormData fdTransformation = new FormData();
    fdTransformation.top = new FormAttachment( lastControl, margin );
    fdTransformation.left = new FormAttachment( middle, 0 );
    fdTransformation.right = new FormAttachment( 100, 0 );
    wTransformation.setLayoutData( fdTransformation );
    lastControl = wTransformation;

    // The step name to test with ...
    //
    Label wlStepName = new Label( shell, SWT.RIGHT );
    props.setLook( wlStepName );
    wlStepName.setText( BaseMessages.getString( PKG, "TransUnitTestDialog.StepName.Label" ) );
    FormData fdlTableName = new FormData();
    fdlTableName.top = new FormAttachment( lastControl, margin );
    fdlTableName.left = new FormAttachment( 0, 0 );
    fdlTableName.right = new FormAttachment( middle, -margin );
    wlStepName.setLayoutData( fdlTableName );
    wStepname = new CCombo( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wStepname );
    FormData fdStepName = new FormData();
    fdStepName.top = new FormAttachment( lastControl, margin );
    fdStepName.left = new FormAttachment( middle, 0 );
    fdStepName.right = new FormAttachment( 100, 0 );
    wStepname.setLayoutData( fdStepName );
    lastControl = wStepname;

    // The data set...
    //
    Label wlDataSet = new Label( shell, SWT.RIGHT );
    props.setLook( wlDataSet );
    wlDataSet.setText( BaseMessages.getString( PKG, "TransUnitTestDialog.DataSet.Label" ) );
    FormData fdlDatabase = new FormData();
    fdlDatabase.top = new FormAttachment( lastControl, margin );
    fdlDatabase.left = new FormAttachment( 0, 0 );
    fdlDatabase.right = new FormAttachment( middle, -margin );
    wlDataSet.setLayoutData( fdlDatabase );
    wDataSet = new CCombo( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wDataSet );
    FormData fdDataSet = new FormData();
    fdDataSet.top = new FormAttachment( lastControl, margin );
    fdDataSet.left = new FormAttachment( middle, 0 );
    fdDataSet.right = new FormAttachment( 100, 0 );
    wDataSet.setLayoutData( fdDataSet );
    lastControl = wDataSet;

    // The field mapping from the input to the data set...
    //
    Label wlFieldMapping = new Label( shell, SWT.NONE );
    wlFieldMapping.setText( BaseMessages.getString( PKG, "TransUnitTestDialog.FieldMapping.Label" ) );
    props.setLook( wlFieldMapping );
    FormData fdlUpIns = new FormData();
    fdlUpIns.left = new FormAttachment( 0, 0 );
    fdlUpIns.top = new FormAttachment( lastControl, margin );
    wlFieldMapping.setLayoutData( fdlUpIns );
    lastControl = wlFieldMapping;

    // Buttons at the bottom...
    //
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );

    wEditSet = new Button( shell, SWT.PUSH );
    wEditSet.setText( BaseMessages.getString( PKG, "TransUnitTestDialog.EditSet.Button" ) );

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    Button[] buttons = new Button[] { wOK, wEditSet, wCancel };
    BaseStepDialog.positionBottomButtons( shell, buttons, margin, null );

    // the field mapping grid in between
    //
    ColumnInfo[] columns = new ColumnInfo[] {
      new ColumnInfo( BaseMessages.getString( PKG, "TransUnitTestDialog.ColumnInfo.StepFieldName" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false ),
      new ColumnInfo( BaseMessages.getString( PKG, "TransUnitTestDialog.ColumnInfo.SetFieldName" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false ),
      new ColumnInfo( BaseMessages.getString( PKG, "TransUnitTestDialog.ColumnInfo.SortOrder" ),
        ColumnInfo.COLUMN_TYPE_TEXT, true, false ),
    };

    wFieldMapping = new TableView(
      new Variables(),
      shell,
      SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
      columns,
      5, // transUnitTest.getFieldMappings().size(),
      null, props );

    FormData fdFieldMapping = new FormData();
    fdFieldMapping.left = new FormAttachment( 0, 0 );
    fdFieldMapping.top = new FormAttachment( lastControl, margin );
    fdFieldMapping.right = new FormAttachment( 100, 0 );
    fdFieldMapping.bottom = new FormAttachment( wOK, -2 * margin );
    wFieldMapping.setLayoutData( fdFieldMapping );

    // Add listeners
    wOK.addListener( SWT.Selection, new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    } );
    wEditSet.addListener( SWT.Selection, new Listener() {
      public void handleEvent( Event e ) {
        editDataSet();
      }
    } );
    wCancel.addListener( SWT.Selection, new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    } );

    SelectionAdapter selAdapter = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };
    wName.addSelectionListener( selAdapter );
    wDescription.addSelectionListener( selAdapter );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();

    BaseStepDialog.setSize( shell );

    shell.open();
    Display display = parent.getDisplay();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return ok;
  }

  private DataSet verifySettings() throws KettleException {
    DataSet set = DataSetConst.findDataSet( sets, wDataSet.getText() );
    verifySettings( set );
    return set;
  }

  private void verifySettings( DataSet set ) throws KettleException {

    if ( set == null ) {
      throw new KettleException( BaseMessages.getString( PKG, "TransUnitTestDialog.Error.NoDataSetSpecified" ) );
    }
    if ( Const.isEmpty( wStepname.getText() ) ) {
      throw new KettleException( BaseMessages.getString( PKG, "TransUnitTestDialog.Error.NoStepNameSpecified" ) );
    }
  }

  protected void editDataSet() {

    try {

      DataSet dataSet = verifySettings();

      DataSetDialog dataSetDialog = new DataSetDialog( shell, dataSet, groups );
      if ( dataSetDialog.open() ) {
        sets = factoriesHierarchy.getSetFactory().getElements();
        groups = factoriesHierarchy.getGroupFactory().getElements();
        updateSetsCombo();
      }

    } catch ( Exception e ) {
      new ErrorDialog( shell, "Error", "Error editing data set", e );
    }
  }

  private void updateSetsCombo() {
    wDataSet.removeAll();
    for ( DataSet set : sets ) {
      wDataSet.add( set.getName() );
    }
  }

  public void dispose() {
    props.setScreen( new WindowProperty( shell ) );
    shell.dispose();
  }

  public void getData() {

    updateSetsCombo();

    wName.setText( Const.NVL( transUnitTest.getName(), "" ) );
    wDescription.setText( Const.NVL( transUnitTest.getDescription(), "" ) );
    /*
    wDataSet.setText( Const.NVL( transUnitTest.getGoldenDataSet() == null ? null : transUnitTest.getGoldenDataSet().getName(), "" ) );
    wStepname.setText( Const.NVL( transUnitTest.getStepname(), "" ) );
    for ( int i = 0; i < transUnitTest.getFieldMappings().size(); i++ ) {
      TransUnitTestFieldMapping field = transUnitTest.getFieldMappings().get( i );
      int colnr = 1;
      wFieldMapping.setText( Const.NVL( field.getStepFieldName(), "" ), colnr++, i );
      wFieldMapping.setText( Const.NVL( field.getDataSetFieldName(), "" ), colnr++, i );
      wFieldMapping.setText( Const.NVL( field.getSortOrder(), "" ), colnr++, i );
    }
    */
    try {

      if ( repository != null ) {
        if ( transUnitTest.getTransObjectId() != null ) {
          // Try to get the name from the repository
          //
          TransMeta transMeta = repository.loadTransformation( new StringObjectId( transUnitTest.getTransObjectId() ), null );
          wTransformation.setText( transMeta.getPathAndName() );
        } else {
          wTransformation.setText( Const.NVL( transUnitTest.getTransRepositoryPath(), "" ) );
        }
      } else {
        wTransformation.setText( Const.NVL( transUnitTest.getTransFilename(), "" ) );
      }

    } catch ( Exception e ) {
      new ErrorDialog( shell, "Error", "Error obtaining transformation information", e );
    }

    wName.setFocus();
  }

  private void cancel() {
    ok = false;
    dispose();
  }

  /**
   * @param set The trans unit test to load the dialog information into
   */
  public void getInfo( TransUnitTest test ) {

    test.setName( wName.getText() );
    test.setDescription( wDescription.getText() );
    /*
    test.setGoldenDataSet( DataSetConst.findDataSet( sets, wDataSet.getText() ) );
    test.setStepname( wStepname.getText() );
    test.getFieldMappings().clear();
    int nrFields = wFieldMapping.nrNonEmpty();
    for ( int i = 0; i < nrFields; i++ ) {
      TableItem item = wFieldMapping.getNonEmpty( i );
      int colnr = 1;
      String stepFieldName = item.getText( colnr++ );
      String dataSetFieldName = item.getText( colnr++ );
      String sortOrder = item.getText( colnr++ );

      TransUnitTestFieldMapping mapping = new TransUnitTestFieldMapping( stepFieldName, dataSetFieldName, sortOrder );
      test.getFieldMappings().add( mapping );
    }
    */

  }

  public void ok() {

    try {
      verifySettings();
    } catch ( Exception e ) {
      new ErrorDialog( shell, "Error", BaseMessages.getString( PKG, "TransUnitTestDialog.Error.ValidationError" ), e );
    }

    getInfo( transUnitTest );

    ok = true;
    dispose();

  }

  public FactoriesHierarchy getFactoriesHierarchy() {
    return factoriesHierarchy;
  }

  public void setFactoriesHierarchy( FactoriesHierarchy factoriesHierarchy ) {
    this.factoriesHierarchy = factoriesHierarchy;
  }

public SharedObjects getSharedObjects() {
	return sharedObjects;
}

public void setSharedObjects(SharedObjects sharedObjects) {
	this.sharedObjects = sharedObjects;
}

}

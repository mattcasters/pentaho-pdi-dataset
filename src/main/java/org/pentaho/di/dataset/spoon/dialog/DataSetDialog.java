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

package org.pentaho.di.dataset.spoon.dialog;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
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
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.DBCache;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LoggingObject;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.dataset.DataSet;
import org.pentaho.di.dataset.DataSetField;
import org.pentaho.di.dataset.DataSetGroup;
import org.pentaho.di.dataset.spoon.DataSetHelper;
import org.pentaho.di.dataset.util.DataSetConst;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.database.dialog.GetTableSizeProgressDialog;
import org.pentaho.di.ui.core.database.dialog.SQLEditor;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.dialog.PreviewRowsDialog;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.gui.WindowProperty;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.di.ui.trans.step.TableItemInsertListener;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.util.PentahoDefaults;

public class DataSetDialog extends Dialog {
  private static Class<?> PKG = DataSetDialog.class; // for i18n purposes, needed by Translator2!!

  private DataSet dataSet;

  private List<DataSetGroup> groups;

  private Shell shell;

  private Text wName;
  private Text wDescription;
  private Text wTableName;
  private CCombo wDataSetGroup;
  private TableView wFieldMapping;

  private Button wOK;
  private Button wGetMetadata;
  private Button wCreateTable;
  private Button wEditData;
  private Button wViewData;
  private Button wCancel;
  
  private Button wNewGroup;

  private PropsUI props;

  private int middle;
  private int margin;

  private boolean ok;

  private IMetaStore metaStore;

  private Button wEditGroup;

  private List<DatabaseMeta> databases;

  public DataSetDialog( Shell parent, IMetaStore metaStore, DataSet dataSet, List<DataSetGroup> groups, List<DatabaseMeta> databases ) {
    super( parent, SWT.NONE );
    this.metaStore = metaStore;
    this.dataSet = dataSet;
    this.groups = groups;
    this.databases = databases;
    props = PropsUI.getInstance();
    ok = false;
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

    shell.setText( BaseMessages.getString( PKG, "DataSetDialog.Shell.Title" ) );
    shell.setLayout( formLayout );

    // The name of the group...
    //
    Label wlName = new Label( shell, SWT.RIGHT );
    props.setLook( wlName );
    wlName.setText( BaseMessages.getString( PKG, "DataSetDialog.Name.Label" ) );
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
    wlDescription.setText( BaseMessages.getString( PKG, "DataSetDialog.Description.Label" ) );
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

    // The table storing the set...
    //
    Label wlTableName = new Label( shell, SWT.RIGHT );
    props.setLook( wlTableName );
    wlTableName.setText( BaseMessages.getString( PKG, "DataSetDialog.TableName.Label" ) );
    FormData fdlTableName = new FormData();
    fdlTableName.top = new FormAttachment( lastControl, margin );
    fdlTableName.left = new FormAttachment( 0, 0 );
    fdlTableName.right = new FormAttachment( middle, -margin );
    wlTableName.setLayoutData( fdlTableName );
    wTableName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTableName );
    FormData fdTableName = new FormData();
    fdTableName.top = new FormAttachment( lastControl, margin );
    fdTableName.left = new FormAttachment( middle, 0 );
    fdTableName.right = new FormAttachment( 100, 0 );
    wTableName.setLayoutData( fdTableName );
    lastControl = wTableName;

    // The data set group...
    //
    Label wlGroup = new Label( shell, SWT.RIGHT );
    props.setLook( wlGroup );
    wlGroup.setText( BaseMessages.getString( PKG, "DataSetDialog.Group.Label" ) );
    FormData fdlDatabase = new FormData();
    fdlDatabase.top = new FormAttachment( lastControl, margin );
    fdlDatabase.left = new FormAttachment( 0, 0 );
    fdlDatabase.right = new FormAttachment( middle, -margin );
    wlGroup.setLayoutData( fdlDatabase );

    wNewGroup = new Button( shell, SWT.PUSH);
    props.setLook( wNewGroup );
    wNewGroup.setText( BaseMessages.getString( PKG, "DataSetDialog.NewGroup.Label" ) );
    FormData fdlNewGroup = new FormData();
    fdlNewGroup.top = new FormAttachment( lastControl, margin );
    fdlNewGroup.right = new FormAttachment( 100, -margin );
    wNewGroup.setLayoutData( fdlNewGroup );
    wNewGroup.addSelectionListener(new SelectionAdapter() {
      @Override
      public void widgetSelected(SelectionEvent arg0) {
        addDataSetGroup();
      }
    });
    
    wEditGroup = new Button( shell, SWT.PUSH);
    props.setLook( wEditGroup );
    wEditGroup.setText( BaseMessages.getString( PKG, "DataSetDialog.EditGroup.Label" ) );
    FormData fdlEditGroup = new FormData();
    fdlEditGroup.top = new FormAttachment( lastControl, margin );
    fdlEditGroup.right = new FormAttachment( wNewGroup, -margin );
    wEditGroup.setLayoutData( fdlEditGroup );
    wEditGroup.addSelectionListener(new SelectionAdapter() {
      @Override
      public void widgetSelected(SelectionEvent arg0) {
        editDataSetGroup();
      }
    });

    wDataSetGroup = new CCombo( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wDataSetGroup );
    FormData fdDatabase = new FormData();
    fdDatabase.top = new FormAttachment( lastControl, margin );
    fdDatabase.left = new FormAttachment( middle, 0 );
    fdDatabase.right = new FormAttachment( wEditGroup, -margin );
    wDataSetGroup.setLayoutData( fdDatabase );
    lastControl = wDataSetGroup;

    // The field mapping from the input to the data set...
    //
    Label wlFieldMapping = new Label( shell, SWT.NONE );
    wlFieldMapping.setText( BaseMessages.getString( PKG, "DataSetDialog.FieldMapping.Label" ) );
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

    wGetMetadata = new Button( shell, SWT.PUSH );
    wGetMetadata.setText( BaseMessages.getString( PKG, "DataSetDialog.GetMetadata.Button" ) );

    wCreateTable = new Button( shell, SWT.PUSH );
    wCreateTable.setText( BaseMessages.getString( PKG, "DataSetDialog.CreateTable.Button" ) );

    wEditData = new Button( shell, SWT.PUSH );
    wEditData.setText( BaseMessages.getString( PKG, "DataSetDialog.EditData.Button" ) );

    wViewData = new Button( shell, SWT.PUSH );
    wViewData.setText( BaseMessages.getString( PKG, "DataSetDialog.ViewData.Button" ) );

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    Button[] buttons = new Button[] { wOK, wGetMetadata, wCreateTable, wEditData, wViewData, wCancel };
    BaseStepDialog.positionBottomButtons( shell, buttons, margin, null );

    // the field mapping grid in between
    //
    ColumnInfo[] columns = new ColumnInfo[] {
      new ColumnInfo( BaseMessages.getString( PKG, "DataSetDialog.ColumnInfo.FieldName" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false ),
      new ColumnInfo( BaseMessages.getString( PKG, "DataSetDialog.ColumnInfo.ColumnName" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false ),
      new ColumnInfo( BaseMessages.getString( PKG, "DataSetDialog.ColumnInfo.FieldType" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, ValueMetaFactory.getAllValueMetaNames(), false ),
      new ColumnInfo( BaseMessages.getString( PKG, "DataSetDialog.ColumnInfo.FieldLength" ),
        ColumnInfo.COLUMN_TYPE_TEXT, true, false ),
      new ColumnInfo( BaseMessages.getString( PKG, "DataSetDialog.ColumnInfo.FieldPrecision" ),
        ColumnInfo.COLUMN_TYPE_TEXT, true, false ),
      new ColumnInfo( BaseMessages.getString( PKG, "DataSetDialog.ColumnInfo.Comment" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
    };

    wFieldMapping = new TableView(
      new Variables(),
      shell,
      SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
      columns,
      dataSet.getFields().size(),
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
    wGetMetadata.addListener( SWT.Selection, new Listener() {
      public void handleEvent( Event e ) {
        getMetadataFromTable();
      }
    } );
    wCreateTable.addListener( SWT.Selection, new Listener() {
      public void handleEvent( Event e ) {
        createTable();
      }
    } );
    wEditData.addListener( SWT.Selection, new Listener() {
      public void handleEvent( Event e ) {
        editData();
      }
    } );
    wViewData.addListener( SWT.Selection, new Listener() {
      public void handleEvent( Event e ) {
        viewData();
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
    wTableName.addSelectionListener( selAdapter );

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

  protected void addDataSetGroup() {
    
    try {
      DataSetHelper.getInstance().addDataSetGroup();
      refreshGroups(metaStore);
    } catch(Exception e) {
      new ErrorDialog(shell, "Error", "Error creating new data set group", e);
    }
  }
  
  protected void editDataSetGroup() {
    
    try {
      DataSetHelper.getInstance().editDataSetGroup();
      refreshGroups(metaStore);   
    } catch(Exception e) {
      new ErrorDialog(shell, "Error", "Error editing data set group", e);
    }
  }

  private void refreshGroups(IMetaStore metaStore) throws MetaStoreException {
    
    MetaStoreFactory<DataSetGroup> factory = new MetaStoreFactory<DataSetGroup>(DataSetGroup.class, metaStore, PentahoDefaults.NAMESPACE);
    factory.addNameList( DataSetConst.DATABASE_LIST_KEY, databases );
    
    groups = factory.getElements();
    
    List<String> names = factory.getElementNames();
    Collections.sort(names);
    wDataSetGroup.setItems(names.toArray(new String[names.size()]));      

  }
  
  /**
   * Look at the metadata specified here and create the table accordingly...
   */
  protected void getMetadataFromTable() {
    try {
      DataSetGroup group = DataSetConst.findDataSetGroup( groups, wDataSetGroup.getText() );
      verifySettings( group );
      String schemaTable = group.getDatabaseMeta().getQuotedSchemaTableCombination( group.getSchemaName(), wTableName.getText() );

      Database database = null;
      try {
        database = new Database( new LoggingObject( "DataSetDialog" ), group.getDatabaseMeta() );
        database.connect();
        if ( database.checkTableExists( schemaTable ) ) {
          RowMetaInterface rowMeta = database.getTableFields( schemaTable );

          BaseStepDialog.getFieldsFromPrevious( rowMeta, wFieldMapping, 2, new int[] { 1, 2 }, new int[] { 3 }, 4, 5, new TableItemInsertListener() {
            public boolean tableItemInserted( TableItem tableItem, ValueMetaInterface v ) {

              tableItem.setText( 4, v.getLength() < 0 ? "" : Integer.toString( v.getLength() ) );
              tableItem.setText( 5, v.getPrecision() < 0 ? "" : Integer.toString( v.getPrecision() ) );

              return true;
            }
          } );

        } else {
          throw new KettleException( BaseMessages.getString( PKG, "DataSetDialog.Error.TableDoesNotExist", schemaTable ) );
        }

      } finally {
        if ( database != null ) {
          database.disconnect();
        }
      }

    } catch ( Exception e ) {
      new ErrorDialog( shell, "Error", "Error retrieving metadata from dataset table", e );
    }
  }

  private DataSetGroup verifySettings() throws KettleException {
    DataSetGroup group = DataSetConst.findDataSetGroup( groups, wDataSetGroup.getText() );
    verifySettings( group );
    return group;
  }

  private void verifySettings( DataSetGroup group ) throws KettleException {

    if ( group == null ) {
      throw new KettleException( BaseMessages.getString( PKG, "DataSetDialog.Error.NoGroupSpecified" ) );
    }
    if ( StringUtil.isEmpty( wTableName.getText() ) ) {
      throw new KettleException( BaseMessages.getString( PKG, "DataSetDialog.Error.NoTableSpecified" ) );
    }
    DatabaseMeta databaseMeta = group.getDatabaseMeta();
    if ( databaseMeta == null ) {
      throw new KettleException( BaseMessages.getString( PKG, "DataSetDialog.Error.GroupHasNoDatabaseSpecified" ) );
    }
  }

  /**
   * Look at the metadata specified here and create the table accordingly...
   */
  protected void createTable() {
    try {

      DataSetGroup group = verifySettings();
      DatabaseMeta databaseMeta = group.getDatabaseMeta();
      String schemaTable = databaseMeta.getQuotedSchemaTableCombination( group.getSchemaName(), wTableName.getText() );

      RowMetaInterface rowMeta = new RowMeta();
      int nrFields = wFieldMapping.nrNonEmpty();
      for ( int i = 0; i < nrFields; i++ ) {
        TableItem item = wFieldMapping.getNonEmpty( i );
        int colnr = 2;
        String columnName = item.getText( colnr++ );
        if ( StringUtil.isEmpty( columnName ) ) {
          throw new KettleException( BaseMessages.getString( PKG, "DataSetDialog.Error.NoColumnName", Integer.toString( i ) ) );
        }
        int fieldType = ValueMetaFactory.getIdForValueMeta( item.getText( colnr++ ) );
        if ( fieldType == ValueMetaInterface.TYPE_NONE ) {
          throw new KettleException( BaseMessages.getString( PKG, "DataSetDialog.Error.NoDataType", Integer.toString( i ) ) );
        }
        int length = Const.toInt( item.getText( colnr++ ), -1 );
        int precision = Const.toInt( item.getText( colnr++ ), -1 );

        ValueMetaInterface valueMeta = ValueMetaFactory.createValueMeta( columnName, fieldType, length, precision );
        rowMeta.addValueMeta( valueMeta );
      }

      Database database = null;
      try {
        database = new Database( new LoggingObject( "DataSetDialog" ), databaseMeta );
        database.connect();
        String sql;
        if ( database.checkTableExists( schemaTable ) ) {
          sql = database.getAlterTableStatement( schemaTable, rowMeta, null, false, null, true );
        } else {
          sql = database.getCreateTableStatement( schemaTable, rowMeta, null, false, null, true );
        }
        if ( StringUtil.isEmpty( sql ) ) {
          sql = "-- " + Const.CR + "-- " + BaseMessages.getString( PKG, "DataSetDialog.TableOKNoSQLNeeded" ) + Const.CR + "-- " + Const.CR + Const.CR;
        }
        SQLEditor sqlEditor = new SQLEditor( shell, SWT.NONE, databaseMeta, DBCache.getInstance(), sql );
        sqlEditor.open();

      } finally {
        if ( database != null ) {
          database.disconnect();
        }
      }

    } catch ( Exception e ) {
      new ErrorDialog( shell, "Error", "Error retrieving metadata from dataset table", e );
    }

  }

  protected void editData() {

    // If the row count is too high, we don't want to load it into memory...
    // Too high simply means: above the preview size...
    //
    int previewSize = props.getDefaultPreviewSize();
    try {

      verifySettings();

      DataSet set = new DataSet();
      getInfo( set );
      DataSetGroup group = set.getGroup();
      DatabaseMeta databaseMeta = group.getDatabaseMeta();
      String schemaTable = databaseMeta.getQuotedSchemaTableCombination( group.getSchemaName(), set.getTableName() );

      // First count the rows to see if we're not dealing with a very large data set...
      //
      GetTableSizeProgressDialog pd = new GetTableSizeProgressDialog( shell, databaseMeta,
        set.getTableName(), group.getSchemaName() );
      Long rowCount = pd.open();
      if ( rowCount == null ) {
        return;
      }

      if ( rowCount > previewSize ) {
        MessageBox box = new MessageBox( shell, SWT.YES | SWT.NO | SWT.ICON_QUESTION );
        box.setText( BaseMessages.getString( PKG, "DataSetDialog.RowCountWarning.Title" ) );
        box.setText( BaseMessages.getString( PKG, "DataSetDialog.RowCountWarning.Message", rowCount.toString() ) );
        int anwser = box.open();
        if ( ( anwser & SWT.NO ) != 0 ) {
          return;
        }
      }

      Database database = null;
      List<Object[]> rows = null;
      RowMetaInterface fieldsRowMeta = set.getSetRowMeta( false );
      RowMetaInterface columnsRowMeta = set.getSetRowMeta( true );

      try {
        database = new Database( new LoggingObject( "DataSetDialog" ), databaseMeta );
        database.connect();

        rows = database.getRows( "SELECT * FROM " + schemaTable, 0 );

      } finally {
        if ( database != null ) {
          database.disconnect();
        }
      }

      EditRowsDialog editRowsDialog = new EditRowsDialog( shell, SWT.NONE,
        BaseMessages.getString( PKG, "DataSetDialog.EditRows.Title" ),
        BaseMessages.getString( PKG, "DataSetDialog.EditRows.Message", set.getName() ),
        fieldsRowMeta,
        rows );
      List<Object[]> newList = editRowsDialog.open();
      if ( newList != null ) {

        RowMetaAndData rmad = new RowMetaAndData();
        rmad.setRowMeta( columnsRowMeta );
        boolean batch = false;

        // Write these rows to the database...
        //
        // For now let's just do the optimistic thing, no batch, no nothing.
        // Delete and insert in one transaction...
        //
        try {
          database = new Database( new LoggingObject( "DataSetDialog" ), databaseMeta );
          database.connect();
          database.setCommit( newList.size() + 10000 );
          database.execStatement( "DELETE FROM " + schemaTable );
          database.prepareInsert( columnsRowMeta, group.getSchemaName(), set.getTableName() );

          for ( Object[] row : newList ) {
            rmad.setData( row );
            database.setValuesInsert( rmad );
            database.insertRow( batch );
          }
          database.insertFinished( batch );
          database.commit();
        } catch ( Exception e ) {
          database.rollback();
          throw e;
        } finally {
          if ( database != null ) {
            database.disconnect();
          }
        }
      }

    } catch ( Exception e ) {
      new ErrorDialog( shell, "Error", "Error previewing data from dataset table", e );
    }
  }

  protected void viewData() {
    int previewSize = props.getDefaultPreviewSize();
    try {
      DataSetGroup group = verifySettings();
      DatabaseMeta databaseMeta = group.getDatabaseMeta();
      String schemaTable = databaseMeta.getQuotedSchemaTableCombination( group.getSchemaName(), wTableName.getText() );

      Database database = null;
      try {
        database = new Database( new LoggingObject( "DataSetDialog" ), databaseMeta );
        database.connect();

        ResultSet resultSet = database.openQuery( "SELECT * FROM " + schemaTable );
        try {
          List<Object[]> buffer = new ArrayList<Object[]>();
          Object[] row = database.getRow( resultSet );
          RowMetaInterface rowMeta = database.getReturnRowMeta();
          while ( row != null ) {
            buffer.add( row );
            if ( buffer.size() > previewSize ) {
              PreviewRowsDialog previewRowsDialog = new PreviewRowsDialog( shell, new Variables(), SWT.NONE, schemaTable, rowMeta, buffer );
              previewRowsDialog.open();
              buffer.clear();
              if ( previewRowsDialog.isAskingToStop() ) {
                break;
              }
            }
            row = database.getRow( resultSet );
          }

          if ( buffer.size() > 0 ) {
            PreviewRowsDialog previewRowsDialog = new PreviewRowsDialog( shell, new Variables(), SWT.NONE, schemaTable, rowMeta, buffer );
            previewRowsDialog.open();
            buffer.clear();
          }
        } finally {
          resultSet.close();
        }

      } finally {
        if ( database != null ) {
          database.disconnect();
        }
      }
    } catch ( Exception e ) {
      new ErrorDialog( shell, "Error", "Error previewing data from dataset table", e );
    }
  }

  public void dispose() {
    props.setScreen( new WindowProperty( shell ) );
    shell.dispose();
  }

  public void getData() {

    for ( DataSetGroup group : groups ) {
      wDataSetGroup.add( group.getName() );
    }

    wName.setText( Const.NVL( dataSet.getName(), "" ) );
    wDescription.setText( Const.NVL( dataSet.getDescription(), "" ) );
    wDataSetGroup.setText( Const.NVL( dataSet.getGroup() == null ? null : dataSet.getGroup().getName(), "" ) );
    wTableName.setText( Const.NVL( dataSet.getTableName(), "" ) );
    for ( int i = 0; i < dataSet.getFields().size(); i++ ) {
      DataSetField field = dataSet.getFields().get( i );
      int colnr = 1;
      wFieldMapping.setText( Const.NVL( field.getFieldName(), "" ), colnr++, i );
      wFieldMapping.setText( Const.NVL( field.getColumnName(), "" ), colnr++, i );
      wFieldMapping.setText( ValueMetaFactory.getValueMetaName( field.getType() ), colnr++, i );
      wFieldMapping.setText( field.getLength() >= 0 ? Integer.toString( field.getLength() ) : "", colnr++, i );
      wFieldMapping.setText( field.getPrecision() >= 0 ? Integer.toString( field.getPrecision() ) : "", colnr++, i );
      wFieldMapping.setText( Const.NVL( field.getComment(), "" ), colnr++, i );
    }
    wName.setFocus();
  }

  private void cancel() {
    ok = false;
    dispose();
  }

  /**
   * @param set The data set to load the dialog information into
   */
  public void getInfo( DataSet set ) {

    set.setName( wName.getText() );
    set.setDescription( wDescription.getText() );
    set.setGroup( DataSetConst.findDataSetGroup( groups, wDataSetGroup.getText() ) );
    set.setTableName( wTableName.getText() );
    set.getFields().clear();
    int nrFields = wFieldMapping.nrNonEmpty();
    for ( int i = 0; i < nrFields; i++ ) {
      TableItem item = wFieldMapping.getNonEmpty( i );
      int colnr = 1;
      String fieldName = item.getText( colnr++ );
      String columnName = item.getText( colnr++ );
      int type = ValueMetaFactory.getIdForValueMeta( item.getText( colnr++ ) );
      int length = Const.toInt( item.getText( colnr++ ), -1 );
      int precision = Const.toInt( item.getText( colnr++ ), -1 );
      String comment = item.getText( colnr++ );

      DataSetField field = new DataSetField( fieldName, columnName, type, length, precision, comment );
      set.getFields().add( field );
    }

  }

  public void ok() {

    try {
      verifySettings();
    } catch ( Exception e ) {
      new ErrorDialog( shell, "Error", BaseMessages.getString( PKG, "DataSetDialog.Error.ValidationError" ), e );
    }

    getInfo( dataSet );

    ok = true;
    dispose();

  }

  public IMetaStore getMetaStore() {
    return metaStore;
  }

  public void setMetaStore(IMetaStore metaStore) {
    this.metaStore = metaStore;
  }

}

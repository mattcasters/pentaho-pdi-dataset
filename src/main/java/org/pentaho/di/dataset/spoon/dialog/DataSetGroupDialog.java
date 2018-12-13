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

import java.util.List;

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
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
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.dataset.DataSetGroup;
import org.pentaho.di.dataset.DataSetGroupType;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.gui.WindowProperty;
import org.pentaho.di.ui.core.widget.ComboVar;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

public class DataSetGroupDialog extends Dialog {
  private static Class<?> PKG = DataSetGroupDialog.class; // for i18n purposes, needed by Translator2!!

  private DataSetGroup dataSetGroup;

  private Group wgDatabase;
  private Group wgCsv;

  private List<DatabaseMeta> databases;

  private Shell shell;

  private Text wName;
  private Text wDescription;
  private Combo wGroupType;

  // Database type
  //
  private Combo wDatabase;
  private Text wSchemaName;

  // CSV type
  //
  private TextVar wFolderName;
  
  private Button wOK, wCancel;

  private PropsUI props;

  private int middle;
  private int margin;

  private boolean ok;

  private VariableSpace space;

  public DataSetGroupDialog( Shell parent, DataSetGroup dataSetGroup, List<DatabaseMeta> databases ) {
    super( parent, SWT.NONE );
    this.dataSetGroup = dataSetGroup;
    this.databases = databases;
    props = PropsUI.getInstance();
    ok = false;

    space = new Variables();
    space.initializeVariablesFrom( null );
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

    shell.setText( BaseMessages.getString( PKG, "DataSetGroupDialog.Shell.Title" ) );
    shell.setLayout( formLayout );

    // The name of the group...
    //
    Label wlName = new Label( shell, SWT.RIGHT );
    props.setLook( wlName );
    wlName.setText( BaseMessages.getString( PKG, "DataSetGroupDialog.GroupName.Label" ) );
    FormData fdlName = new FormData();
    fdlName.top = new FormAttachment( 0, 0 );
    fdlName.left = new FormAttachment( 0, 0 );
    fdlName.right = new FormAttachment( middle, -margin );
    wlName.setLayoutData( fdlName );
    wName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wName );
    FormData fdName = new FormData();
    fdName.top = new FormAttachment( wlName, 0, SWT.CENTER);
    fdName.left = new FormAttachment( middle, 0 );
    fdName.right = new FormAttachment( 100, 0 );
    wName.setLayoutData( fdName );
    Control lastControl = wName;

    // The description of the group...
    //
    Label wlDescription = new Label( shell, SWT.RIGHT );
    props.setLook( wlDescription );
    wlDescription.setText( BaseMessages.getString( PKG, "DataSetGroupDialog.GroupDescription.Label" ) );
    FormData fdlDescription = new FormData();
    fdlDescription.top = new FormAttachment( lastControl, margin );
    fdlDescription.left = new FormAttachment( 0, 0 );
    fdlDescription.right = new FormAttachment( middle, -margin );
    wlDescription.setLayoutData( fdlDescription );
    wDescription = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wDescription );
    FormData fdDescription = new FormData();
    fdDescription.top = new FormAttachment( wlDescription, 0, SWT.CENTER);
    fdDescription.left = new FormAttachment( middle, 0 );
    fdDescription.right = new FormAttachment( 100, 0 );
    wDescription.setLayoutData( fdDescription );
    lastControl = wDescription;


    // The type of the group...
    //
    Label wlGroupType = new Label( shell, SWT.RIGHT );
    props.setLook( wlGroupType );
    wlGroupType.setText( BaseMessages.getString( PKG, "DataSetGroupDialog.GroupType.Label" ) );
    FormData fdlGroupType = new FormData();
    fdlGroupType.top = new FormAttachment( lastControl, margin );
    fdlGroupType.left = new FormAttachment( 0, 0 );
    fdlGroupType.right = new FormAttachment( middle, -margin );
    wlGroupType.setLayoutData( fdlGroupType );
    wGroupType = new Combo( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wGroupType.setItems( DataSetGroupType.getNames() );
    FormData fdGroupType = new FormData();
    fdGroupType.top = new FormAttachment( wlGroupType, 0, SWT.CENTER);
    fdGroupType.left = new FormAttachment( middle, 0 );
    fdGroupType.right = new FormAttachment( 100, 0 );
    wGroupType.setLayoutData( fdGroupType );
    lastControl = wGroupType;

    wGroupType.addListener( SWT.Selection, e->enableGroups() );


    wgDatabase = new Group(shell, SWT.NO_BACKGROUND | SWT.SHADOW_ETCHED_IN);
    props.setLook(wgDatabase);
    wgDatabase.setText("Database settings");
    FormLayout databaseGroupLayout = new FormLayout();
    databaseGroupLayout.spacing = 10;
    databaseGroupLayout.marginTop = 10;
    databaseGroupLayout.marginBottom= 10;
    wgDatabase.setLayout(databaseGroupLayout);

    // The database of the group...
    //
    Label wlDatabase = new Label( wgDatabase, SWT.RIGHT );
    props.setLook( wlDatabase );
    wlDatabase.setText( BaseMessages.getString( PKG, "DataSetGroupDialog.GroupDatabase.Label" ) );
    FormData fdlDatabase = new FormData();
    fdlDatabase.top = new FormAttachment( 0, 0 );
    fdlDatabase.left = new FormAttachment( 0, 0 );
    fdlDatabase.right = new FormAttachment( middle, -margin );
    wlDatabase.setLayoutData( fdlDatabase );
    wDatabase = new Combo( wgDatabase, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    FormData fdDatabase = new FormData();
    fdDatabase.top = new FormAttachment( wlDatabase, 0, SWT.CENTER);
    fdDatabase.left = new FormAttachment( middle, 0 );
    fdDatabase.right = new FormAttachment( 100, 0 );
    wDatabase.setLayoutData( fdDatabase );
    Control lastGroupControl = wDatabase;

    // The schema in the database...
    //
    Label wlSchemaName = new Label( wgDatabase, SWT.RIGHT );
    props.setLook( wlSchemaName );
    wlSchemaName.setText( BaseMessages.getString( PKG, "DataSetGroupDialog.GroupSchemaName.Label" ) );
    FormData fdlSchemaName = new FormData();
    fdlSchemaName.top = new FormAttachment( lastGroupControl, margin );
    fdlSchemaName.left = new FormAttachment( 0, 0 );
    fdlSchemaName.right = new FormAttachment( middle, -margin );
    wlSchemaName.setLayoutData( fdlSchemaName );
    wSchemaName = new Text( wgDatabase, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSchemaName );
    FormData fdSchemaName = new FormData();
    fdSchemaName.top = new FormAttachment( wlSchemaName, 0, SWT.CENTER );
    fdSchemaName.left = new FormAttachment( middle, 0 );
    fdSchemaName.right = new FormAttachment( 100, 0 );
    wSchemaName.setLayoutData( fdSchemaName );

    FormData fdgDatabase = new FormData();
    fdgDatabase.top = new FormAttachment( lastControl, margin*2 );
    fdgDatabase.left = new FormAttachment( 0, 0 );
    fdgDatabase.right = new FormAttachment( 100, 0 );
    wgDatabase.setLayoutData( fdgDatabase );
    lastControl = wgDatabase;

    wgCsv = new Group(shell, SWT.NO_BACKGROUND | SWT.SHADOW_ETCHED_IN);
    props.setLook(wgCsv);
    wgCsv.setText("CSV settings");
    FormLayout csvGroupLayout = new FormLayout();
    csvGroupLayout.spacing = 10;
    csvGroupLayout.marginTop = 10;
    csvGroupLayout.marginBottom= 10;
    wgCsv.setLayout(csvGroupLayout);


    // The folder for the CSV files...
    //
    Label wlFolderName = new Label( wgCsv, SWT.RIGHT );
    props.setLook( wlFolderName );
    wlFolderName.setText( BaseMessages.getString( PKG, "DataSetGroupDialog.GroupFolderName.Label" ) );
    FormData fdlFolderName = new FormData();
    fdlFolderName.top = new FormAttachment( 0, 0 );
    fdlFolderName.left = new FormAttachment( 0, 0 );
    fdlFolderName.right = new FormAttachment( middle, -margin );
    wlFolderName.setLayoutData( fdlFolderName );
    wFolderName = new TextVar( space, wgCsv, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFolderName );
    FormData fdFolderName = new FormData();
    fdFolderName.top = new FormAttachment( wlFolderName, 0, SWT.CENTER);
    fdFolderName.left = new FormAttachment( middle, 0 );
    fdFolderName.right = new FormAttachment( 100, 0 );
    wFolderName.setLayoutData( fdFolderName );

    FormData fdgCsv = new FormData();
    fdgCsv.top = new FormAttachment( lastControl, margin*2 );
    fdgCsv.left = new FormAttachment( 0, 0 );
    fdgCsv.right = new FormAttachment( 100, 0 );
    wgCsv.setLayoutData( fdgCsv );
    lastControl = wgCsv;

    // Buttons
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    Button[] buttons = new Button[] { wOK, wCancel };
    BaseStepDialog.positionBottomButtons( shell, buttons, margin, lastControl );

    // Add listeners
    wOK.addListener( SWT.Selection, new Listener() {
      public void handleEvent( Event e ) {
        ok();
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
    wSchemaName.addSelectionListener( selAdapter );
    wFolderName.addSelectionListener( selAdapter );
    wGroupType.addSelectionListener( selAdapter );

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

  private void enableGroups() {
    DataSetGroupType type = DataSetGroupType.fromName( wGroupType.getText() );

    wgDatabase.setEnabled( type==DataSetGroupType.Database );
    wDatabase.setEnabled( type==DataSetGroupType.Database );
    wSchemaName.setEnabled( type==DataSetGroupType.Database );

    wgCsv.setEnabled( type==DataSetGroupType.CSV );
    wFolderName.setEnabled( type==DataSetGroupType.CSV );

  }

  public void dispose() {
    props.setScreen( new WindowProperty( shell ) );
    shell.dispose();
  }

  public void getData() {

    for ( DatabaseMeta database : databases ) {
      wDatabase.add( database.getName() );
    }

    wName.setText( Const.NVL( dataSetGroup.getName(), "" ) );
    wDescription.setText( Const.NVL( dataSetGroup.getDescription(), "" ) );
    wGroupType.setText( dataSetGroup.getType()==null ? DataSetGroupType.Database.name() : dataSetGroup.getType().name());
    wDatabase.setText( Const.NVL( dataSetGroup.getDatabaseMeta() == null ? null : dataSetGroup.getDatabaseMeta().getName(), "" ) );
    wSchemaName.setText( Const.NVL( dataSetGroup.getSchemaName(), "" ) );
    wFolderName.setText( Const.NVL( dataSetGroup.getFolderName(), "" ) );

    enableGroups();

    wName.setFocus();
  }

  private void cancel() {
    ok = false;
    dispose();
  }

  public void ok() {

    dataSetGroup.setName( wName.getText() );
    dataSetGroup.setDescription( wDescription.getText() );
    dataSetGroup.setType( DataSetGroupType.fromName( wGroupType.getText() ) );
    dataSetGroup.setDatabaseMeta( DatabaseMeta.findDatabase( databases, wDatabase.getText() ) );
    dataSetGroup.setSchemaName( wSchemaName.getText() );
    dataSetGroup.setFolderName( wFolderName.getText() );

    ok = true;

    dispose();
  }
}

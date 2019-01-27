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

import org.apache.commons.lang.StringUtils;
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
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.SourceToTargetMapping;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.dataset.DataSet;
import org.pentaho.di.dataset.TransUnitTestFieldMapping;
import org.pentaho.di.dataset.TransUnitTestSetLocation;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.dialog.EnterMappingDialog;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.gui.WindowProperty;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TransUnitTestSetLocationDialog extends Dialog {
  private static Class<?> PKG = TransUnitTestSetLocationDialog.class; // for i18n purposes, needed by Translator2!!

  private TransUnitTestSetLocation location;
  private final List<DataSet> dataSets;
  private final Map<String, RowMetaInterface> stepFieldsMap;

  private String[] stepNames;
  private String[] datasetNames;

  private Shell shell;

  private Combo wStepName;
  private Combo wDatasetName;
  private TableView wFieldMappings;
  private TableView wFieldOrder;

  private Button wOK;
  private Button wMapFields;
  private Button wGetSortFields;
  private Button wCancel;

  private PropsUI props;

  private int middle;
  private int margin;

  private boolean ok;

  private List<DatabaseMeta> databases;

  public TransUnitTestSetLocationDialog( Shell parent, TransUnitTestSetLocation location, List<DataSet> dataSets, Map<String, RowMetaInterface> stepFieldsMap ) {
    super( parent, SWT.NONE );
    this.location = location;
    this.dataSets = dataSets;
    this.stepFieldsMap = stepFieldsMap;
    props = PropsUI.getInstance();
    ok = false;

    stepNames = stepFieldsMap.keySet().toArray( new String[ 0 ] );
    datasetNames = new String[ dataSets.size() ];
    for ( int i = 0; i < datasetNames.length; i++ ) {
      datasetNames[ i ] = dataSets.get( i ).getName();
    }
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

    shell.setText( BaseMessages.getString( PKG, "TransUnitTestSetLocationDialog.Shell.Title" ) );
    shell.setLayout( formLayout );

    // Step name
    //
    Label wlStepName = new Label( shell, SWT.RIGHT );
    props.setLook( wlStepName );
    wlStepName.setText( BaseMessages.getString( PKG, "TransUnitTestSetLocationDialog.StepName.Label" ) );
    FormData fdlStepName = new FormData();
    fdlStepName.top = new FormAttachment( 0, 0 );
    fdlStepName.left = new FormAttachment( 0, 0 );
    fdlStepName.right = new FormAttachment( middle, -margin );
    wlStepName.setLayoutData( fdlStepName );
    wStepName = new Combo( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wStepName.setItems( stepNames );
    FormData fdStepName = new FormData();
    fdStepName.top = new FormAttachment( 0, 0 );
    fdStepName.left = new FormAttachment( middle, 0 );
    fdStepName.right = new FormAttachment( 100, 0 );
    wStepName.setLayoutData( fdStepName );
    Control lastControl = wStepName;

    //
    //
    Label wlDatasetName = new Label( shell, SWT.RIGHT );
    props.setLook( wlDatasetName );
    wlDatasetName.setText( BaseMessages.getString( PKG, "TransUnitTestSetLocationDialog.DatasetName.Label" ) );
    FormData fdlDatasetName = new FormData();
    fdlDatasetName.top = new FormAttachment( lastControl, margin );
    fdlDatasetName.left = new FormAttachment( 0, 0 );
    fdlDatasetName.right = new FormAttachment( middle, -margin );
    wlDatasetName.setLayoutData( fdlDatasetName );
    wDatasetName = new Combo( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wDatasetName.setItems( datasetNames );
    FormData fdDatasetName = new FormData();
    fdDatasetName.top = new FormAttachment( lastControl, margin );
    fdDatasetName.left = new FormAttachment( middle, 0 );
    fdDatasetName.right = new FormAttachment( 100, 0 );
    wDatasetName.setLayoutData( fdDatasetName );
    lastControl = wDatasetName;

    // The field mapping from the step to the data set...
    //
    Label wlFieldMapping = new Label( shell, SWT.LEFT );
    wlFieldMapping.setText( BaseMessages.getString( PKG, "TransUnitTestSetLocationDialog.FieldMapping.Label" ) );
    props.setLook( wlFieldMapping );
    FormData fdlFieldMapping = new FormData();
    fdlFieldMapping.left = new FormAttachment( 0, 0 );
    fdlFieldMapping.right = new FormAttachment( 60, -margin );
    fdlFieldMapping.top = new FormAttachment( lastControl, margin * 2 );
    wlFieldMapping.setLayoutData( fdlFieldMapping );

    Label wlFieldOrder = new Label( shell, SWT.LEFT );
    wlFieldOrder.setText( BaseMessages.getString( PKG, "TransUnitTestSetLocationDialog.FieldOrder.Label" ) );
    props.setLook( wlFieldOrder );
    FormData fdlFieldOrder = new FormData();
    fdlFieldOrder.left = new FormAttachment( 60, margin );
    fdlFieldOrder.right = new FormAttachment( 100, 0 );
    fdlFieldOrder.top = new FormAttachment( lastControl, margin * 2 );
    wlFieldOrder.setLayoutData( fdlFieldOrder );

    lastControl = wlFieldMapping;


    // Buttons at the bottom...
    //
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );

    wMapFields = new Button( shell, SWT.PUSH );
    wMapFields.setText( BaseMessages.getString( PKG, "TransUnitTestSetLocationDialog.MapFields.Button" ) );

    wGetSortFields = new Button( shell, SWT.PUSH );
    wGetSortFields.setText( BaseMessages.getString( PKG, "TransUnitTestSetLocationDialog.GetSortFields.Button" ) );

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    Button[] buttons = new Button[] { wOK, wMapFields, wGetSortFields, wCancel };
    BaseStepDialog.positionBottomButtons( shell, buttons, margin, null );


    // the field mapping grid in between on the left
    //
    ColumnInfo[] FieldMappingColumns = new ColumnInfo[] {
      new ColumnInfo( BaseMessages.getString( PKG, "TransUnitTestSetLocationDialog.ColumnInfo.StepField" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false ),
      new ColumnInfo( BaseMessages.getString( PKG, "TransUnitTestSetLocationDialog.ColumnInfo.DatasetField" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false ),
    };

    wFieldMappings = new TableView(
      new Variables(),
      shell,
      SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
      FieldMappingColumns,
      location.getFieldMappings().size(),
      null, props );

    FormData fdFieldMapping = new FormData();
    fdFieldMapping.left = new FormAttachment( 0, 0 );
    fdFieldMapping.top = new FormAttachment( lastControl, margin );
    fdFieldMapping.right = new FormAttachment( 60, -margin );
    fdFieldMapping.bottom = new FormAttachment( wOK, -2 * margin );
    wFieldMappings.setLayoutData( fdFieldMapping );

    // the field mapping grid in between on the left
    //
    ColumnInfo[] FieldOrderColumns = new ColumnInfo[] {
      new ColumnInfo( BaseMessages.getString( PKG, "TransUnitTestSetLocationDialog.ColumnInfo.DatasetField" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false ),
    };

    wFieldOrder = new TableView(
      new Variables(),
      shell,
      SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
      FieldOrderColumns,
      location.getFieldOrder().size(),
      null, props );

    FormData fdFieldOrder = new FormData();
    fdFieldOrder.left = new FormAttachment( 60, margin );
    fdFieldOrder.top = new FormAttachment( lastControl, margin );
    fdFieldOrder.right = new FormAttachment( 100, 0 );
    fdFieldOrder.bottom = new FormAttachment( wOK, -2 * margin );
    wFieldOrder.setLayoutData( fdFieldOrder );


    // Add listeners
    wOK.addListener( SWT.Selection, new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    } );
    wMapFields.addListener( SWT.Selection, new Listener() {
      public void handleEvent( Event e ) {
        getFieldMappings();
      }
    } );
    wGetSortFields.addListener( SWT.Selection, new Listener() {
      public void handleEvent( Event e ) {
        getSortFields();
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
    wStepName.addSelectionListener( selAdapter );
    wDatasetName.addSelectionListener( selAdapter );

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

  protected void getFieldMappings() {

    try {

      TransUnitTestSetLocation loc = new TransUnitTestSetLocation();
      getInfo( loc );

      String stepName = wStepName.getText();
      String datasetName = wDatasetName.getText();
      if ( StringUtils.isEmpty( stepName ) || StringUtils.isEmpty( datasetName ) ) {
        throw new KettleException( "Please select a step and a data set to map fields between" );
      }

      RowMetaInterface stepRowMeta = stepFieldsMap.get( stepName );
      if ( stepRowMeta == null ) {
        throw new KettleException( "Unable to find fields for step " + stepName );
      }
      String[] stepFieldNames = stepRowMeta.getFieldNames();

      DataSet dataSet = findDataSet( datasetName );
      RowMetaInterface setRowMeta = dataSet.getSetRowMeta( false );
      String[] setFieldNames = setRowMeta.getFieldNames();

      // Get the current mappings...
      //
      List<SourceToTargetMapping> currentMappings = new ArrayList<>();
      for ( TransUnitTestFieldMapping mapping : loc.getFieldMappings() ) {
        int stepFieldIndex = stepRowMeta.indexOfValue( mapping.getStepFieldName() );
        int setFieldIndex = stepRowMeta.indexOfValue( mapping.getDataSetFieldName() );
        if ( stepFieldIndex >= 0 && setFieldIndex >= 0 ) {
          currentMappings.add( new SourceToTargetMapping( stepFieldIndex, setFieldIndex ) );
        }
      }
      // Edit them
      //
      EnterMappingDialog mappingDialog = new EnterMappingDialog( shell, stepFieldNames, setFieldNames, currentMappings );
      List<SourceToTargetMapping> newMappings = mappingDialog.open();
      if ( newMappings != null ) {
        // Simply clean everything and add the new mappings
        //
        wFieldMappings.clearAll();
        for ( SourceToTargetMapping sourceToTargetMapping : newMappings ) {
          TableItem item = new TableItem( wFieldMappings.table, SWT.NONE );
          item.setText( 1, stepFieldNames[ sourceToTargetMapping.getSourcePosition() ] );
          item.setText( 2, setFieldNames[ sourceToTargetMapping.getTargetPosition() ] );
        }
        wFieldMappings.removeEmptyRows();
        wFieldMappings.setRowNums();
        wFieldMappings.optWidth( true );
      }
    } catch ( Exception e ) {
      new ErrorDialog( shell, "Error", "Error mapping fields from step to dataset", e );
    }
  }

  private DataSet findDataSet( String datasetName ) throws KettleException {
    for ( DataSet dataSet : dataSets ) {
      if ( dataSet.getName().equalsIgnoreCase( datasetName ) ) {
        return dataSet;
      }
    }
    throw new KettleException( "Unable to find data set with name " + datasetName );
  }

  protected void getSortFields() {
    try {
      String datasetName = wDatasetName.getText();
      if ( StringUtils.isEmpty( datasetName ) ) {
        throw new KettleException( "Please select a data set to get order fields from" );
      }

      DataSet dataSet = findDataSet( datasetName );
      RowMetaInterface setRowMeta = dataSet.getSetRowMeta( false );
      String[] setFieldNames = setRowMeta.getFieldNames();

      wFieldOrder.clearAll();
      for ( String setFieldName : setFieldNames ) {
        TableItem item = new TableItem( wFieldOrder.table, SWT.NONE );
        item.setText( 1, setFieldName );
      }
      wFieldOrder.removeEmptyRows();
      wFieldOrder.setRowNums();
      wFieldOrder.optWidth( true );

    } catch ( Exception e ) {
      new ErrorDialog( shell, "Error", "Error getting sort fields", e );
    }
  }

  public void dispose() {
    props.setScreen( new WindowProperty( shell ) );
    shell.dispose();
  }

  public void getData() {

    wStepName.setText( Const.NVL( location.getStepname(), "" ) );
    wDatasetName.setText( Const.NVL( location.getDataSetName(), "" ) );

    for ( int i = 0; i < location.getFieldMappings().size(); i++ ) {
      TransUnitTestFieldMapping fieldMapping = location.getFieldMappings().get( i );
      int colnr = 1;
      wFieldMappings.setText( Const.NVL( fieldMapping.getStepFieldName(), "" ), colnr++, i );
      wFieldMappings.setText( Const.NVL( fieldMapping.getDataSetFieldName(), "" ), colnr++, i );
    }
    wFieldMappings.removeEmptyRows();
    wFieldMappings.setRowNums();
    wFieldMappings.optWidth( true );

    for ( int i = 0; i < location.getFieldOrder().size(); i++ ) {
      String field = location.getFieldOrder().get( i );
      int colnr = 1;
      wFieldOrder.setText( Const.NVL( field, "" ), colnr++, i );
    }
    wFieldOrder.removeEmptyRows();
    wFieldOrder.setRowNums();
    wFieldOrder.optWidth( true );

    wStepName.setFocus();
  }

  private void cancel() {
    ok = false;
    dispose();
  }

  /**
   * @param loc The data set to load the dialog information into
   */
  public void getInfo( TransUnitTestSetLocation loc ) {

    loc.setStepname( wStepName.getText() );
    loc.setDataSetName( wDatasetName.getText() );
    loc.getFieldMappings().clear();

    int nrMappings = wFieldMappings.nrNonEmpty();
    for ( int i = 0; i < nrMappings; i++ ) {
      TableItem item = wFieldMappings.getNonEmpty( i );
      int colnr = 1;
      String stepFieldName = item.getText( colnr++ );
      String dataSetFieldName = item.getText( colnr++ );
      loc.getFieldMappings().add( new TransUnitTestFieldMapping( stepFieldName, dataSetFieldName ) );
    }

    loc.getFieldOrder().clear();
    int nrFields = wFieldOrder.nrNonEmpty();
    for ( int i = 0; i < nrFields; i++ ) {
      TableItem item = wFieldOrder.getNonEmpty( i );
      int colnr = 1;
      String fieldname = item.getText( colnr++ );
      loc.getFieldOrder().add( fieldname );
    }
  }

  public void ok() {
    getInfo( location );
    ok = true;
    dispose();
  }
}

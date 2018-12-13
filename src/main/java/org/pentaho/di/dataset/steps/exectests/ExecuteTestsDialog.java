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

package org.pentaho.di.dataset.steps.exectests;

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
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.dataset.util.DataSetConst;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

public class ExecuteTestsDialog extends BaseStepDialog implements StepDialogInterface {
  private static Class<?> PKG = ExecuteTestsDialog.class; // i18n
  
  private ExecuteTestsMeta input;
  
  private Combo wTypeToExecute;
  private TextVar wTransformationNameField;
  private TextVar wUnitTestNameField;
  private TextVar wDataSetNameField;
  private TextVar wStepNameField;
  private TextVar wErrorField;
  private TextVar wCommentField;
  
  public ExecuteTestsDialog(Shell parent, Object baseStepMeta, TransMeta transMeta, String stepname) {
    super(parent, (BaseStepMeta)baseStepMeta, transMeta, stepname);
    
    input = (ExecuteTestsMeta) baseStepMeta;
  }

  @Override
  public String open() {
    
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX );
    props.setLook( shell );
    setShellImage( shell, input );
    
    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "ExecuteTestsDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;
  
    // Step name...
    //
    wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname.setText( BaseMessages.getString( PKG, "ExecuteTestsDialog.Stepname.Label" ) );
    props.setLook( wlStepname );
    fdlStepname = new FormData();
    fdlStepname.left = new FormAttachment( 0, 0 );
    fdlStepname.right = new FormAttachment( middle, -margin );
    fdlStepname.top = new FormAttachment( 0, margin );
    wlStepname.setLayoutData( fdlStepname );
    wStepname = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wStepname.setText( stepname );
    props.setLook( wStepname );
    fdStepname = new FormData();
    fdStepname.left = new FormAttachment( middle, 0 );
    fdStepname.top = new FormAttachment( 0, margin );
    fdStepname.right = new FormAttachment( 100, 0 );
    wStepname.setLayoutData( fdStepname );
    Control lastControl = wStepname;
    
    // Type to execute
    //
    Label wlTypeToExecute = new Label( shell, SWT.RIGHT );
    wlTypeToExecute.setText( BaseMessages.getString( PKG, "ExecuteTestsDialog.TypeToExecute.Label" ) );
    props.setLook( wlTypeToExecute );
    FormData fdlTypeToExecute = new FormData();
    fdlTypeToExecute.left = new FormAttachment( 0, 0 );
    fdlTypeToExecute.right = new FormAttachment( middle, -margin );
    fdlTypeToExecute.top = new FormAttachment( lastControl, margin );
    wlTypeToExecute.setLayoutData( fdlTypeToExecute );
    wTypeToExecute = new Combo( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wTypeToExecute.setItems(DataSetConst.getTestTypeDescriptions());
    FormData fdTypeToExecute = new FormData();
    fdTypeToExecute.left = new FormAttachment( middle, 0 );
    fdTypeToExecute.top = new FormAttachment( lastControl, margin );
    fdTypeToExecute.right = new FormAttachment( 100, 0 );
    wTypeToExecute.setLayoutData( fdTypeToExecute );
    lastControl = wTypeToExecute;
    
    // Transformation name field
    //
    Label wlTransformationNameField = new Label( shell, SWT.RIGHT );
    wlTransformationNameField.setText( BaseMessages.getString( PKG, "ExecuteTestsDialog.TransformationNameField.Label" ) );
    props.setLook( wlTransformationNameField );
    FormData fdlTransformationNameField = new FormData();
    fdlTransformationNameField.left = new FormAttachment( 0, 0 );
    fdlTransformationNameField.right = new FormAttachment( middle, -margin );
    fdlTransformationNameField.top = new FormAttachment( lastControl, margin );
    wlTransformationNameField.setLayoutData( fdlTransformationNameField );
    wTransformationNameField = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTransformationNameField );
    FormData fdTransformationNameField = new FormData();
    fdTransformationNameField.left = new FormAttachment( middle, 0 );
    fdTransformationNameField.top = new FormAttachment( lastControl, margin );
    fdTransformationNameField.right = new FormAttachment( 100, 0 );
    wTransformationNameField.setLayoutData( fdTransformationNameField );
    lastControl = wTransformationNameField;
    
    // Unit test name field
    //
    Label wlUnitTestNameField = new Label( shell, SWT.RIGHT );
    wlUnitTestNameField.setText( BaseMessages.getString( PKG, "ExecuteTestsDialog.UnitTestNameField.Label" ) );
    props.setLook( wlUnitTestNameField );
    FormData fdlUnitTestNameField = new FormData();
    fdlUnitTestNameField.left = new FormAttachment( 0, 0 );
    fdlUnitTestNameField.right = new FormAttachment( middle, -margin );
    fdlUnitTestNameField.top = new FormAttachment( lastControl, margin );
    wlUnitTestNameField.setLayoutData( fdlUnitTestNameField );
    wUnitTestNameField = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wUnitTestNameField );
    FormData fdUnitTestNameField = new FormData();
    fdUnitTestNameField.left = new FormAttachment( middle, 0 );
    fdUnitTestNameField.top = new FormAttachment( lastControl, margin );
    fdUnitTestNameField.right = new FormAttachment( 100, 0 );
    wUnitTestNameField.setLayoutData( fdUnitTestNameField );
    lastControl = wUnitTestNameField;
    
    // Data Set Name field
    //
    Label wlDataSetNameField = new Label( shell, SWT.RIGHT );
    wlDataSetNameField.setText( BaseMessages.getString( PKG, "ExecuteTestsDialog.DataSetNameField.Label" ) );
    props.setLook( wlDataSetNameField );
    FormData fdlDataSetNameField = new FormData();
    fdlDataSetNameField.left = new FormAttachment( 0, 0 );
    fdlDataSetNameField.right = new FormAttachment( middle, -margin );
    fdlDataSetNameField.top = new FormAttachment( lastControl, margin );
    wlDataSetNameField.setLayoutData( fdlDataSetNameField );
    wDataSetNameField = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wDataSetNameField );
    FormData fdDataSetNameField = new FormData();
    fdDataSetNameField.left = new FormAttachment( middle, 0 );
    fdDataSetNameField.top = new FormAttachment( lastControl, margin );
    fdDataSetNameField.right = new FormAttachment( 100, 0 );
    wDataSetNameField.setLayoutData( fdDataSetNameField );
    lastControl = wDataSetNameField;

    // Step Name field
    //
    Label wlStepNameField = new Label( shell, SWT.RIGHT );
    wlStepNameField.setText( BaseMessages.getString( PKG, "ExecuteTestsDialog.StepNameField.Label" ) );
    props.setLook( wlStepNameField );
    FormData fdlStepNameField = new FormData();
    fdlStepNameField.left = new FormAttachment( 0, 0 );
    fdlStepNameField.right = new FormAttachment( middle, -margin );
    fdlStepNameField.top = new FormAttachment( lastControl, margin );
    wlStepNameField.setLayoutData( fdlStepNameField );
    wStepNameField = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wStepNameField );
    FormData fdStepNameField = new FormData();
    fdStepNameField.left = new FormAttachment( middle, 0 );
    fdStepNameField.top = new FormAttachment( lastControl, margin );
    fdStepNameField.right = new FormAttachment( 100, 0 );
    wStepNameField.setLayoutData( fdStepNameField );
    lastControl = wStepNameField;

    // Error field
    //
    Label wlErrorField = new Label( shell, SWT.RIGHT );
    wlErrorField.setText( BaseMessages.getString( PKG, "ExecuteTestsDialog.ErrorField.Label" ) );
    props.setLook( wlErrorField );
    FormData fdlErrorField = new FormData();
    fdlErrorField.left = new FormAttachment( 0, 0 );
    fdlErrorField.right = new FormAttachment( middle, -margin );
    fdlErrorField.top = new FormAttachment( lastControl, margin );
    wlErrorField.setLayoutData( fdlErrorField );
    wErrorField = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wErrorField );
    FormData fdErrorField = new FormData();
    fdErrorField.left = new FormAttachment( middle, 0 );
    fdErrorField.top = new FormAttachment( lastControl, margin );
    fdErrorField.right = new FormAttachment( 100, 0 );
    wErrorField.setLayoutData( fdErrorField );
    lastControl = wErrorField;

    // Comment field
    //
    Label wlCommentField = new Label( shell, SWT.RIGHT );
    wlCommentField.setText( BaseMessages.getString( PKG, "ExecuteTestsDialog.CommentField.Label" ) );
    props.setLook( wlCommentField );
    FormData fdlCommentField = new FormData();
    fdlCommentField.left = new FormAttachment( 0, 0 );
    fdlCommentField.right = new FormAttachment( middle, -margin );
    fdlCommentField.top = new FormAttachment( lastControl, margin );
    wlCommentField.setLayoutData( fdlCommentField );
    wCommentField = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wCommentField );
    FormData fdCommentField = new FormData();
    fdCommentField.left = new FormAttachment( middle, 0 );
    fdCommentField.top = new FormAttachment( lastControl, margin );
    fdCommentField.right = new FormAttachment( 100, 0 );
    wCommentField.setLayoutData( fdCommentField );
    lastControl = wCommentField;
    
    
    // Some buttons
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    // Add listeners
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };
    lsOK = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };

    wCancel.addListener( SWT.Selection, lsCancel );
    wOK.addListener( SWT.Selection, lsOK );
    setButtonPositions( new Button[] { wOK, wCancel }, margin, null );

    
    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wTransformationNameField.addSelectionListener( lsDef );
    wUnitTestNameField.addSelectionListener( lsDef );
    wDataSetNameField.addSelectionListener( lsDef );
    wStepNameField.addSelectionListener( lsDef );
    wErrorField.addSelectionListener( lsDef );
    wCommentField.addSelectionListener( lsDef );
    

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    // Set the shell size, based upon previous time...
    setSize();

    getData();
    input.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return stepname;
  }

  private void getData() {
    
    wTypeToExecute.setText( DataSetConst.getTestTypeDescription(input.getTypeToExecute()));
    wTransformationNameField.setText(Const.NVL(input.getTransformationNameField(), ""));
    wUnitTestNameField.setText(Const.NVL(input.getUnitTestNameField(), ""));
    wDataSetNameField.setText(Const.NVL(input.getDataSetNameField(), ""));
    wStepNameField.setText(Const.NVL(input.getStepNameField(), ""));
    wErrorField.setText(Const.NVL(input.getErrorField(), ""));
    wCommentField.setText(Const.NVL(input.getCommentField(), ""));
    
    wStepname.selectAll();
    wStepname.setFocus();
  }

  private void cancel() {
    stepname = null;
    input.setChanged( changed );
    dispose();
  }

  private void ok() {
    if ( StringUtil.isEmpty( wStepname.getText() ) ) {
      return;
    }

    stepname = wStepname.getText(); // return value

    input.setChanged();
    
    input.setTypeToExecute( DataSetConst.getTestTypeForDescription(wTypeToExecute.getText()) );
    input.setTransformationNameField( wTransformationNameField.getText() );
    input.setUnitTestNameField( wUnitTestNameField.getText() );
    input.setDataSetNameField( wDataSetNameField.getText() );
    input.setStepNameField( wStepNameField.getText() );
    input.setErrorField( wErrorField.getText() );
    input.setCommentField( wCommentField.getText() );
    
    dispose();
  }
}

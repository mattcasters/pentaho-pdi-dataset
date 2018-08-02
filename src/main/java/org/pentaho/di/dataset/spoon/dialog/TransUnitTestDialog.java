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

import java.util.Arrays;

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
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.dataset.TransUnitTest;
import org.pentaho.di.dataset.TransUnitTestDatabaseReplacement;
import org.pentaho.di.dataset.util.DataSetConst;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.gui.WindowProperty;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.util.PentahoDefaults;

public class TransUnitTestDialog extends Dialog {
  private static Class<?> PKG = TransUnitTestDialog.class; // for i18n purposes, needed by Translator2!!

  private TransUnitTest transUnitTest;
  protected TransMeta transMeta;

  private Shell shell;

  private Text wName;
  private Text wDescription;
  private CCombo wTestType;
  private TextVar wFilename;

  private TableView wDbReplacements;

  private Button wOK;
  private Button wCancel;

  private PropsUI props;

  private int middle;
  private int margin;

  private boolean ok;

  protected IMetaStore metaStore;
  MetaStoreFactory<TransUnitTest> setFactory;
  
  public TransUnitTestDialog(Shell parent, TransMeta transMeta, IMetaStore metaStore, TransUnitTest transUnitTest) throws KettleException, MetaStoreException {
    super(parent, SWT.NONE);
    this.transMeta = transMeta;
    this.metaStore = metaStore;
    this.transUnitTest = transUnitTest;
    props = PropsUI.getInstance();
    ok = false;

    setFactory = new MetaStoreFactory<TransUnitTest>(TransUnitTest.class, metaStore, PentahoDefaults.NAMESPACE);
  }

  public boolean open() {
    Shell parent = getParent();
    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    props.setLook(shell);
    shell.setImage(GUIResource.getInstance().getImageTable());

    middle = props.getMiddlePct();
    margin = Const.MARGIN;

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setText(BaseMessages.getString(PKG, "TransUnitTestDialog.Shell.Title"));
    shell.setLayout(formLayout);

    // The name of the unit test...
    //
    Label wlName = new Label(shell, SWT.RIGHT);
    props.setLook(wlName);
    wlName.setText(BaseMessages.getString(PKG, "TransUnitTestDialog.Name.Label"));
    FormData fdlName = new FormData();
    fdlName.top = new FormAttachment(0, 0);
    fdlName.left = new FormAttachment(0, 0);
    fdlName.right = new FormAttachment(middle, -margin);
    wlName.setLayoutData(fdlName);
    wName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wName);
    FormData fdName = new FormData();
    fdName.top = new FormAttachment(0, 0);
    fdName.left = new FormAttachment(middle, 0);
    fdName.right = new FormAttachment(100, 0);
    wName.setLayoutData(fdName);
    Control lastControl = wName;

    // The description of the test...
    //
    Label wlDescription = new Label(shell, SWT.RIGHT);
    props.setLook(wlDescription);
    wlDescription.setText(BaseMessages.getString(PKG, "TransUnitTestDialog.Description.Label"));
    FormData fdlDescription = new FormData();
    fdlDescription.top = new FormAttachment(lastControl, margin);
    fdlDescription.left = new FormAttachment(0, 0);
    fdlDescription.right = new FormAttachment(middle, -margin);
    wlDescription.setLayoutData(fdlDescription);
    wDescription = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wDescription);
    FormData fdDescription = new FormData();
    fdDescription.top = new FormAttachment(lastControl, margin);
    fdDescription.left = new FormAttachment(middle, 0);
    fdDescription.right = new FormAttachment(100, 0);
    wDescription.setLayoutData(fdDescription);
    lastControl = wDescription;

    // The type of test...
    //
    Label wlTestType = new Label(shell, SWT.RIGHT);
    props.setLook(wlTestType);
    wlTestType.setText(BaseMessages.getString(PKG, "TransUnitTestDialog.TestType.Label"));
    FormData fdlTestType = new FormData();
    fdlTestType.top = new FormAttachment(lastControl, margin);
    fdlTestType.left = new FormAttachment(0, 0);
    fdlTestType.right = new FormAttachment(middle, -margin);
    wlTestType.setLayoutData(fdlTestType);
    wTestType = new CCombo(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wTestType);
    FormData fdTestType = new FormData();
    fdTestType.top = new FormAttachment(lastControl, margin);
    fdTestType.left = new FormAttachment(middle, 0);
    fdTestType.right = new FormAttachment(100, 0);
    wTestType.setLayoutData(fdTestType);
    wTestType.setItems(DataSetConst.getTestTypeDescriptions());
    lastControl = wTestType;
    
    // The optional filename of the test result...
    //
    Label wlFilename = new Label(shell, SWT.RIGHT);
    props.setLook(wlFilename);
    wlFilename.setText(BaseMessages.getString(PKG, "TransUnitTestDialog.Filename.Label"));
    FormData fdlFilename = new FormData();
    fdlFilename.top = new FormAttachment(lastControl, margin);
    fdlFilename.left = new FormAttachment(0, 0);
    fdlFilename.right = new FormAttachment(middle, -margin);
    wlFilename.setLayoutData(fdlFilename);
    wFilename = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wFilename);
    FormData fdFilename = new FormData();
    fdFilename.top = new FormAttachment(lastControl, margin);
    fdFilename.left = new FormAttachment(middle, 0);
    fdFilename.right = new FormAttachment(100, 0);
    wFilename.setLayoutData(fdFilename);
    lastControl = wFilename;


    // The list of database replacements in the unit test transformation
    //
    Label wlFieldMapping = new Label(shell, SWT.NONE);
    wlFieldMapping.setText(BaseMessages.getString(PKG, "TransUnitTestDialog.DbReplacements.Label"));
    props.setLook(wlFieldMapping);
    FormData fdlUpIns = new FormData();
    fdlUpIns.left = new FormAttachment(0, 0);
    fdlUpIns.top = new FormAttachment(lastControl, margin);
    wlFieldMapping.setLayoutData(fdlUpIns);
    lastControl = wlFieldMapping;

    // Buttons at the bottom...
    //
    wOK = new Button(shell, SWT.PUSH);
    wOK.setText(BaseMessages.getString(PKG, "System.Button.OK"));

    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));

    Button[] buttons = new Button[] { wOK, wCancel };
    BaseStepDialog.positionBottomButtons(shell, buttons, margin, null);

    // the database replacements
    //
    String[] dbNames = transMeta.getDatabaseNames();
    Arrays.sort(dbNames);
    ColumnInfo[] columns = new ColumnInfo[] {
        new ColumnInfo(BaseMessages.getString(PKG, "TransUnitTestDialog.DbReplacement.ColumnInfo.OriginalDb"),
            ColumnInfo.COLUMN_TYPE_CCOMBO, dbNames, false),
        new ColumnInfo(BaseMessages.getString(PKG, "TransUnitTestDialog.DbReplacement.ColumnInfo.ReplacementDb"),
            ColumnInfo.COLUMN_TYPE_CCOMBO, dbNames, false), };
    columns[0].setUsingVariables(true);
    columns[1].setUsingVariables(true);
    
    wDbReplacements = new TableView(new Variables(), shell,
        SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL, columns,
        transUnitTest.getTweaks().size(), null, props);

    FormData fdDbReplacements = new FormData();
    fdDbReplacements.left = new FormAttachment(0, 0);
    fdDbReplacements.top = new FormAttachment(lastControl, margin);
    fdDbReplacements.right = new FormAttachment(100, 0);
    fdDbReplacements.bottom = new FormAttachment(wOK, -2 * margin);
    wDbReplacements.setLayoutData(fdDbReplacements);

    // Add listeners
    wOK.addListener(SWT.Selection, new Listener() {
      public void handleEvent(Event e) {
        ok();
      }
    });
    wCancel.addListener(SWT.Selection, new Listener() {
      public void handleEvent(Event e) {
        cancel();
      }
    });

    SelectionAdapter selAdapter = new SelectionAdapter() {
      public void widgetDefaultSelected(SelectionEvent e) {
        ok();
      }
    };
    wName.addSelectionListener(selAdapter);
    wDescription.addSelectionListener(selAdapter);
    wTestType.addSelectionListener(selAdapter);
    wFilename.addSelectionListener(selAdapter);

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener(new ShellAdapter() {
      public void shellClosed(ShellEvent e) {
        cancel();
      }
    });

    getData();

    BaseStepDialog.setSize(shell);

    shell.open();
    Display display = parent.getDisplay();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
    return ok;
  }

  public void dispose() {
    props.setScreen(new WindowProperty(shell));
    shell.dispose();
  }

  public void getData() {

    wName.setText( Const.NVL( transUnitTest.getName(), "" ) );
    wDescription.setText( Const.NVL( transUnitTest.getDescription(), "" ) );
    wTestType.setText( Const.NVL( DataSetConst.getTestTypeDescription(transUnitTest.getType()), "") );
    wFilename.setText( Const.NVL( transUnitTest.getFilename(), ""));
    
    for ( int i = 0; i < transUnitTest.getDatabaseReplacements().size(); i++ ) {
      TransUnitTestDatabaseReplacement dbReplacement = transUnitTest.getDatabaseReplacements().get( i );
      int colnr = 1;
      wDbReplacements.setText( Const.NVL( dbReplacement.getOriginalDatabaseName(), ""), colnr++, i );
      wDbReplacements.setText( Const.NVL( dbReplacement.getReplacementDatabaseName(), "" ), colnr++, i );
    }
    wDbReplacements.removeEmptyRows();
    wDbReplacements.setRowNums();

    wName.setFocus();
  }

  private void cancel() {
    ok = false;
    dispose();
  }

  /**
   * @param set
   *          The trans unit test to load the dialog information into
   */
  public void getInfo(TransUnitTest test) {

    test.setName(wName.getText());
    test.setDescription(wDescription.getText());
    test.setType(DataSetConst.getTestTypeForDescription(wTestType.getText()));
    test.setFilename(wFilename.getText());
    
    test.getDatabaseReplacements().clear();
    int nrFields = wDbReplacements.nrNonEmpty();
    for (int i=0;i<nrFields;i++) {
      TableItem item = wDbReplacements.getNonEmpty( i );
      String sourceDb = item.getText(1);
      String replaceDb = item.getText(2);
      TransUnitTestDatabaseReplacement dbReplacement = new TransUnitTestDatabaseReplacement(sourceDb, replaceDb);
      test.getDatabaseReplacements().add(dbReplacement);
    }    
  }

  public void ok() {

    getInfo(transUnitTest);

    ok = true;
    dispose();

  }

 
}

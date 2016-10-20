package org.pentaho.di.dataset.spoon.dialog;

import java.util.Arrays;

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
import org.pentaho.di.dataset.TransTweak;
import org.pentaho.di.dataset.TransUnitTest;
import org.pentaho.di.dataset.TransUnitTestTweak;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.gui.WindowProperty;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.util.PentahoDefaults;

public class TransUnitTestDialog extends Dialog {
  private static Class<?> PKG = TransUnitTestDialog.class; // for i18n purposes,
                                                           // needed by
                                                           // Translator2!!

  public static final String[] tweakDesc = new String[] {
      BaseMessages.getString(PKG, "TransUnitTestDialog.Tweak.NONE.Desc"),
      BaseMessages.getString(PKG, "TransUnitTestDialog.Tweak.BYPASS_STEP.Desc"),
      BaseMessages.getString(PKG, "TransUnitTestDialog.Tweak.REMOVE_STEP.Desc"),
      /*
        BaseMessages.getString(PKG, "TransUnitTestDialog.Tweak.REMOVE_STEPS_AFTER.Desc"),
        BaseMessages.getString(PKG, "TransUnitTestDialog.Tweak.REMOVE_STEPS_BEFORE.Desc"), 
      */
      };

  private TransUnitTest transUnitTest;
  protected TransMeta transMeta;

  private Shell shell;

  private Text wName;
  private Text wDescription;

  private TableView wTweaks;

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

    // The list of tweaks to the transformation
    //
    Label wlFieldMapping = new Label(shell, SWT.NONE);
    wlFieldMapping.setText(BaseMessages.getString(PKG, "TransUnitTestDialog.Tweaks.Label"));
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

    // the transformation tweaks
    //
    String[] stepNames = transMeta.getStepNames();
    Arrays.sort(stepNames);
    ColumnInfo[] columns = new ColumnInfo[] {
        new ColumnInfo(BaseMessages.getString(PKG, "TransUnitTestDialog.Tweak.ColumnInfo.Tweak"),
            ColumnInfo.COLUMN_TYPE_CCOMBO, tweakDesc, false),
        new ColumnInfo(BaseMessages.getString(PKG, "TransUnitTestDialog.Tweak.ColumnInfo.Step"),
            ColumnInfo.COLUMN_TYPE_CCOMBO, stepNames, false), };

    wTweaks = new TableView(new Variables(), shell,
        SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL, columns,
        transUnitTest.getTweaks().size(), null, props);

    FormData fdFieldMapping = new FormData();
    fdFieldMapping.left = new FormAttachment(0, 0);
    fdFieldMapping.top = new FormAttachment(lastControl, margin);
    fdFieldMapping.right = new FormAttachment(100, 0);
    fdFieldMapping.bottom = new FormAttachment(wOK, -2 * margin);
    wTweaks.setLayoutData(fdFieldMapping);

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
    
    for ( int i = 0; i < transUnitTest.getTweaks().size(); i++ ) {
      TransUnitTestTweak tweak = transUnitTest.getTweaks().get( i );
      int colnr = 1;
      wTweaks.setText( Const.NVL( getTweakDescription(tweak.getTweak()), "" ), colnr++, i );
      wTweaks.setText( Const.NVL( tweak.getStepName(), "" ), colnr++, i );
    }

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

    test.getTweaks().clear();
    int nrFields = wTweaks.nrNonEmpty();
    for (int i=0;i<nrFields;i++) {
      TableItem item = wTweaks.getNonEmpty( i );
      String tweakDesc = item.getText(1);
      TransTweak tweak = getTweakForDescription(tweakDesc);
      String stepName = item.getText(2);
      test.getTweaks().add(new TransUnitTestTweak(tweak, stepName));
    }    
  }

  public void ok() {

    getInfo(transUnitTest);

    ok = true;
    dispose();

  }

  public String getTweakDescription(TransTweak tweak) {
    int index = 0; // NONE
    if (tweak!=null) {
      TransTweak[] tweaks = TransTweak.values();
      for (int i=0;i<tweaks.length;i++) {
        if (tweaks[i]==tweak) {
          index=i;
          break;
        }
      }
    }
    
    return tweakDesc[index];
  }

  /**
   * Get the TransTweak for a tweak description (from the dialog)
   * @param tweakDescription The description to look for
   * @return the tweak or null if nothing matched
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
  
  
}

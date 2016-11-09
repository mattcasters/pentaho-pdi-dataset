package org.pentaho.di.dataset.spoon.xtpoint;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.widgets.Canvas;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;
import org.eclipse.swt.widgets.MessageBox;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.gui.AreaOwner;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.dataset.TransUnitTest;
import org.pentaho.di.dataset.spoon.DataSetHelper;
import org.pentaho.di.dataset.spoon.dialog.TransUnitTestDialog;
import org.pentaho.di.dataset.util.DataSetConst;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.spoon.trans.TransGraphExtension;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.stores.delegate.DelegatingMetaStore;
import org.pentaho.metastore.util.PentahoDefaults;

@ExtensionPoint(
    id = "ShowUnitTestMenuExtensionPoint",
    description = "Quick unit test menu",
    extensionPointId = "TransGraphMouseDown" )
public class ShowUnitTestMenuExtensionPoint implements ExtensionPointInterface {
  private static Class<?> PKG = ShowUnitTestMenuExtensionPoint.class; // for i18n purposes, needed by Translator2!!

  @Override
  public void callExtensionPoint(LogChannelInterface log, Object object) throws KettleException {
    
    if (!(object instanceof TransGraphExtension)) {
      return;
    }
    
    TransGraphExtension tge = (TransGraphExtension) object;
    final TransMeta transMeta = tge.getTransGraph().getTransMeta();
    Spoon spoon = Spoon.getInstance();
    MouseEvent e = tge.getEvent();
    
    if ( e.button == 1 || e.button == 2 ) {
      AreaOwner areaOwner = tge.getTransGraph().getVisibleAreaOwner( tge.getPoint().x, tge.getPoint().y );
      if ( areaOwner != null && areaOwner.getAreaType() != null ) {
        // Check if this is the flask...
        //
        if (DataSetConst.AREA_DRAWN_UNIT_ICON.equals(areaOwner.getParent())) {
          final String unitTestName = (String) areaOwner.getOwner(); 
          
          Canvas canvas = findCanvas(tge.getTransGraph());
          if (canvas!=null) {
            
            SelectionListener lsNew = new SelectionAdapter() { 
              @Override
              public void widgetSelected(SelectionEvent event) {
                newUnitTest();
              } };
            SelectionListener lsEdit = new SelectionAdapter() { 
              @Override
              public void widgetSelected(SelectionEvent event) {
                editUnitTest(unitTestName, transMeta);
              } };
            SelectionListener lsDelete = new SelectionAdapter() { 
              @Override
              public void widgetSelected(SelectionEvent event) {
                deleteUnitTest(unitTestName);
              } };
            SelectionListener lsDisable = new SelectionAdapter() { 
              @Override
              public void widgetSelected(SelectionEvent event) {
                disableUnitTest(unitTestName);
              } };
              
            
            Menu menu = new Menu(spoon.getShell(), SWT.POP_UP);
            MenuItem newItem = new MenuItem(menu, SWT.PUSH);
            newItem.setText(BaseMessages.getString(PKG, "ShowUnitTestMenuExtensionPoint.UnitMenu.New.Label"));
            newItem.addSelectionListener(lsNew);
            
            MenuItem editItem = new MenuItem(menu, SWT.PUSH);
            editItem.setText(BaseMessages.getString(PKG, "ShowUnitTestMenuExtensionPoint.UnitTest.Edit.Label"));
            editItem.addSelectionListener(lsEdit);
            editItem.setEnabled(StringUtils.isNotBlank(unitTestName));
            
            MenuItem deleteItem = new MenuItem(menu, SWT.PUSH);
            deleteItem.setText(BaseMessages.getString(PKG, "ShowUnitTestMenuExtensionPoint.UnitTest.Delete.Label"));
            deleteItem.addSelectionListener(lsDelete);
            deleteItem.setEnabled(StringUtils.isNotBlank(unitTestName));
            
            MenuItem disableItem = new MenuItem(menu, SWT.PUSH);
            disableItem.setText(BaseMessages.getString(PKG, "ShowUnitTestMenuExtensionPoint.UnitTest.Disable.Label"));
            disableItem.addSelectionListener(lsDisable);
            disableItem.setEnabled(StringUtils.isNotBlank(unitTestName));
            
            MenuItem switchItem = new MenuItem(menu, SWT.CASCADE);
            switchItem.setText(BaseMessages.getString(PKG, "ShowUnitTestMenuExtensionPoint.UnitTest.Switch.Label"));
            
            Menu switchMenu = new Menu(menu);
            switchItem.setMenu(switchMenu);
            List<TransUnitTest> tests = findUnitTests(tge.getTransGraph().getTransMeta(), spoon.getMetaStore());
            for (final TransUnitTest test : tests) {
              MenuItem testItem = new MenuItem(switchMenu, SWT.PUSH);
              testItem.setText(test.getName());
              testItem.addSelectionListener(new SelectionAdapter() {
                @Override
                public void widgetSelected(SelectionEvent event) {
                  switchUnitTest(test, transMeta);
                }
              });
            }
            
            canvas.setMenu(menu);
            Point location = canvas.toDisplay(tge.getEvent().x, tge.getEvent().y);
            menu.setLocation(location);
            menu.setVisible(true);
            
          }
        }
      }
    }
  }

  protected void newUnitTest() {
    DataSetHelper.getInstance().createUnitTest();
  }
  
  protected void editUnitTest(String unitTestName, TransMeta transMeta) {
    try {
      Spoon spoon = Spoon.getInstance();
      MetaStoreFactory<TransUnitTest> setFactory = new MetaStoreFactory<TransUnitTest>(
          TransUnitTest.class, spoon.getMetaStore(), PentahoDefaults.NAMESPACE);
      TransUnitTest unitTest = setFactory.loadElement(unitTestName);
      if (unitTest==null) {
        throw new KettleException(BaseMessages.getString(PKG, "ShowUnitTestMenuExtensionPoint.ErrorEditingUnitTest.Message", unitTestName));
      }
      TransUnitTestDialog dialog = new TransUnitTestDialog(spoon.getShell(), transMeta, spoon.getMetaStore(), unitTest);
      if (dialog.open()) {
        setFactory.saveElement(unitTest);
      }
    } catch(Exception exception) {
      new ErrorDialog(Spoon.getInstance().getShell(), 
          BaseMessages.getString(PKG, "ShowUnitTestMenuExtensionPoint.ErrorEditingUnitTest.Title"),
          BaseMessages.getString(PKG, "ShowUnitTestMenuExtensionPoint.ErrorEditingUnitTest.Message", unitTestName), 
          exception);
    }
  }
  
  protected void deleteUnitTest(String unitTestName) {
    MessageBox box = new MessageBox(Spoon.getInstance().getShell(), SWT.YES | SWT.NO);
    box.setText(BaseMessages.getString(PKG, "ShowUnitTestMenuExtensionPoint.YouSureToDelete.Title"));
    box.setMessage(BaseMessages.getString(PKG, "ShowUnitTestMenuExtensionPoint.YouSureToDelete.Message", unitTestName));
    int answer = box.open();
    if ((answer&SWT.YES)!=0) {
      try {
        MetaStoreFactory<TransUnitTest> factory = new MetaStoreFactory<TransUnitTest>(TransUnitTest.class, Spoon.getInstance().getMetaStore(), PentahoDefaults.NAMESPACE);
        factory.deleteElement(unitTestName);
        DataSetHelper.getInstance().detachUnitTest();
      } catch(Exception exception) {
        new ErrorDialog(Spoon.getInstance().getShell(), 
            BaseMessages.getString(PKG, "ShowUnitTestMenuExtensionPoint.ErrorDeletingUnitTest.Title"),
            BaseMessages.getString(PKG, "ShowUnitTestMenuExtensionPoint.ErrorDeletingUnitTest.Message", unitTestName), 
            exception);

      }
    }
  }
  
  protected void disableUnitTest(String unitTestName) {
    DataSetHelper.getInstance().detachUnitTest();
  }
  
  protected void switchUnitTest(TransUnitTest targetTest, TransMeta transMeta) {
    try {
      DataSetHelper.getInstance().detachUnitTest();
      DataSetHelper.getInstance().selectUnitTest(transMeta, targetTest);
    } catch (Exception exception) {
      new ErrorDialog(Spoon.getInstance().getShell(),
          BaseMessages.getString(PKG, "ShowUnitTestMenuExtensionPoint.ErrorSwitchingUnitTest.Title"),
          BaseMessages.getString(PKG, "ShowUnitTestMenuExtensionPoint.ErrorSwitchingUnitTest.Message", targetTest.getName()),
          exception);
    }
    Spoon.getInstance().refreshGraph();
  }

  private List<TransUnitTest> findUnitTests(TransMeta transMeta, DelegatingMetaStore metaStore) {
    
    MetaStoreFactory<TransUnitTest> factory = new MetaStoreFactory<TransUnitTest>(TransUnitTest.class, metaStore, PentahoDefaults.NAMESPACE);
    List<TransUnitTest> tests = new ArrayList<TransUnitTest>();
    
    try {
      List<TransUnitTest> allTests = factory.getElements();
      for (TransUnitTest test : allTests) {
        // Match the filename
        //
        if (StringUtils.isNotEmpty(transMeta.getFilename())) {
          if (transMeta.getFilename().equals(test.getTransFilename())) {
            tests.add(test);
          }
        } else {
          if (transMeta.getRepository()!=null) {
            // No filename, check the object_id ...
            //
            if (transMeta.getObjectId()!=null && transMeta.getObjectId().getId().equals(test.getTransObjectId())) {
              tests.add(test);
            } else {
              // Try the repository path..
              //
              // What is the repsository path?
              String repositoryPath = transMeta.getRepositoryDirectory().getPath() + "/" + transMeta.getName();
              if (repositoryPath.equals(test.getTransRepositoryPath())) {
                tests.add(test);
              }
            }
          }
        }
      }
      
    } catch(Exception exception) {
      new ErrorDialog(Spoon.getInstance().getShell(), 
          BaseMessages.getString(PKG, "ShowUnitTestMenuExtensionPoint.ErrorFindingUnitTestsForTransformation.Title"),
          BaseMessages.getString(PKG, "ShowUnitTestMenuExtensionPoint.ErrorFindingUnitTestsForTransformation.Message"), 
          exception);   
    }
    return tests;
  }

  /** Find the canvas of TransGraph.  For some reason it's not exposed.
   * 
   * @param transGraph
   * @return Canvas of null if it can't be found.
   */
  private Canvas findCanvas(Composite composite) {
    for (Control child : composite.getChildren()) {
      if (child instanceof Canvas) {
        return (Canvas) child;
      }
      if (child instanceof Composite) {
        Canvas look = findCanvas((Composite) child);
        if (look!=null) {
          return look;
        }
      }
    }
    
    return null;
  }

}

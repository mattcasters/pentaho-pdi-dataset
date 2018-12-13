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

package org.pentaho.di.dataset.spoon.xtpoint;

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.widgets.MessageBox;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.gui.AreaOwner;
import org.pentaho.di.core.gui.Point;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.dataset.DataSet;
import org.pentaho.di.dataset.DataSetGroup;
import org.pentaho.di.dataset.TransUnitTest;
import org.pentaho.di.dataset.spoon.DataSetHelper;
import org.pentaho.di.dataset.spoon.dialog.DataSetDialog;
import org.pentaho.di.dataset.util.DataSetConst;
import org.pentaho.di.dataset.util.FactoriesHierarchy;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.spoon.trans.TransGraph;
import org.pentaho.di.ui.spoon.trans.TransGraphExtension;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.persist.MetaStoreFactory;

import java.util.List;

@ExtensionPoint(
  extensionPointId = "TransGraphMouseDoubleClick",
  id = "DataSetsMouseDoubleClickExtensionPoint",
  description = "Open a data set when double clicked on it" )
public class DataSetsMouseDoubleClickExtensionPoint implements ExtensionPointInterface {

  @Override
  public void callExtensionPoint( LogChannelInterface log, Object object ) throws KettleException {
    if ( !( object instanceof TransGraphExtension ) ) {
      return;
    }

    TransGraphExtension transGraphExtension = (TransGraphExtension) object;
    TransGraph transGraph = transGraphExtension.getTransGraph();
    TransMeta transMeta = transGraph.getTransMeta();

    TransUnitTest unitTest = DataSetHelper.getCurrentUnitTest( transMeta );
    if ( unitTest == null ) {
      return;
    }

    MouseEvent e = transGraphExtension.getEvent();
    Point point = transGraphExtension.getPoint();

    if ( e.button == 1 || e.button == 2 ) {
      AreaOwner areaOwner = transGraphExtension.getTransGraph().getVisibleAreaOwner( point.x, point.y );
      if ( areaOwner != null && areaOwner.getAreaType() != null ) {
        // Check if this is the flask...
        //
        if ( DataSetConst.AREA_DRAWN_INPUT_DATA_SET.equals( areaOwner.getParent() ) ||
          DataSetConst.AREA_DRAWN_GOLDEN_DATA_SET.equals( areaOwner.getParent() ) ) {

          // Open the dataset double clicked on...
          //
          String dataSetName = (String) areaOwner.getOwner();
          openDataSet( dataSetName );

        }
      }
    }
  }

  private void openDataSet( String dataSetName ) {

    Spoon spoon = Spoon.getInstance();

    List<DatabaseMeta> databases = spoon.getActiveDatabases();
    IMetaStore metaStore = spoon.getMetaStore();
    try {
      FactoriesHierarchy factoriesHierarchy = new FactoriesHierarchy( metaStore, databases );
      MetaStoreFactory<DataSet> setFactory = factoriesHierarchy.getSetFactory();
      MetaStoreFactory<DataSetGroup> groupFactory = factoriesHierarchy.getGroupFactory();

      List<DataSetGroup> groups = groupFactory.getElements();
      DataSet dataSet = factoriesHierarchy.getSetFactory().loadElement( dataSetName );
      DataSetDialog dataSetDialog = new DataSetDialog( spoon.getShell(), metaStore, dataSet, groups, databases );
      while ( dataSetDialog.open() ) {
        String message = DataSetHelper.validateDataSet( dataSet, dataSetName, setFactory.getElementNames() );

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
      new ErrorDialog( spoon.getShell(), "Error", "Error editing data set '" + dataSetName + "'", e );
    }

  }

}

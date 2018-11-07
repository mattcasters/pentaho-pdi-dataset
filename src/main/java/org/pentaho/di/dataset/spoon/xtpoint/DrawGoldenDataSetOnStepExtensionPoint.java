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

import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.gui.GCInterface;
import org.pentaho.di.core.gui.Point;
import org.pentaho.di.core.gui.PrimitiveGCInterface.EColor;
import org.pentaho.di.core.gui.PrimitiveGCInterface.EFont;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.dataset.TransUnitTest;
import org.pentaho.di.dataset.TransUnitTestSetLocation;
import org.pentaho.di.dataset.spoon.DataSetHelper;
import org.pentaho.di.dataset.util.DataSetConst;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.TransPainterExtension;
import org.pentaho.di.trans.step.StepMeta;

@ExtensionPoint(
  id = "DrawGoldenDataSetOnStepExtensionPoint",
  description = "Draws a marker on top of a step if it has a golden data set defined is for it",
  extensionPointId = "TransPainterStep" )
public class DrawGoldenDataSetOnStepExtensionPoint implements ExtensionPointInterface {

  @Override
  public void callExtensionPoint( LogChannelInterface log, Object object ) throws KettleException {
    if ( !( object instanceof TransPainterExtension ) ) {
      return;
    }

    TransPainterExtension ext = (TransPainterExtension) object;
    StepMeta stepMeta = ext.stepMeta;
    TransMeta transMeta = ext.transMeta;
    TransUnitTest unitTest = DataSetHelper.getInstance().getActiveTests().get( transMeta );
    if (unitTest!=null) {
      drawGoldenSetMarker( ext, stepMeta, unitTest );
    }
  }

  protected void drawGoldenSetMarker( TransPainterExtension ext, StepMeta stepMeta, TransUnitTest unitTest ) {

    TransUnitTestSetLocation location = unitTest.findGoldenLocation( stepMeta.getName() );
    if (location==null) {
      return;
    }
    String dataSetName = Const.NVL(location.getDataSetName(), "");

    // Now we're here, draw a marker and indicate the name of the unit test
    //
    GCInterface gc = ext.gc;
    int iconsize = ext.iconsize;
    int x = ext.x1;
    int y = ext.y1;

    gc.setLineWidth( stepMeta.isSelected() ? 2 : 1 );
    gc.setForeground( EColor.CRYSTAL );
    gc.setBackground( EColor.LIGHTGRAY );
    gc.setFont( EFont.GRAPH );
    Point textExtent = gc.textExtent( dataSetName );
    textExtent.x += 6; // add a tiny bit of a margin
    textExtent.y += 6;

    // Draw it at the right hand side
    //
    int arrowSize = textExtent.y;
    Point point = new Point( x + iconsize, y + ( iconsize - textExtent.y ) / 2 );

    int[] arrow = new int[] {
      point.x, point.y + textExtent.y/2,
      point.x + arrowSize, point.y,
      point.x + textExtent.x + arrowSize, point.y,
      point.x + textExtent.x + arrowSize, point.y + textExtent.y,
      point.x + arrowSize, point.y + textExtent.y,
    };

    gc.fillPolygon( arrow );
    gc.drawPolygon( arrow );
    gc.drawText( dataSetName, point.x + arrowSize + 3, point.y + 3 );
  }

}

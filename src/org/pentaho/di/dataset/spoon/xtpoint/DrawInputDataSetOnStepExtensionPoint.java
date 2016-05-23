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
import org.pentaho.di.dataset.util.DataSetConst;
import org.pentaho.di.trans.TransPainterExtension;
import org.pentaho.di.trans.step.StepMeta;

@ExtensionPoint(
  id = "DrawInputDataSetOnStepExtensionPoint",
  description = "Draws a marker on top of a step if it has an input data set defined for it",
  extensionPointId = "TransPainterStep" )
public class DrawInputDataSetOnStepExtensionPoint implements ExtensionPointInterface {

  @Override
  public void callExtensionPoint( LogChannelInterface log, Object object ) throws KettleException {
    if ( !( object instanceof TransPainterExtension ) ) {
      return;
    }

    TransPainterExtension ext = (TransPainterExtension) object;
    StepMeta stepMeta = ext.stepMeta;
    String dataSetName = stepMeta.getAttribute( DataSetConst.ATTR_GROUP_DATASET, DataSetConst.ATTR_STEP_DATASET_INPUT );
    if ( !Const.isEmpty( dataSetName ) ) {
      drawInputDataSetMarker( ext, stepMeta, dataSetName );
    }
  }

  private void drawInputDataSetMarker( TransPainterExtension ext, StepMeta stepMeta, String dataSetName ) {
    // Now we're here, draw a marker and indicate the name of the data set name
    //
    GCInterface gc = ext.gc;
    int iconsize = ext.iconsize;
    int x = ext.x1;
    int y = ext.y1;

    gc.setLineWidth( stepMeta.isSelected() ? 2 : 1 );
    gc.setForeground( EColor.BLUE );
    gc.setBackground( EColor.LIGHTGRAY );
    gc.setFont( EFont.GRAPH );
    Point textExtent = gc.textExtent( dataSetName );
    textExtent.x += 6; // add a tiny bit of a margin
    textExtent.y += 6;

    // Draw it to the left as an arrow
    //
    int arrowSize = textExtent.y;
    Point point = new Point( x - textExtent.x - arrowSize - 2, y + ( iconsize - textExtent.y ) / 2 );

    int[] arrow = new int[] {
      point.x, point.y,
      point.x + textExtent.x, point.y,
      point.x + textExtent.x + arrowSize, point.y + textExtent.y / 2,
      point.x + textExtent.x, point.y + textExtent.y,
      point.x, point.y + textExtent.y };

    gc.fillPolygon( arrow );
    gc.drawPolygon( arrow );
    gc.drawText( dataSetName, point.x + 3, point.y + 3 );

  }

  protected void drawGoldenSetMarker( TransPainterExtension ext, StepMeta stepMeta, String testName ) {
    // Now we're here, draw a marker and indicate the name of the unit test
    //
    GCInterface gc = ext.gc;
    int iconsize = ext.iconsize;
    int x = ext.x1;
    int y = ext.y1;

    gc.setLineWidth( stepMeta.isSelected() ? 2 : 1 );
    gc.setForeground( EColor.BLUE );
    gc.setBackground( EColor.LIGHTGRAY );
    gc.setFont( EFont.GRAPH );
    Point textExtent = gc.textExtent( testName );
    textExtent.x += 6; // add a tiny bit of a margin
    textExtent.y += 6;

    // Draw it at the right hand side
    //
    int arrowSize = textExtent.y;
    Point point = new Point( x + iconsize, y + ( iconsize - textExtent.y ) / 2 );

    int[] arrow = new int[] {
      point.x, point.y,
      point.x + textExtent.x + arrowSize, point.y,
      point.x + textExtent.x + arrowSize, point.y + textExtent.y,
      point.x, point.y + textExtent.y,
      point.x + arrowSize, point.y + textExtent.y / 2
    };

    gc.fillPolygon( arrow );
    gc.drawPolygon( arrow );
    gc.drawText( testName, point.x + arrowSize + 3, point.y + 3 );
  }

}

package org.pentaho.di.dataset.spoon.xtpoint;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.gui.AreaOwner;
import org.pentaho.di.core.gui.AreaOwner.AreaType;
import org.pentaho.di.core.gui.GCInterface;
import org.pentaho.di.core.gui.Point;
import org.pentaho.di.core.gui.PrimitiveGCInterface.EColor;
import org.pentaho.di.core.gui.PrimitiveGCInterface.EFont;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.dataset.util.DataSetConst;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.TransPainter;

@ExtensionPoint(
  id = "DrawUnitTestOnTransExtentionPoint",
  description = "Indicates graphically which unit test is currently selected.",
  extensionPointId = "TransPainterEnd" )
public class DrawUnitTestOnTransExtentionPoint implements ExtensionPointInterface {

  @Override
  public void callExtensionPoint( LogChannelInterface log, Object object ) throws KettleException {
    if ( !( object instanceof TransPainter ) ) {
      return;
    }

    TransPainter painter = (TransPainter) object;
    TransMeta transMeta = painter.getTransMeta();
    String testName = transMeta.getAttribute( DataSetConst.ATTR_GROUP_DATASET, DataSetConst.ATTR_TRANS_SELECTED_UNIT_TEST_NAME);
    // System.out.println("Drawing unit test usage/editing : '"+testName+"'");
    drawUnitTestName( painter, transMeta, testName );
  }

  private void drawUnitTestName( TransPainter painter, TransMeta transMeta, String unitTestName ) {
    
    // Draw the name of the unit test in a rectangle
    //
    GCInterface gc = painter.getGc();
    int x = 5;
    int y = 5;

    gc.setLineWidth( 1 );
    gc.setForeground( EColor.BLUE );
    gc.setBackground( EColor.LIGHTGRAY );
    gc.setFont( EFont.GRAPH );
    String message;
    if (Const.isEmpty( unitTestName )){ 
      message = "Click for unit test options";
    } else {
      message = "Selected unit test: "+unitTestName;
    }
    Point textExtent = gc.textExtent( message );
    textExtent.x += 6; // add a tiny bit of a margin
    textExtent.y += 6;


    int[] enclosure = new int[] {
      x, y,
      x + textExtent.x, y,
      x + textExtent.x, y + textExtent.y,
      x, y + textExtent.y 
    };

    gc.fillPolygon( enclosure );
    gc.drawPolygon( enclosure );
    gc.drawText( message, x + 3, y + 3 );
    
    painter.getAreaOwners().add(new AreaOwner( AreaType.CUSTOM, x, y, textExtent.x, textExtent.y, painter.getOffset(), 
        DataSetConst.AREA_DRAWN_UNIT_TEST_PARENT_NAME, 
        Const.isEmpty(unitTestName)?null:unitTestName)
        );
    
  }
}

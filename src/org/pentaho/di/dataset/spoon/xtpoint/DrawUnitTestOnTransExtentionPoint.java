package org.pentaho.di.dataset.spoon.xtpoint;

import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.gui.AreaOwner;
import org.pentaho.di.core.gui.AreaOwner.AreaType;
import org.pentaho.di.core.gui.GCInterface;
import org.pentaho.di.core.gui.Point;
import org.pentaho.di.core.gui.PrimitiveGCInterface.EColor;
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
    
    GCInterface gc = painter.getGc();
    EColor color = EColor.CRYSTAL;
    
    int x=5+painter.getOffset().x;
    int y=5+painter.getOffset().y;
    int w = 36; // flask width
    int h = 40; // flask height
    int b =  1; // top rim height
    int c =  2; // top rim extra width
    int e =  4; // fluid height
    int f = 10; // fluid-to-neck height
    int a = w-2*e-2*f; // neck width
    int bx=2; // bubble size
    
    gc.setForeground(color);
    gc.setBackground(color);
    gc.setLineWidth(2);
    
    // top of test-flask
    //
    gc.fillRoundRectangle(x+w/2-a/2-c, y, a+2*c, b, c, b);
    gc.drawRoundRectangle(x+w/2-a/2-c, y, a+2*c, b, c, b);
    
    // liquid at bottom
    //
    gc.setForeground(color);
    gc.setBackground(color);
    
    int[] flask1 = new int[] {
        x, y+h, 
        x+e, y+h-e,
        x+w-e, y+h-e,
        x+w, y+h
      };
    gc.fillPolygon(flask1);
    gc.drawPolygon(flask1);
    
    gc.setForeground(color);
    gc.setBackground(EColor.BACKGROUND);
    
    // The rest of the flask
    //
    int[] flask2 = new int[] {
        x+e, y+h-e,
        x+e+f, y+h-e-f,
        x+e+f, y+b/2,
        x+w/2+a/2, y+b/2,
        x+w/2+a/2, y+h-e-f,
        x+w-e, y+h-e
    };
    gc.drawPolygon(flask2);
    
    // A few bubbles above the liquid
    //
    gc.setForeground(color);
    gc.setBackground(color);
    gc.fillRoundRectangle(x+e+ 8*f/8-bx/2, y+h-e-f+ 4*f/8-bx/2, bx, bx, bx, bx);    
    gc.fillRoundRectangle(x+e+ 6*f/8-bx/2, y+h-e-f+ 6*f/8-bx/2, bx, bx, bx, bx);    
    gc.fillRoundRectangle(x+e+14*f/8-bx/2, y+h-e-f+ 5*f/8-bx/2, bx, bx, bx, bx);    
    gc.fillRoundRectangle(x+e+11*f/8-bx/2, y+h-e-f- 0*f/8-bx/2, bx, bx, bx, bx);    

    // Let the world know where the flask is...
    //
    painter.getAreaOwners().add(new AreaOwner( AreaType.CUSTOM, x, y, w, h, painter.getOffset(), 
        DataSetConst.AREA_DRAWN_UNIT_ICON, unitTestName));

    // Write the name of the unit test to the right...
    //
    if (StringUtils.isNotEmpty(unitTestName)) {
      gc.setForeground(color);
      gc.setBackground(EColor.BACKGROUND);
      
      Point te = gc.textExtent(unitTestName);
      int tx = x+w+5;
      int ty = y+h/2-te.y/2;
      gc.drawText(unitTestName, tx, ty, true);
      painter.getAreaOwners().add(new AreaOwner( AreaType.CUSTOM, tx, ty, te.x, te.y, painter.getOffset(), 
          DataSetConst.AREA_DRAWN_UNIT_ICON, unitTestName));
    }
  }

}

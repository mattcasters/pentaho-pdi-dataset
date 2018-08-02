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

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.gui.GCInterface;
import org.pentaho.di.core.gui.PrimitiveGCInterface.EColor;
import org.pentaho.di.core.gui.PrimitiveGCInterface.EFont;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.dataset.TransTweak;
import org.pentaho.di.dataset.util.DataSetConst;
import org.pentaho.di.trans.TransPainterExtension;
import org.pentaho.di.trans.step.StepMeta;

@ExtensionPoint(
    id = "DrawTweakOnStepExtensionPoint", 
    description = "Draws a marker on top of a step if is tweaked", 
    extensionPointId = "TransPainterStep")
public class DrawTweakOnStepExtensionPoint implements ExtensionPointInterface {

  @Override
  public void callExtensionPoint(LogChannelInterface log, Object object) throws KettleException {
    if (!(object instanceof TransPainterExtension)) {
      return;
    }

    TransPainterExtension ext = (TransPainterExtension) object;
    StepMeta stepMeta = ext.stepMeta;
    String tweakDesc = stepMeta.getAttribute(DataSetConst.ATTR_GROUP_DATASET, DataSetConst.ATTR_STEP_TWEAK);
    try {
      TransTweak tweak = TransTweak.valueOf(tweakDesc);
      switch(tweak) {
      case NONE: break;
      case REMOVE_STEP: drawRemovedTweak(ext, stepMeta); break;
      case BYPASS_STEP: drawBypassedTweak(ext, stepMeta); break;
      default: break;
      }
    } catch(Exception e) {
      // Ignore
    }    
  }

  private void drawRemovedTweak(TransPainterExtension ext, StepMeta stepMeta) {
    // Now we're here, mark the step as removed: a cross over the step icon
    //
    GCInterface gc = ext.gc;
    int iconsize = ext.iconsize;
    int x = ext.x1-5;
    int y = ext.y1-5;

    gc.setLineWidth(stepMeta.isSelected() ? 4 : 3);
    gc.setForeground(EColor.CRYSTAL);
    gc.setBackground(EColor.LIGHTGRAY);
    gc.setFont(EFont.GRAPH);

    gc.drawLine(x               , y, x + iconsize / 2, y + iconsize / 2);
    gc.drawLine(x + iconsize / 2, y, x               , y + iconsize / 2);
  }

  protected void drawBypassedTweak(TransPainterExtension ext, StepMeta stepMeta) {
    // put an arrow over the step to indicate bypass
    //
    GCInterface gc = ext.gc;
    int iconsize = ext.iconsize;
    int x = ext.x1-5;
    int y = ext.y1-5;

    int aW = iconsize/2;
    int aH = 3*iconsize/8;

    gc.setForeground(EColor.CRYSTAL);
    gc.setBackground(EColor.CRYSTAL);

    //                 C\
    //                 | \
    //    A------------B  \
    //    |                D
    //    G------------F  /
    //                 | /
    //                 E/  
    //
    int[] arrow = new int[] { 
        x, y + aH/3, // A
        x + 5 * aW / 8, y + aH/3, // B
        x + 5 * aW / 8, y,  // C
        x + aW, y + aH / 2, // D
        x + 5 * aW / 8, y + aH,  // E
        x + 5 * aW / 8, y + 2*aH/3, // F
        x, y + 2*aH/3, // G        
      };
    gc.fillPolygon(arrow);
  }
}

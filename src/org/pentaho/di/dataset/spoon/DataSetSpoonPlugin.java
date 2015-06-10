package org.pentaho.di.dataset.spoon;

import org.pentaho.di.ui.spoon.SpoonLifecycleListener;
import org.pentaho.di.ui.spoon.SpoonPerspective;
import org.pentaho.di.ui.spoon.SpoonPlugin;
import org.pentaho.di.ui.spoon.SpoonPluginCategories;
import org.pentaho.di.ui.spoon.SpoonPluginInterface;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.XulException;

@SpoonPlugin( id = "DataSet", image = "" )
@SpoonPluginCategories( { "spoon", "trans-graph" } )
public class DataSetSpoonPlugin implements SpoonPluginInterface {

  public DataSetSpoonPlugin() {
  }

  public void applyToContainer( String category, XulDomContainer container ) throws XulException {
    container.registerClassLoader( getClass().getClassLoader() );
    if ( category.equals( "spoon" ) ) {
      container.loadOverlay( "org/pentaho/di/dataset/spoon/xul/spoon_overlays.xul" );
      container.addEventHandler( DataSetHelper.getInstance() );
    }
    if ( category.equals( "trans-graph" ) ) {
      container.loadOverlay( "org/pentaho/di/dataset/spoon/xul/trans_graph_overlays.xul" );
      container.addEventHandler( DataSetHelper.getInstance() );
    }
  }

  public SpoonLifecycleListener getLifecycleListener() {
    return null;
  }

  public SpoonPerspective getPerspective() {
    return null;
  }

}

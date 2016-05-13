package org.pentaho.di.dataset.trans;

import java.util.ArrayList;
import java.util.List;

import org.pentaho.di.core.row.RowMetaInterface;

public class RowCollection {

  private RowMetaInterface rowMeta;
  private List<Object[]> rows;

  public RowCollection() {
    rowMeta = null;
    rows = new ArrayList<Object[]>();
  }

  public RowCollection( RowMetaInterface rowMeta, List<Object[]> rows ) {
    this.rowMeta = rowMeta;
    this.rows = rows;
  }
  
  public RowMetaInterface getRowMeta() {
    return rowMeta;
  }
  
  public void setRowMeta( RowMetaInterface rowMeta ) {
    this.rowMeta = rowMeta;
  }
  
  public List<Object[]> getRows() {
    return rows;
  }
  
  public void setRows( List<Object[]> rows ) {
    this.rows = rows;
  }
}
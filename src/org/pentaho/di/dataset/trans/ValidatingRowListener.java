package org.pentaho.di.dataset.trans;

import java.util.List;

import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.dataset.DataSet;
import org.pentaho.di.dataset.TransUnitTest;
import org.pentaho.di.trans.step.RowAdapter;
import org.pentaho.di.trans.step.RowListener;

public class ValidatingRowListener extends RowAdapter implements RowListener {

  private DataSet goldenDataSet;
  private TransUnitTest unitTest;
  private List<Object[]> goldenRows;
  private RowMetaInterface goldenRowMeta;
  private RowMetaInterface stepRowMeta;
  private int[] stepFieldIndices;
  private int[] goldenIndices;

  private int rowNumber = 0;

  public ValidatingRowListener( DataSet goldenDataSet, TransUnitTest unitTest, List<Object[]> goldenRows, RowMetaInterface goldenRowMeta, RowMetaInterface stepRowMeta, int[] stepFieldIndices, int[] goldenIndices ) {
    super();
    this.goldenDataSet = goldenDataSet;
    this.unitTest = unitTest;
    this.goldenRows = goldenRows;
    this.goldenRowMeta = goldenRowMeta;
    this.stepRowMeta = stepRowMeta;
    this.stepFieldIndices = stepFieldIndices;
    this.goldenIndices = goldenIndices;
  }

  public void rowWrittenEvent( RowMetaInterface rowMeta, Object[] row ) throws KettleStepException {

    if ( rowNumber + 1 > goldenRows.size() ) {
      throw new KettleStepException( "Too many rows received from step, golden data set '" + goldenDataSet.getName() + "' only has " + goldenRows.size() + " rows in it" );
    }
    Object[] goldenRow = goldenRows.get( rowNumber );
    rowNumber++;

    // Now compare the input to the golden row
    //
    for ( int i = 0; i < unitTest.getFieldMappings().size(); i++ ) {
      ValueMetaInterface stepValueMeta = stepRowMeta.getValueMeta( stepFieldIndices[i] );
      Object stepValue = row[stepFieldIndices[i]];

      ValueMetaInterface goldenValueMeta = goldenRowMeta.getValueMeta( stepFieldIndices[i] );
      Object goldenValue = goldenRow[goldenIndices[i]];
      try {
        int cmp = stepValueMeta.compare( stepValue, goldenValueMeta, goldenValue );
        if ( cmp != 0 ) {
          throw new KettleStepException( "Validation againt golden data failed for row number " + rowNumber
            + ": step value [" + stepValueMeta.getString( stepValue ) + "] does not correspond to data set value [" + goldenValueMeta.getString( goldenValue ) + "]" );
        }
      } catch ( KettleValueException e ) {
        throw new KettleStepException( "Unable to compare step data against golden data set '" + goldenDataSet.getName() + "'", e );
      }
    }
  }

  public int getRowNumber() {
    return rowNumber;
  }
}

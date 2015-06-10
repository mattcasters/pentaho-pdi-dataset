package org.pentaho.di.dataset;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.dataset.util.DataSetConst;
import org.pentaho.metastore.persist.MetaStoreAttribute;
import org.pentaho.metastore.persist.MetaStoreElementType;

/**
 * This class describes a test-case where a transformation output is verified against golden data.
 * 
 * @author matt
 *
 */
@MetaStoreElementType(
  name = "Kettle Transformation Unit Test",
  description = "This describes a golden data unit test for a transformation with defined input data sets" )
public class TransUnitTest {

  private String name;

  @MetaStoreAttribute( key = "description" )
  private String description;

  @MetaStoreAttribute(
    key = "golden_data_set_name",
    nameReference = true,
    nameListKey = DataSetConst.SET_LIST_KEY )
  private DataSet goldenDataSet;

  @MetaStoreAttribute( key = "stepname" )
  protected String stepname;

  @MetaStoreAttribute( key = "transformation_rep_object_id" )
  protected String transObjectId; // rep: by reference (1st priority)

  @MetaStoreAttribute( key = "transformation_rep_path" )
  protected String transRepositoryPath; // rep: by name (2nd priority)

  @MetaStoreAttribute( key = "transformation_filename" )
  protected String transFilename; // file (3rd priority)

  @MetaStoreAttribute( key = "field_mappings" )
  protected List<TransUnitTestFieldMapping> fieldMappings;

  public TransUnitTest() {
    fieldMappings = new ArrayList<TransUnitTestFieldMapping>();
  }

  public TransUnitTest( String name, String description, DataSet goldenDataSet, String stepname, String transObjectId, String transRepositoryPath, String transFilename, List<TransUnitTestFieldMapping> fieldMappings ) {
    this();
    this.name = name;
    this.description = description;
    this.goldenDataSet = goldenDataSet;
    this.stepname = stepname;
    this.transObjectId = transObjectId;
    this.transRepositoryPath = transRepositoryPath;
    this.transFilename = transFilename;
    this.fieldMappings = fieldMappings;
  }

  public String getName() {
    return name;
  }

  public void setName( String name ) {
    this.name = name;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription( String description ) {
    this.description = description;
  }

  public DataSet getGoldenDataSet() {
    return goldenDataSet;
  }

  public void setGoldenDataSet( DataSet goldenDataSet ) {
    this.goldenDataSet = goldenDataSet;
  }

  public String getStepname() {
    return stepname;
  }

  public void setStepname( String stepname ) {
    this.stepname = stepname;
  }

  public String getTransObjectId() {
    return transObjectId;
  }

  public void setTransObjectId( String transObjectId ) {
    this.transObjectId = transObjectId;
  }

  public String getTransRepositoryPath() {
    return transRepositoryPath;
  }

  public void setTransRepositoryPath( String transRepositoryPath ) {
    this.transRepositoryPath = transRepositoryPath;
  }

  public String getTransFilename() {
    return transFilename;
  }

  public void setTransFilename( String transFilename ) {
    this.transFilename = transFilename;
  }

  public List<TransUnitTestFieldMapping> getFieldMappings() {
    return fieldMappings;
  }

  public void setFieldMappings( List<TransUnitTestFieldMapping> fieldMappings ) {
    this.fieldMappings = fieldMappings;
  }

  public List<Object[]> getGoldenRows() throws KettleException {

    try {
      // Create a sorted list of all the field mappings.
      List<TransUnitTestFieldMapping> sortedMappings = new ArrayList<TransUnitTestFieldMapping>();
      sortedMappings.addAll( fieldMappings );
      Collections.sort( sortedMappings, new Comparator<TransUnitTestFieldMapping>() {
        @Override
        public int compare( TransUnitTestFieldMapping o1, TransUnitTestFieldMapping o2 ) {
          if ( ( o1 == null || o1.getSortOrder() == null ) && ( o2 == null || o2.getSortOrder() == null ) ) {
            return 0;
          }
          if ( ( o1 == null || o1.getSortOrder() == null ) && ( o2 != null && o2.getSortOrder() != null ) ) {
            return -1;
          }
          if ( ( o1 != null && o1.getSortOrder() != null ) && ( o2 == null || o2.getSortOrder() == null ) ) {
            return 1;
          }
          return o1.getSortOrder().compareTo( o2.getSortOrder() );
        }
      } );

      // Let's see which columns indexes need to be sorted...
      //
      final RowMetaInterface setFields = goldenDataSet.getSetRowMeta( false );

      final List<Integer> sortIndexes = new ArrayList<Integer>();

      for ( int i = 0; i < sortedMappings.size(); i++ ) {
        TransUnitTestFieldMapping fieldMapping = sortedMappings.get( i );
        String dataSetFieldName = fieldMapping.getDataSetFieldName();
        int index = setFields.indexOfValue( dataSetFieldName );
        if ( index < 0 ) {
          throw new KettleException( "data set field '" + dataSetFieldName + "' could not be found in golden data set '" + goldenDataSet.getName() + "'" );
        }
        if ( !Const.isEmpty( fieldMapping.getSortOrder() ) ) {
          sortIndexes.add( index );
        }
      }
      final int[] compareIndexes = new int[sortIndexes.size()];
      for ( int i = 0; i < compareIndexes.length; i++ ) {
        compareIndexes[i] = sortIndexes.get( i );
      }

      // now get all the rows from the data set and sort them.
      //
      List<Object[]> allRows = goldenDataSet.getAllRows();

      // Sort if needed...
      //
      if ( !sortIndexes.isEmpty() ) {
        Collections.sort( allRows, new Comparator<Object[]>() {
          @Override
          public int compare( Object[] o1, Object[] o2 ) {
            try {
              return setFields.compare( o1, o2, compareIndexes );
            } catch ( Exception e ) {
              throw new RuntimeException( "Error comparing 2 rows during golden data set sort", e );
            }
          }
        } );
      }

      // All done
      //
      return allRows;
    } catch ( Exception e ) {
      e.printStackTrace();
      throw new KettleException( "Unable to retrieve sorted golden row data set", e );
    }
  }
}

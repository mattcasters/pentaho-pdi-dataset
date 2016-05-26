package org.pentaho.di.dataset;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.dataset.trans.RowCollection;
import org.pentaho.di.dataset.util.FactoriesHierarchy;
import org.pentaho.metastore.api.IMetaStore;
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

  @MetaStoreAttribute( key = "transformation_rep_object_id" )
  protected String transObjectId; // rep: by reference (1st priority)

  @MetaStoreAttribute( key = "transformation_rep_path" )
  protected String transRepositoryPath; // rep: by name (2nd priority)

  @MetaStoreAttribute( key = "transformation_filename" )
  protected String transFilename; // file (3rd priority)
  
  @MetaStoreAttribute( key = "input_data_sets" )
  protected List<TransUnitTestSetLocation> inputDataSets;

  @MetaStoreAttribute( key = "golden_data_sets" )
  protected List<TransUnitTestSetLocation> goldenDataSets;

  public TransUnitTest() {
    inputDataSets = new ArrayList<TransUnitTestSetLocation>();
    goldenDataSets = new ArrayList<TransUnitTestSetLocation>();
  }

  public TransUnitTest( String name, String description, 
      String transObjectId, String transRepositoryPath, String transFilename, 
      List<TransUnitTestSetLocation> inputDataSets, 
      List<TransUnitTestSetLocation> goldenDataSets ) {
    this();
    this.name = name;
    this.description = description;
    this.transObjectId = transObjectId;
    this.transRepositoryPath = transRepositoryPath;
    this.transFilename = transFilename;
    this.inputDataSets = inputDataSets;
    this.goldenDataSets = goldenDataSets;
  }
  
  @Override
  public boolean equals( Object obj ) {
    if (obj==this) {
      return true;
    }
    if (!(obj instanceof TransUnitTest)) {
      return false;
    }
    return ((TransUnitTest)obj).name.equalsIgnoreCase( name );
  }
  
  @Override
  public int hashCode() {
    return name.hashCode();
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

  public List<TransUnitTestSetLocation> getInputDataSets() {
    return inputDataSets;
  }

  public void setInputDataSets( List<TransUnitTestSetLocation> inputDataSets ) {
    this.inputDataSets = inputDataSets;
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
  
  public TransUnitTestSetLocation findGoldenLocation(String stepName) {
    for (TransUnitTestSetLocation location : goldenDataSets) {
      if (stepName.equalsIgnoreCase( location.getStepname() )) {
        return location;
      }
    }
    return null;
  }
  
  public TransUnitTestSetLocation findInputLocation(String stepName) {
    for (TransUnitTestSetLocation location : inputDataSets) {
      if (stepName.equalsIgnoreCase( location.getStepname() )) {
        return location;
      }
    }
    return null;
  }
  
  /**
   * Retrieve the golden data rows, for the specified step name, from the data sets;
   * 
   * @param hierarchy The factories to load sets with
   * @param stepName The step to check
   * @return The golden data rows 
   * 
   * @throws KettleException
   */
  public RowCollection getGoldenRows(FactoriesHierarchy hierarchy, String stepName) throws KettleException {

    try {      
      
      // Look in the golden data sets list for the mentioned step name
      //
      String goldenDataSetName = null;
      List<TransUnitTestFieldMapping> fieldMappings = null;
      for (TransUnitTestSetLocation location : goldenDataSets) {
        if (stepName.equalsIgnoreCase( location.getStepname() )) {
          goldenDataSetName = location.getDataSetName();
          fieldMappings = location.getFieldMappings();
          break;
        }
      }
      if (goldenDataSetName==null) {
        throw new KettleException("Unable to find golden data set for step '"+stepName+"'");
      }
      
      DataSet goldenDataSet = hierarchy.getSetFactory().loadElement( goldenDataSetName );
      if (goldenDataSet==null) {
        throw new KettleException("Unable to find golden data set '"+goldenDataSetName+"' for step '"+stepName+"'");
      }
      
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
      return new RowCollection(setFields, allRows);
    } catch ( Exception e ) {
      throw new KettleException( "Unable to retrieve sorted golden row data set for step '"+stepName+"'", e );
    }
  }

  public List<TransUnitTestSetLocation> getGoldenDataSets() {
    return goldenDataSets;
  }

  public void setGoldenDataSets( List<TransUnitTestSetLocation> goldenDataSets ) {
    this.goldenDataSets = goldenDataSets;
  }
}

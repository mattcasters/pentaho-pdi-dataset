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

package org.pentaho.di.dataset;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.dataset.spoon.xtpoint.RowCollection;
import org.pentaho.di.dataset.util.DataSetConst;
import org.pentaho.di.dataset.util.FactoriesHierarchy;
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

  @MetaStoreAttribute( key = "trans_test_tweaks" )
  protected List<TransUnitTestTweak> tweaks;

  @MetaStoreAttribute( key = "test_type")
  protected TestType type;
  
  @MetaStoreAttribute( key = "persist_filename")
  protected String filename;

  @MetaStoreAttribute
  protected String basePath;

  @MetaStoreAttribute( key = "database_replacements" )
  protected List<TransUnitTestDatabaseReplacement> databaseReplacements;

  @MetaStoreAttribute
  protected List<VariableValue> variableValues;

  public TransUnitTest() {
    inputDataSets = new ArrayList<TransUnitTestSetLocation>();
    goldenDataSets = new ArrayList<TransUnitTestSetLocation>();
    tweaks = new ArrayList<TransUnitTestTweak>();
    type = TestType.NONE;
    databaseReplacements = new ArrayList<TransUnitTestDatabaseReplacement>();
    variableValues = new ArrayList<>();
    basePath = DataSetConst.VARIABLE_UNIT_TESTS_BASE_PATH;
  }

  public TransUnitTest( String name, String description, 
      String transObjectId, String transRepositoryPath, String transFilename, 
      List<TransUnitTestSetLocation> inputDataSets, 
      List<TransUnitTestSetLocation> goldenDataSets,
      List<TransUnitTestTweak> tweaks,
      TestType type,
      String filename, 
      List<TransUnitTestDatabaseReplacement> databaseReplacements) {
    this();
    this.name = name;
    this.description = description;
    this.transObjectId = transObjectId;
    this.transRepositoryPath = transRepositoryPath;
    this.transFilename = transFilename;
    this.inputDataSets = inputDataSets;
    this.goldenDataSets = goldenDataSets;
    this.tweaks = tweaks;
    this.type = type;
    this.filename = filename;
    this.databaseReplacements = databaseReplacements;
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
   * Retrieve the golden data set for the specified location
   *
   * @param log the logging channel to log to
   * @param hierarchy The factories to load sets with
   * @param location the location where we want to check against golden rows
   * @return The golden data set
   *
   * @throws KettleException
   */
  public DataSet getGoldenDataSet(LogChannelInterface log, FactoriesHierarchy hierarchy, TransUnitTestSetLocation location) throws KettleException {

    String stepName = location.getStepname();
    String goldenDataSetName = location.getDataSetName();

    try {
      // Look in the golden data sets list for the mentioned step name
      //
      if (goldenDataSetName==null) {
        throw new KettleException("Unable to find golden data set for step '"+stepName+"'");
      }

      DataSet goldenDataSet = hierarchy.getSetFactory().loadElement( goldenDataSetName );
      if (goldenDataSet==null) {
        throw new KettleException("Unable to find golden data set '"+goldenDataSetName+"' for step '"+stepName+"'");
      }

      return goldenDataSet;

    } catch ( Exception e ) {
      throw new KettleException( "Unable to retrieve sorted golden row data set '"+stepName+"'", e );
    }
  }

  /** Find the first tweak for a certain step
   * @param stepname the name of the step on which a tweak is put
   * @return the first tweak for a certain step or null if nothing was found
   */
  public TransUnitTestTweak findTweak(String stepname) {
    for (TransUnitTestTweak tweak : tweaks) {
      if (tweak.getStepName()!=null && tweak.getStepName().equalsIgnoreCase(stepname)) {
        return tweak;
      }
    }
    return null;
  }

  /**
   * Remove all input and golden data sets on the step with the provided name
   * @param stepname the name of the step for which we need to clear out all input and golden data sets
   */
  public void removeInputAndGoldenDataSets(String stepname) {

    for (Iterator<TransUnitTestSetLocation> iterator = inputDataSets.iterator() ; iterator.hasNext() ; ) {
      TransUnitTestSetLocation inputLocation = iterator.next();
      if (inputLocation.getStepname().equalsIgnoreCase(stepname)) {
        iterator.remove();
      }
    }

    for (Iterator<TransUnitTestSetLocation> iterator = goldenDataSets.iterator() ; iterator.hasNext() ; ) {
      TransUnitTestSetLocation goldenLocation = iterator.next();
      if (goldenLocation.getStepname().equalsIgnoreCase(stepname)) {
        iterator.remove();
      }
    }
  }

  /**
   * Gets name
   *
   * @return value of name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name The name to set
   */
  public void setName( String name ) {
    this.name = name;
  }

  /**
   * Gets description
   *
   * @return value of description
   */
  public String getDescription() {
    return description;
  }

  /**
   * @param description The description to set
   */
  public void setDescription( String description ) {
    this.description = description;
  }

  /**
   * Gets transObjectId
   *
   * @return value of transObjectId
   */
  public String getTransObjectId() {
    return transObjectId;
  }

  /**
   * @param transObjectId The transObjectId to set
   */
  public void setTransObjectId( String transObjectId ) {
    this.transObjectId = transObjectId;
  }

  /**
   * Gets transRepositoryPath
   *
   * @return value of transRepositoryPath
   */
  public String getTransRepositoryPath() {
    return transRepositoryPath;
  }

  /**
   * @param transRepositoryPath The transRepositoryPath to set
   */
  public void setTransRepositoryPath( String transRepositoryPath ) {
    this.transRepositoryPath = transRepositoryPath;
  }

  /**
   * Gets transFilename
   *
   * @return value of transFilename
   */
  public String getTransFilename() {
    return transFilename;
  }

  /**
   * @param transFilename The transFilename to set
   */
  public void setTransFilename( String transFilename ) {
    this.transFilename = transFilename;
  }

  /**
   * Gets inputDataSets
   *
   * @return value of inputDataSets
   */
  public List<TransUnitTestSetLocation> getInputDataSets() {
    return inputDataSets;
  }

  /**
   * @param inputDataSets The inputDataSets to set
   */
  public void setInputDataSets( List<TransUnitTestSetLocation> inputDataSets ) {
    this.inputDataSets = inputDataSets;
  }

  /**
   * Gets goldenDataSets
   *
   * @return value of goldenDataSets
   */
  public List<TransUnitTestSetLocation> getGoldenDataSets() {
    return goldenDataSets;
  }

  /**
   * @param goldenDataSets The goldenDataSets to set
   */
  public void setGoldenDataSets( List<TransUnitTestSetLocation> goldenDataSets ) {
    this.goldenDataSets = goldenDataSets;
  }

  /**
   * Gets tweaks
   *
   * @return value of tweaks
   */
  public List<TransUnitTestTweak> getTweaks() {
    return tweaks;
  }

  /**
   * @param tweaks The tweaks to set
   */
  public void setTweaks( List<TransUnitTestTweak> tweaks ) {
    this.tweaks = tweaks;
  }

  /**
   * Gets type
   *
   * @return value of type
   */
  public TestType getType() {
    return type;
  }

  /**
   * @param type The type to set
   */
  public void setType( TestType type ) {
    this.type = type;
  }

  /**
   * Gets filename
   *
   * @return value of filename
   */
  public String getFilename() {
    return filename;
  }

  /**
   * @param filename The filename to set
   */
  public void setFilename( String filename ) {
    this.filename = filename;
  }

  /**
   * Gets basePath
   *
   * @return value of basePath
   */
  public String getBasePath() {
    return basePath;
  }

  /**
   * @param basePath The basePath to set
   */
  public void setBasePath( String basePath ) {
    this.basePath = basePath;
  }

  /**
   * Gets databaseReplacements
   *
   * @return value of databaseReplacements
   */
  public List<TransUnitTestDatabaseReplacement> getDatabaseReplacements() {
    return databaseReplacements;
  }

  /**
   * @param databaseReplacements The databaseReplacements to set
   */
  public void setDatabaseReplacements( List<TransUnitTestDatabaseReplacement> databaseReplacements ) {
    this.databaseReplacements = databaseReplacements;
  }

  /**
   * Gets variableValues
   *
   * @return value of variableValues
   */
  public List<VariableValue> getVariableValues() {
    return variableValues;
  }

  /**
   * @param variableValues The variableValues to set
   */
  public void setVariableValues( List<VariableValue> variableValues ) {
    this.variableValues = variableValues;
  }
}

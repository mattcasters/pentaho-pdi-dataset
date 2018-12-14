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

package org.pentaho.di.dataset.steps.exectests;

import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.dataset.TestType;
import org.pentaho.di.dataset.UnitTestResult;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.List;

@Step(
    id = "ExecuteTests",
    description = "Execute Unit Tests",
    name = "Execute Unit Tests",
    image = "ui/images/TRNEx.svg",
    categoryDescription = "Flow"
    )
public class ExecuteTestsMeta extends BaseStepMeta implements StepMetaInterface {

  public static final String TAG_TEST_NAME_INPUT_FIELD = "test_name_input_field";
  public static final String TAG_TYPE_TO_EXECUTE = "type_to_execute";
  public static final String TAG_TRANSFORMATION_NAME_FIELD = "trans_name_field";
  public static final String TAG_UNIT_TEST_NAME_FIELD = "unit_test_name_field";
  public static final String TAG_DATASET_NAME_FIELD = "data_set_name_field";
  public static final String TAG_STEP_NAME_FIELD = "step_name_field";
  public static final String TAG_ERROR_FIELD = "error_field";
  public static final String TAG_COMMENT_FIELD = "comment_field";

  private String testNameInputField;
  private TestType typeToExecute;
  private String transformationNameField;
  private String unitTestNameField;
  private String dataSetNameField;
  private String stepNameField;
  private String errorField;
  private String commentField;
  
  public ExecuteTestsMeta() {
    super();
  }
  
  @Override
  public void getFields(RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep, 
      VariableSpace space, Repository repository, IMetaStore metaStore) throws KettleStepException {
    RowMetaInterface rowMeta = UnitTestResult.getRowMeta();
    int index=0;
    rowMeta.getValueMeta(index++).setName(space.environmentSubstitute(transformationNameField));
    rowMeta.getValueMeta(index++).setName(space.environmentSubstitute(unitTestNameField));
    rowMeta.getValueMeta(index++).setName(space.environmentSubstitute(dataSetNameField));
    rowMeta.getValueMeta(index++).setName(space.environmentSubstitute(stepNameField));
    rowMeta.getValueMeta(index++).setName(space.environmentSubstitute(errorField));
    rowMeta.getValueMeta(index++).setName(space.environmentSubstitute(commentField));
    
    inputRowMeta.addRowMeta( rowMeta );    
  }

  @Override
  public String getXML() throws KettleException {
    StringBuilder xml = new StringBuilder();
    xml.append( XMLHandler.addTagValue( TAG_TEST_NAME_INPUT_FIELD, testNameInputField ) );
    xml.append( XMLHandler.addTagValue( TAG_TYPE_TO_EXECUTE, typeToExecute==null ? TestType.NONE.name() : typeToExecute.name() ) );
    xml.append( XMLHandler.addTagValue( TAG_TRANSFORMATION_NAME_FIELD, transformationNameField ) );
    xml.append( XMLHandler.addTagValue( TAG_UNIT_TEST_NAME_FIELD, unitTestNameField ) );
    xml.append( XMLHandler.addTagValue( TAG_DATASET_NAME_FIELD, dataSetNameField ) );
    xml.append( XMLHandler.addTagValue( TAG_STEP_NAME_FIELD, stepNameField ) );
    xml.append( XMLHandler.addTagValue( TAG_ERROR_FIELD, errorField ) );
    xml.append( XMLHandler.addTagValue( TAG_COMMENT_FIELD, commentField ) );
    
    return xml.toString();
  }

  @Override
  public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    try {

      testNameInputField = XMLHandler.getTagValue( stepnode, TAG_TEST_NAME_INPUT_FIELD);
      String typeDesc = XMLHandler.getTagValue( stepnode, TAG_TYPE_TO_EXECUTE );
      try {
        typeToExecute = TestType.valueOf(typeDesc);
      } catch(Exception e) {
        typeToExecute = TestType.NONE;
      }
      transformationNameField = XMLHandler.getTagValue( stepnode, TAG_TRANSFORMATION_NAME_FIELD );
      unitTestNameField = XMLHandler.getTagValue( stepnode, TAG_UNIT_TEST_NAME_FIELD );
      dataSetNameField = XMLHandler.getTagValue( stepnode, TAG_DATASET_NAME_FIELD );
      stepNameField = XMLHandler.getTagValue( stepnode, TAG_STEP_NAME_FIELD );
      errorField = XMLHandler.getTagValue( stepnode, TAG_ERROR_FIELD );
      commentField = XMLHandler.getTagValue( stepnode, TAG_COMMENT_FIELD );
      
    } catch ( Exception e ) {
      throw new KettleXMLException( "Unable to load execute test step details", e );
    }
  }
  
  @Override
  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step ) throws KettleException {
    rep.saveStepAttribute( id_transformation, id_step, TAG_UNIT_TEST_NAME_FIELD, testNameInputField );
    rep.saveStepAttribute( id_transformation, id_step, TAG_TYPE_TO_EXECUTE, typeToExecute==null ? TestType.NONE.name() : typeToExecute.name() );
    rep.saveStepAttribute( id_transformation, id_step, TAG_TRANSFORMATION_NAME_FIELD, transformationNameField );
    rep.saveStepAttribute( id_transformation, id_step, TAG_UNIT_TEST_NAME_FIELD, unitTestNameField );
    rep.saveStepAttribute( id_transformation, id_step, TAG_DATASET_NAME_FIELD, dataSetNameField );
    rep.saveStepAttribute( id_transformation, id_step, TAG_STEP_NAME_FIELD, stepNameField );
    rep.saveStepAttribute( id_transformation, id_step, TAG_ERROR_FIELD, errorField );
    rep.saveStepAttribute( id_transformation, id_step, TAG_COMMENT_FIELD, commentField );
  }

  @Override
  public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases ) throws KettleException {

    testNameInputField = rep.getStepAttributeString(id_step, TAG_TEST_NAME_INPUT_FIELD);
    try {
      String typeDesc = rep.getStepAttributeString( id_step, TAG_TYPE_TO_EXECUTE );
      try {
        typeToExecute = TestType.valueOf(typeDesc);
      } catch(Exception e) {
        typeToExecute = TestType.NONE;
      }
    } catch ( Exception e ) {
      throw new KettleXMLException( "Unable to load execute tests step details from the repository", e );
    } 
    transformationNameField = rep.getStepAttributeString(id_step, TAG_TRANSFORMATION_NAME_FIELD);
    unitTestNameField = rep.getStepAttributeString(id_step, TAG_UNIT_TEST_NAME_FIELD);
    dataSetNameField = rep.getStepAttributeString(id_step, TAG_DATASET_NAME_FIELD);
    stepNameField = rep.getStepAttributeString(id_step, TAG_STEP_NAME_FIELD);
    errorField = rep.getStepAttributeString(id_step, TAG_ERROR_FIELD);
    commentField = rep.getStepAttributeString(id_step, TAG_COMMENT_FIELD);
  }

  @Override
  public StepInterface getStep(StepMeta meta, StepDataInterface data, int copy, TransMeta transMeta, Trans trans) {
    return new ExecuteTests(meta, data, copy, transMeta, trans);
  }

  @Override
  public StepDataInterface getStepData() {
    return new ExecuteTestsData();
  }
  
  @Override
  public String getDialogClassName() {
    return ExecuteTestsDialog.class.getName();
  }

  @Override
  public void setDefault() {
    testNameInputField = null;
    transformationNameField = "transformation";
    unitTestNameField = "unittest";
    dataSetNameField = "dataset";
    stepNameField = "step";
    errorField = "error";
    commentField = "comment";
    
  }


  /**
   * Gets testNameInputField
   *
   * @return value of testNameInputField
   */
  public String getTestNameInputField() {
    return testNameInputField;
  }

  /**
   * @param testNameInputField The testNameInputField to set
   */
  public void setTestNameInputField( String testNameInputField ) {
    this.testNameInputField = testNameInputField;
  }

  /**
   * Gets typeToExecute
   *
   * @return value of typeToExecute
   */
  public TestType getTypeToExecute() {
    return typeToExecute;
  }

  /**
   * @param typeToExecute The typeToExecute to set
   */
  public void setTypeToExecute( TestType typeToExecute ) {
    this.typeToExecute = typeToExecute;
  }

  /**
   * Gets transformationNameField
   *
   * @return value of transformationNameField
   */
  public String getTransformationNameField() {
    return transformationNameField;
  }

  /**
   * @param transformationNameField The transformationNameField to set
   */
  public void setTransformationNameField( String transformationNameField ) {
    this.transformationNameField = transformationNameField;
  }

  /**
   * Gets unitTestNameField
   *
   * @return value of unitTestNameField
   */
  public String getUnitTestNameField() {
    return unitTestNameField;
  }

  /**
   * @param unitTestNameField The unitTestNameField to set
   */
  public void setUnitTestNameField( String unitTestNameField ) {
    this.unitTestNameField = unitTestNameField;
  }

  /**
   * Gets dataSetNameField
   *
   * @return value of dataSetNameField
   */
  public String getDataSetNameField() {
    return dataSetNameField;
  }

  /**
   * @param dataSetNameField The dataSetNameField to set
   */
  public void setDataSetNameField( String dataSetNameField ) {
    this.dataSetNameField = dataSetNameField;
  }

  /**
   * Gets stepNameField
   *
   * @return value of stepNameField
   */
  public String getStepNameField() {
    return stepNameField;
  }

  /**
   * @param stepNameField The stepNameField to set
   */
  public void setStepNameField( String stepNameField ) {
    this.stepNameField = stepNameField;
  }

  /**
   * Gets errorField
   *
   * @return value of errorField
   */
  public String getErrorField() {
    return errorField;
  }

  /**
   * @param errorField The errorField to set
   */
  public void setErrorField( String errorField ) {
    this.errorField = errorField;
  }

  /**
   * Gets commentField
   *
   * @return value of commentField
   */
  public String getCommentField() {
    return commentField;
  }

  /**
   * @param commentField The commentField to set
   */
  public void setCommentField( String commentField ) {
    this.commentField = commentField;
  }
}

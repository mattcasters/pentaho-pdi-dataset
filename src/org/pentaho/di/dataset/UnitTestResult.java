package org.pentaho.di.dataset;

public class UnitTestResult {
  private String transformationName;
  private String unitTestName;
  private String dataSetName;
  private String stepName;
  private boolean error;
  private String comment;

  public UnitTestResult() {
    super();
  }

  public UnitTestResult(String transformationName, String unitTestName, String dataSetName, String stepName, boolean error, String comment) {
    super();
    this.transformationName = transformationName;
    this.unitTestName = unitTestName;
    this.dataSetName = dataSetName;
    this.stepName = stepName;
    this.error = error;
    this.comment = comment;
  }

  public String getTransformationName() {
    return transformationName;
  }

  public void setTransformationName(String transformationName) {
    this.transformationName = transformationName;
  }

  public String getUnitTestName() {
    return unitTestName;
  }

  public void setUnitTestName(String unitTestName) {
    this.unitTestName = unitTestName;
  }

  public String getDataSetName() {
    return dataSetName;
  }

  public void setDataSetName(String dataSetName) {
    this.dataSetName = dataSetName;
  }

  public String getStepName() {
    return stepName;
  }

  public void setStepName(String stepName) {
    this.stepName = stepName;
  }

  public boolean isError() {
    return error;
  }

  public void setError(boolean error) {
    this.error = error;
  }

  public String getComment() {
    return comment;
  }

  public void setComment(String comment) {
    this.comment = comment;
  }

}

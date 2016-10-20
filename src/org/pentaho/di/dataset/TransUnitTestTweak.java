package org.pentaho.di.dataset;

import org.pentaho.metastore.persist.MetaStoreAttribute;

public class TransUnitTestTweak {

  @MetaStoreAttribute
  private TransTweak tweak;

  @MetaStoreAttribute
  private String stepName;

  public TransUnitTestTweak() {
    tweak = TransTweak.NONE;
  }

  public TransUnitTestTweak(TransTweak tweak, String stepName) {
    super();
    this.tweak = tweak;
    this.stepName = stepName;
  }
  
  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof TransUnitTestTweak)) {
      return false;
    }
    if (obj==this) {
      return true;
    }
    
    TransUnitTestTweak other = (TransUnitTestTweak)obj;
    
    return stepName==null ? false : stepName.equals(other.stepName);
  }
  
  @Override
  public int hashCode() {
    return stepName==null ? 0 : stepName.hashCode();
  }

  public TransTweak getTweak() {
    return tweak;
  }

  public void setTweak(TransTweak tweak) {
    this.tweak = tweak;
  }

  public String getStepName() {
    return stepName;
  }

  public void setStepName(String stepName) {
    this.stepName = stepName;
  }
}

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

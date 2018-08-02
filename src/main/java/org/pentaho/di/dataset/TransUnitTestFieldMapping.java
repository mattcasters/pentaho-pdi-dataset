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

/**
 * This class simply describes a mapping between the step fields we want to test and the data set fields you want to match with.
 * @author matt
 *
 */
public class TransUnitTestFieldMapping {

  @MetaStoreAttribute( key = "step_field" )
  private String stepFieldName;

  @MetaStoreAttribute( key = "data_set_field" )
  private String dataSetFieldName;

  public TransUnitTestFieldMapping() {
  }

  public TransUnitTestFieldMapping( String stepFieldName, String dataSetFieldName ) {
    this();
    this.stepFieldName = stepFieldName;
    this.dataSetFieldName = dataSetFieldName;
  }

  public String getStepFieldName() {
    return stepFieldName;
  }

  public void setStepFieldName( String stepFieldName ) {
    this.stepFieldName = stepFieldName;
  }

  public String getDataSetFieldName() {
    return dataSetFieldName;
  }

  public void setDataSetFieldName( String dataSetFieldName ) {
    this.dataSetFieldName = dataSetFieldName;
  }
}

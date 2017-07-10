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

package org.pentaho.di.dataset.util;

import java.util.List;

import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.dataset.DataSet;
import org.pentaho.di.dataset.DataSetGroup;
import org.pentaho.di.dataset.TransUnitTest;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.util.PentahoDefaults;

public class FactoriesHierarchy {
  private IMetaStore metaStore;
  private List<DatabaseMeta> databases;
  private MetaStoreFactory<DataSetGroup> groupFactory;
  private MetaStoreFactory<DataSet> setFactory;
  private MetaStoreFactory<TransUnitTest> testFactory;

  public FactoriesHierarchy( IMetaStore metaStore, List<DatabaseMeta> databases ) throws MetaStoreException {
    this.metaStore = metaStore;
    this.databases = databases;
    groupFactory = new MetaStoreFactory<DataSetGroup>( DataSetGroup.class, metaStore, PentahoDefaults.NAMESPACE );
    groupFactory.addNameList( DataSetConst.DATABASE_LIST_KEY, databases );
    List<DataSetGroup> groups = groupFactory.getElements();

    setFactory = new MetaStoreFactory<DataSet>( DataSet.class, metaStore, PentahoDefaults.NAMESPACE );
    setFactory.addNameList( DataSetConst.GROUP_LIST_KEY, groups );
    List<DataSet> sets = setFactory.getElements();

    setFactory.addNameFactory( DataSetConst.GROUP_LIST_KEY, groupFactory );

    testFactory = new MetaStoreFactory<TransUnitTest>( TransUnitTest.class, metaStore, PentahoDefaults.NAMESPACE );
    testFactory.addNameList( DataSetConst.SET_LIST_KEY, sets );

  }

  public IMetaStore getMetaStore() {
    return metaStore;
  }

  public void setMetaStore( IMetaStore metaStore ) {
    this.metaStore = metaStore;
  }

  public List<DatabaseMeta> getDatabases() {
    return databases;
  }

  public void setDatabases( List<DatabaseMeta> databases ) {
    this.databases = databases;
  }

  public MetaStoreFactory<DataSetGroup> getGroupFactory() {
    return groupFactory;
  }

  public void setGroupFactory( MetaStoreFactory<DataSetGroup> groupFactory ) {
    this.groupFactory = groupFactory;
  }

  public MetaStoreFactory<DataSet> getSetFactory() {
    return setFactory;
  }

  public void setSetFactory( MetaStoreFactory<DataSet> setFactory ) {
    this.setFactory = setFactory;
  }

  public MetaStoreFactory<TransUnitTest> getTestFactory() {
    return testFactory;
  }

  public void setTestFactory( MetaStoreFactory<TransUnitTest> testFactory ) {
    this.testFactory = testFactory;
  }
}

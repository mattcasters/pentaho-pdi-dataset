package org.pentaho.di.dataset.steps.exectests;

import java.util.Iterator;
import java.util.List;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.dataset.TransUnitTest;
import org.pentaho.di.dataset.util.FactoriesHierarchy;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.metastore.persist.MetaStoreFactory;

public class ExecuteTestsData extends BaseStepData implements StepDataInterface {

  public MetaStoreFactory<TransUnitTest> testFactory;
  public List<TransUnitTest> tests;
  public Iterator<TransUnitTest> testsIterator;
  public RowMetaInterface outputRowMeta;
  public FactoriesHierarchy hierarchy;

  public ExecuteTestsData() {
  }
  
}

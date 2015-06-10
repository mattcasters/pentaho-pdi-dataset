package org.pentaho.di.dataset;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;

import org.junit.Test;
import org.pentaho.di.core.KettleClientEnvironment;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.dataset.util.DataSetConst;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.stores.memory.MemoryMetaStore;

public class DataSetTest extends TestCase {

  private static final int NR_FIELDS = 10;

  public static final String NAMESPACE = "test";

  public static final String GROUP_NAME = "GroupName";
  public static final String GROUP_DESC = "GroupDescription";
  public static final String GROUP_SCHEMA = "GroupSchemaName";

  public static final String NAME = "SetName";
  public static final String DESC = "SetDescription";
  public static final String TABLE = "SetTable";

  protected IMetaStore metaStore;
  protected DatabaseMeta databaseMeta;
  protected DataSetGroup dataSetGroup;

  private DataSet dataSet;

  @Override
  protected void setUp() throws Exception {
    KettleClientEnvironment.init();
    metaStore = new MemoryMetaStore();
    databaseMeta = new DatabaseMeta( "foo", "H2", "JDBC", null, null, null, null, null );
    dataSetGroup = new DataSetGroup( GROUP_NAME, GROUP_DESC, databaseMeta, GROUP_SCHEMA );

    List<DataSetField> fields = new ArrayList<>();
    for ( int i = 0; i < NR_FIELDS; i++ ) {
      fields.add( new DataSetField( "field" + i, "column" + i, ValueMetaInterface.TYPE_STRING, 50, 0, "comment" + i ) );
    }
    dataSet = new DataSet( NAME, DESC, dataSetGroup, TABLE, fields );
  }

  @Test
  public void testConstructor() {
    assertEquals( NAME, dataSet.getName() );
    assertEquals( DESC, dataSet.getDescription() );
    assertEquals( TABLE, dataSet.getTableName() );
    assertEquals( dataSetGroup, dataSet.getGroup() );
  }

  @Test
  public void testSerialisation() throws Exception {
    MetaStoreFactory<DataSetGroup> groupFactory = new MetaStoreFactory<DataSetGroup>( DataSetGroup.class, metaStore, NAMESPACE );
    MetaStoreFactory<DataSet> setFactory = new MetaStoreFactory<DataSet>( DataSet.class, metaStore, NAMESPACE );

    // Save the group...
    //
    groupFactory.saveElement( dataSetGroup );

    // Save the DataSet...
    //
    setFactory.saveElement( dataSet );

    // Load it back up...
    //
    // First load the data set groups
    //
    groupFactory.addNameList( DataSetConst.DATABASE_LIST_KEY, Arrays.asList( databaseMeta ) );
    List<DataSetGroup> groups = groupFactory.getElements();

    // Pass this list to the set factory for reference...
    //
    setFactory.addNameList( DataSetConst.GROUP_LIST_KEY, groups );

    // Re-load the data set
    //
    DataSet verify = setFactory.loadElement( NAME );

    // Verify if everything is still the same...
    //
    assertEquals( NAME, dataSet.getName() );
    assertEquals( NAME, verify.getName() );
    assertEquals( DESC, dataSet.getDescription() );
    assertEquals( DESC, verify.getDescription() );
    assertEquals( TABLE, dataSet.getTableName() );
    assertEquals( TABLE, verify.getTableName() );
    assertEquals( dataSetGroup, dataSet.getGroup() );
    assertEquals( dataSetGroup, verify.getGroup() );
    assertEquals( NR_FIELDS, dataSet.getFields().size() );
    assertEquals( NR_FIELDS, verify.getFields().size() );

    for ( int i = 0; i < NR_FIELDS; i++ ) {
      DataSetField referenceField = dataSet.getFields().get( i );
      DataSetField verifyField = verify.getFields().get( i );
      assertEquals( referenceField, verifyField );
    }
  }

}

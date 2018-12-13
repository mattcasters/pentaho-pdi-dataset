package org.pentaho.di.dataset;

import java.util.Arrays;

import junit.framework.TestCase;

import org.pentaho.di.core.KettleClientEnvironment;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.dataset.util.DataSetConst;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.stores.memory.MemoryMetaStore;

public class DataSetGroupTest extends TestCase {

  public static final String NAMESPACE = "test";

  public static final DataSetGroupType TYPE = DataSetGroupType.Database;
  public static final String NAME = "GroupName";
  public static final String DESC = "GroupDescription";
  public static final String SCHEMA = "SchemaName";

  protected DatabaseMeta databaseMeta;
  protected IMetaStore metaStore;

  @Override
  protected void setUp() throws Exception {
    KettleClientEnvironment.init();
    databaseMeta = new DatabaseMeta( "foo", "H2", "JDBC", null, null, null, null, null );
    metaStore = new MemoryMetaStore();
  }

  public void testSerialisation() throws Exception {
    DataSetGroup group = new DataSetGroup( TYPE, NAME, DESC, databaseMeta, SCHEMA );
    MetaStoreFactory<DataSetGroup> groupFactory = new MetaStoreFactory<DataSetGroup>( DataSetGroup.class, metaStore, NAMESPACE );

    // save the group
    //
    groupFactory.saveElement( group );

    // Load the element
    //
    groupFactory.addNameList( DataSetConst.DATABASE_LIST_KEY, Arrays.asList( databaseMeta ) );
    DataSetGroup verify = groupFactory.loadElement( NAME );

    // Verify loading...
    //
    assertNotNull( verify );
    assertEquals( group.getType(), verify.getType() );
    assertEquals( group.getName(), verify.getName() );
    assertEquals( group.getDescription(), verify.getDescription() );
    assertNotNull( verify.getDatabaseMeta() );
    assertEquals( group.getDatabaseMeta(), verify.getDatabaseMeta() );
    assertEquals( group.getSchemaName(), verify.getSchemaName() );

  }
}

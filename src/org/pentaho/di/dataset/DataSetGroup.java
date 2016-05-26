package org.pentaho.di.dataset;

import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.dataset.util.DataSetConst;
import org.pentaho.metastore.persist.MetaStoreAttribute;
import org.pentaho.metastore.persist.MetaStoreElementType;

@MetaStoreElementType( name = "Kettle Data Set Group", description = "Describes a data set group, the database where the sets are stored." )
public class DataSetGroup {

  private String name;

  @MetaStoreAttribute( key = "description" )
  private String description;

  @MetaStoreAttribute( key = "database_name", nameReference = true, nameListKey = DataSetConst.DATABASE_LIST_KEY )
  private DatabaseMeta databaseMeta;

  @MetaStoreAttribute( key = "schema_name" )
  private String schemaName;

  public DataSetGroup() {
    // empty constructor for the IMetaStore factory
  }

  public String getName() {
    return name;
  }

  public DataSetGroup( String name, String description, DatabaseMeta databaseMeta, String schemaName ) {
    this();
    this.name = name;
    this.description = description;
    this.databaseMeta = databaseMeta;
    this.schemaName = schemaName;
  }

  @Override
  public boolean equals( Object obj ) {

    if ( this == obj ) {
      return true;
    }
    if ( !( obj instanceof DataSetGroup ) ) {
      return false;
    }

    DataSetGroup cmp = (DataSetGroup) obj;

    return name.equals( cmp.name );
  }
  
  @Override
  public int hashCode() {
    return name.hashCode();
  }

  public void setName( String name ) {
    this.name = name;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription( String description ) {
    this.description = description;
  }

  public DatabaseMeta getDatabaseMeta() {
    return databaseMeta;
  }

  public void setDatabaseMeta( DatabaseMeta databaseMeta ) {
    this.databaseMeta = databaseMeta;
  }

  public String getSchemaName() {
    return schemaName;
  }

  public void setSchemaName( String schemaName ) {
    this.schemaName = schemaName;
  }

}

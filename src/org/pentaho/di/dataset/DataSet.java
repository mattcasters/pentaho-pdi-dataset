package org.pentaho.di.dataset;

import java.util.ArrayList;
import java.util.List;

import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.logging.LoggingObject;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.dataset.util.DataSetConst;
import org.pentaho.metastore.persist.MetaStoreAttribute;
import org.pentaho.metastore.persist.MetaStoreElementType;

@MetaStoreElementType(
  name = "Kettle Data Set",
  description = "This defines a Kettle data set" )
public class DataSet {

  private String name;

  @MetaStoreAttribute( key = "description" )
  private String description;

  @MetaStoreAttribute( key = "group_name", nameReference = true, nameListKey = DataSetConst.GROUP_LIST_KEY )
  private DataSetGroup group;

  @MetaStoreAttribute( key = "table_name" )
  private String tableName;

  @MetaStoreAttribute( key = "dataset_fields" )
  private List<DataSetField> fields;

  public DataSet() {
    fields = new ArrayList<DataSetField>();
  }

  public DataSet( String name, String description, DataSetGroup group, String tableName, List<DataSetField> fields ) {
    this();
    this.name = name;
    this.description = description;
    this.group = group;
    this.tableName = tableName;
    this.fields = fields;
  }

  @Override
  public boolean equals( Object obj ) {

    if ( this == obj ) {
      return true;
    }
    if ( !( obj instanceof DataSet ) ) {
      return false;
    }
    DataSet cmp = (DataSet) obj;
    return name.equals( cmp );
  }

  public String getName() {
    return name;
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

  public DataSetGroup getGroup() {
    return group;
  }

  public void setGroup( DataSetGroup group ) {
    this.group = group;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName( String tableName ) {
    this.tableName = tableName;
  }

  public List<DataSetField> getFields() {
    return fields;
  }

  public void setFields( List<DataSetField> fields ) {
    this.fields = fields;
  }

  /**
   * Get standard Kettle row metadata from the defined data set fields
   * @param columnName true if you want the field names to be called after the columns, false if you prefer the field names in the result.
   * @return The row metadata
   * @throws KettlePluginException
   */
  public RowMetaInterface getSetRowMeta( boolean columnName ) throws KettlePluginException {
    RowMetaInterface rowMeta = new RowMeta();
    for ( DataSetField field : getFields() ) {
      ValueMetaInterface valueMeta = ValueMetaFactory.createValueMeta(
        columnName ? field.getColumnName() : field.getFieldName(),
        field.getType(),
        field.getLength(),
        field.getPrecision() );
      valueMeta.setComments( field.getComment() );
      rowMeta.addValueMeta( valueMeta );
    }
    return rowMeta;
  }

  public DataSetField findFieldWithName( String fieldName ) {
    for ( DataSetField field : fields ) {
      if ( field.getFieldName().equalsIgnoreCase( fieldName ) ) {
        return field;
      }
    }
    return null;
  }

  public String findColumnForField( String dataSetFieldName ) {
    DataSetField field = findFieldWithName( dataSetFieldName );
    if ( field == null ) {
      return null;
    }
    return field.getColumnName();
  }

  public List<Object[]> getAllRows() throws KettleException {
    try {
      DatabaseMeta databaseMeta = group.getDatabaseMeta();
      String schemaTable = databaseMeta.getQuotedSchemaTableCombination( group.getSchemaName(), getTableName() );
      Database database = null;
      List<Object[]> rows = null;

      try {
        database = new Database( new LoggingObject( "DataSetDialog" ), group.getDatabaseMeta() );
        database.connect();

        String sql = "SELECT ";
        for ( int i = 0; i < fields.size(); i++ ) {
          DataSetField field = fields.get( i );
          if ( i > 0 ) {
            sql += ", ";
          }
          sql += databaseMeta.quoteField( field.getColumnName() );
        }
        sql += " FROM " + schemaTable;

        rows = database.getRows( sql, 0 );

      } finally {
        if ( database != null ) {
          database.disconnect();
        }
      }

      return rows;
    } catch ( Exception e ) {
      throw new KettleException( "Unable to get all rows for data set " + name, e );
    }
  }
}

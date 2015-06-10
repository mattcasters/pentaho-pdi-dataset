package org.pentaho.di.dataset;

import org.pentaho.metastore.persist.MetaStoreAttribute;

public class DataSetField {
  @MetaStoreAttribute( key = "field_name" )
  private String fieldName;

  @MetaStoreAttribute( key = "column_name" )
  private String columnName;

  @MetaStoreAttribute( key = "field_type" )
  private int type;

  @MetaStoreAttribute( key = "field_length" )
  private int length;

  @MetaStoreAttribute( key = "field_precision" )
  private int precision;

  @MetaStoreAttribute( key = "field_comment" )
  private String comment;

  public DataSetField() {
    // Empty constructor for MetaStoreFactory.
  }

  public DataSetField( String fieldName, String columnName, int type, int length, int precision, String comment ) {
    super();
    this.fieldName = fieldName;
    this.columnName = columnName;
    this.type = type;
    this.length = length;
    this.precision = precision;
    this.comment = comment;
  }

  @Override
  public boolean equals( Object obj ) {
    if ( this == obj ) {
      return true;
    }
    if ( !( obj instanceof DataSetField ) ) {
      return false;
    }
    DataSetField cmp = (DataSetField) obj;
    return fieldName.equals( cmp.fieldName )
      && columnName.equals( cmp.columnName )
      && type == cmp.type
      && length == cmp.length
      && precision == cmp.precision
      && ( comment == null && cmp.comment == null || comment != null && comment.equals( cmp.comment ) );
  }

  public String getFieldName() {
    return fieldName;
  }

  public void setFieldName( String fieldName ) {
    this.fieldName = fieldName;
  }

  public String getColumnName() {
    return columnName;
  }

  public void setColumnName( String columnName ) {
    this.columnName = columnName;
  }

  public int getType() {
    return type;
  }

  public void setType( int type ) {
    this.type = type;
  }

  public int getLength() {
    return length;
  }

  public void setLength( int length ) {
    this.length = length;
  }

  public int getPrecision() {
    return precision;
  }

  public void setPrecision( int precision ) {
    this.precision = precision;
  }

  public String getComment() {
    return comment;
  }

  public void setComment( String comment ) {
    this.comment = comment;
  }

}

package org.pentaho.di.dataset;

import org.pentaho.metastore.persist.MetaStoreAttribute;

/**
 * This class simply replaces all occurrences of a certain database connection with another one. It allows developers to point to a test database for lookup data and database related steps like database lookup, dimension lookup and so on.
 * 
 * @author matt
 *
 */
public class TransUnitTestDatabaseReplacement {

  @MetaStoreAttribute(key = "original_connection")
  private String originalDatabaseName;
  
  @MetaStoreAttribute(key = "replacement_connection")
  private String replacementDatabaseName;

  public TransUnitTestDatabaseReplacement(String originalDatabaseName, String replacementDatabaseName) {
    this();
    this.originalDatabaseName = originalDatabaseName;
    this.replacementDatabaseName = replacementDatabaseName;
  }

  public TransUnitTestDatabaseReplacement() {
  }

  public String getOriginalDatabaseName() {
    return originalDatabaseName;
  }

  public void setOriginalDatabaseName(String originalDatabaseName) {
    this.originalDatabaseName = originalDatabaseName;
  }

  public String getReplacementDatabaseName() {
    return replacementDatabaseName;
  }

  public void setReplacementDatabaseName(String replacementDatabaseName) {
    this.replacementDatabaseName = replacementDatabaseName;
  }
}

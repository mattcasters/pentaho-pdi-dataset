package org.pentaho.di.dataset;

public enum DataSetGroupType {
  Database, CSV,
  ;


  public static String[] getNames() {
    String names[] = new String[values().length];
    for (int i=0;i<names.length;i++) {
      names[i] = values()[i].name();
    }
    return names;
  }

  public static DataSetGroupType fromName(String name) {
    for (DataSetGroupType type : values()) {
      if (type.name().equalsIgnoreCase( name )) {
        return type;
      }
    }
    return Database;
  }
}

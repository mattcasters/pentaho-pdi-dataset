package org.pentaho.di.dataset;

import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LoggingObject;
import org.pentaho.di.core.logging.LoggingObjectInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.StringUtil;

import java.util.ArrayList;
import java.util.List;

public class DataSetDatabaseGroup {

  // Simply get all rows in the data set
  //
  public static final List<Object[]> getAllRows( LogChannelInterface log, DataSetGroup group, DataSet dataSet ) throws KettleException {

    try {
      DatabaseMeta databaseMeta = group.getDatabaseMeta();
      synchronized ( databaseMeta ) {
        String schemaTable = databaseMeta.getQuotedSchemaTableCombination( group.getSchemaName(), dataSet.getTableName() );
        Database database = null;
        List<Object[]> rows = null;

        try {
          database = new Database( new LoggingObject( dataSet.getName() ), group.getDatabaseMeta() );
          database.connect();

          rows = database.getRows( "SELECT * FROM " + schemaTable, 0 );
        } finally {
          if ( database != null ) {
            database.disconnect();
          }
        }
        return rows;
      }
    } catch ( Exception e ) {
      throw new KettleException( "Unable to get all rows for database data set '" + dataSet.getName() + "'", e );
    }

  }


  /**
   * Get the rows for this data set in the format of the data set.
   *
   * @param log      the logging channel to which you can write.
   * @param location The fields to obtain in the order given
   * @return The rows for the given location
   * @throws KettleException
   */
  public static final List<Object[]> getAllRows( LogChannelInterface log, DataSetGroup group, DataSet dataSet, TransUnitTestSetLocation location ) throws KettleException {
    try {
      RowMetaInterface setRowMeta = dataSet.getSetRowMeta( false );
      DatabaseMeta databaseMeta = group.getDatabaseMeta();
      synchronized ( databaseMeta ) {
        String schemaTable = databaseMeta.getQuotedSchemaTableCombination( group.getSchemaName(), dataSet.getTableName() );
        Database database = null;
        List<Object[]> rows = null;
        List<String> sortFields = location.getFieldOrder();
        List<TransUnitTestFieldMapping> fieldMappings = location.getFieldMappings();


        // Which columns do we need for the location (input or golden)
        // See how we mapped the fields
        //
        List<String> selectedColumns = new ArrayList<>();
        for ( TransUnitTestFieldMapping fieldMapping : fieldMappings ) {
          // Which fields does the step need and get the column names for these fields.
          //
          String dataSetFieldName = fieldMapping.getDataSetFieldName();
          String column = dataSet.findColumnForField( dataSetFieldName );
          selectedColumns.add( column );
        }

        // Which columns are we sorting on (if any)
        //
        List<String> sortColumns = new ArrayList<>();
        for ( String sortField : sortFields ) {
          String sortColumn = dataSet.findColumnForField( sortField );
          if ( sortColumn == null ) {
            throw new KettleException( "Unable to find sort column with field name '" + sortField + "' (from mapping) in database data set '" + dataSet.getName() + "'" );
          }
          sortColumns.add( sortColumn );
        }

        try {
          database = new Database( new LoggingObject( "DataSetDialog" ), group.getDatabaseMeta() );
          database.connect();

          String sql = "SELECT ";
          for ( int i = 0; i < selectedColumns.size(); i++ ) {
            String column = selectedColumns.get( i );
            if ( i > 0 ) {
              sql += ", ";
            }
            sql += databaseMeta.quoteField( column );
          }
          sql += " FROM " + schemaTable;

          if ( !sortColumns.isEmpty() ) {
            sql += " ORDER BY ";
            boolean first = true;
            for ( String sortColumn : sortColumns ) {
              if ( first ) {
                first = false;
              } else {
                sql += ", ";
              }
              sql += databaseMeta.quoteField( sortColumn );
            }
          }

          if ( log.isDetailed() ) {
            log.logDetailed( "---------------------------------------------" );
            log.logDetailed( "SQL = " + sql );
            log.logDetailed( "---------------------------------------------" );
          }
          rows = database.getRows( sql, 0 );

          // Now, our work is not done...
          // We will probably have data conversation issues so let's handle this...
          //
          RowMetaInterface dbRowMeta = database.getReturnRowMeta();
          if ( log.isDetailed() ) {
            log.logDetailed( "DB RowMeta = " + dbRowMeta.toStringMeta() );
            log.logDetailed( "---------------------------------------------" );
          }

          // Correct data types if needed
          //
          RowMetaInterface requestedRowMeta = dataSet.getMappedDataSetFieldsRowMeta( location );
          for ( int i = 0; i < dbRowMeta.size(); i++ ) {
            // In case the data in the table is a different data type for some reasons, bring it back up to spec.
            // The spec is given in getSetRowMeta()
            //
            ValueMetaInterface dataSetValueMeta = requestedRowMeta.getValueMeta( i );
            ValueMetaInterface dbValueMeta = dbRowMeta.getValueMeta( i );
            if ( dbValueMeta.getType() != dataSetValueMeta.getType() ) {
              // Convert the values in the result set...
              //
              for ( Object[] row : rows ) {
                row[ i ] = dataSetValueMeta.convertData( dbValueMeta, row[ i ] );
              }
            }
          }
        } finally {
          if ( database != null ) {
            database.disconnect();
          }
        }

        return rows;
      }
    } catch ( Exception e ) {
      throw new KettleException( "Unable to get all rows for database data set '" + dataSet.getName() + "'", e );
    }
  }


  public static final void writeDataSetData( LoggingObjectInterface loggingObject, DataSetGroup dataSetGroup, String tableName,
                                             RowMetaInterface rowMeta, List<Object[]> rows ) throws KettleException {

    Database database = new Database( loggingObject, dataSetGroup.getDatabaseMeta() );
    try {
      database.connect();

      String schemaTable = dataSetGroup.getDatabaseMeta().getQuotedSchemaTableCombination( dataSetGroup.getSchemaName(), tableName );

      String sql;
      if ( database.checkTableExists( schemaTable ) ) {
        // Clean out old junk, allow for rollback
        //
        database.truncateTable( schemaTable );
        sql = database.getAlterTableStatement( schemaTable, rowMeta, null, false, null, true );
      } else {
        sql = database.getCreateTableStatement( schemaTable, rowMeta, null, false, null, true );
      }
      if ( !StringUtil.isEmpty( sql ) ) {
        database.execStatements( sql );
      }
      database.prepareInsert( rowMeta, schemaTable );
      for ( Object[] row : rows ) {
        database.setValuesInsert( new RowMetaAndData( rowMeta, row ) );
        database.insertRow();
      }

      database.commit();
    } finally {
      database.disconnect();
    }

  }


  public static void createTable( DataSetGroup group, String tableName, RowMetaInterface rowMeta ) throws KettleDatabaseException {

    DatabaseMeta databaseMeta = group.getDatabaseMeta();
    String schemaTable = databaseMeta.getQuotedSchemaTableCombination( group.getSchemaName(), tableName );
    Database database = null;
    try {
      database = new Database( new LoggingObject( "DataSetDialog" ), databaseMeta );
      database.connect();
      String sql;
      if ( database.checkTableExists( schemaTable ) ) {
        sql = database.getAlterTableStatement( schemaTable, rowMeta, null, false, null, true );
      } else {
        sql = database.getCreateTableStatement( schemaTable, rowMeta, null, false, null, true );
      }
      if ( !StringUtil.isEmpty( sql ) ) {
        // Simply execute the statement.
        //
        database.execStatement( sql );
      }
    } finally {
      if ( database != null ) {
        database.disconnect();
      }
    }
  }
}

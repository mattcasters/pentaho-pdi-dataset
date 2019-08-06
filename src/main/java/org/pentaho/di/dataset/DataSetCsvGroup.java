package org.pentaho.di.dataset;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LoggingObjectInterface;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.core.vfs.KettleVFS;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * The implementation of a CSV Data Set Group
 * We simply write/read the rows without a header into a file defined by the tableName in the data set
 */
public class DataSetCsvGroup {

  public static final String VARIABLE_DATASETS_BASE_PATH = "DATASETS_BASE_PATH";

  /**
   * Get the base folder for the data set group
   *
   * @param group
   * @return
   */
  private static String getDataSetFolder( DataSetGroup group ) {
    String folderName = group.getFolderName();
    if ( StringUtils.isEmpty( folderName ) ) {
      folderName = System.getProperty( VARIABLE_DATASETS_BASE_PATH );
    }
    if ( StringUtils.isEmpty( folderName ) ) {
      // Local folder
      folderName = ".";
    } else {
      // Let's not forget to replace variables as well...
      //
      VariableSpace space = Variables.getADefaultVariableSpace();
      folderName = space.environmentSubstitute( folderName );
    }

    if ( !folderName.endsWith( File.separator ) ) {
      folderName += File.separator;
    }

    return folderName;
  }

  private static void setValueFormats( RowMetaInterface rowMeta ) {
    for ( ValueMetaInterface valueMeta : rowMeta.getValueMetaList() ) {
      if ( StringUtils.isEmpty( valueMeta.getConversionMask() ) ) {
        switch ( valueMeta.getType() ) {
          case ValueMetaInterface.TYPE_INTEGER:
            valueMeta.setConversionMask( "0" );
            break;
          case ValueMetaInterface.TYPE_NUMBER:
            valueMeta.setConversionMask( "0.#" );
            break;
          case ValueMetaInterface.TYPE_DATE:
            valueMeta.setConversionMask( "yyyyMMdd-HHmmss.SSS" );
            break;
          default:
            break;
        }
      }
    }
  }

  public static String getDataSetFilename( DataSetGroup dataSetGroup, String tableName ) {
    String setFolderName = getDataSetFolder( dataSetGroup );
    setFolderName += tableName + ".csv";
    return setFolderName;
  }


  public static final List<Object[]> getAllRows( LogChannelInterface log, DataSetGroup group, DataSet dataSet ) throws KettleException {
    RowMetaInterface setRowMeta = dataSet.getSetRowMeta( true );
    setValueFormats( setRowMeta );
    String dataSetFilename = getDataSetFilename( group, dataSet.getTableName() );
    List<Object[]> rows = new ArrayList<>();
    final ValueMetaString constantValueMeta = new ValueMetaString( "constant" );

    try {
      FileObject file = KettleVFS.getFileObject( dataSetFilename );
      if ( !file.exists() ) {
        // This is fine.  We haven't put rows in yet.
        //
        return rows;
      }

      try (
        Reader reader = new InputStreamReader( new BufferedInputStream( KettleVFS.getInputStream( file ) ) );
        CSVParser csvParser = new CSVParser( reader, getCsvFormat( setRowMeta ) );
      ) {
        for ( CSVRecord csvRecord : csvParser ) {
          if ( csvRecord.getRecordNumber() > 1 ) {
            Object[] row = RowDataUtil.allocateRowData( setRowMeta.size() );
            for ( int i = 0; i < setRowMeta.size(); i++ ) {
              ValueMetaInterface valueMeta = setRowMeta.getValueMeta( i ).clone();
              constantValueMeta.setConversionMetadata( valueMeta );
              String value = csvRecord.get( i );
              row[ i ] = valueMeta.convertData( constantValueMeta, value );
            }
            rows.add( row );
          }
        }
      }
      return rows;
    } catch ( Exception e ) {
      throw new KettleException( "Unable to get all rows for CSV data set '" + dataSet.getName() + "'", e );
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

    RowMetaInterface setRowMeta = dataSet.getSetRowMeta( false );

    // The row description of the output of this step...
    //
    final RowMetaInterface outputRowMeta = dataSet.getMappedDataSetFieldsRowMeta( location );

    setValueFormats( setRowMeta );
    String dataSetFilename = getDataSetFilename( group, dataSet.getTableName() );
    List<Object[]> rows = new ArrayList<>();
    final ValueMetaString constantValueMeta = new ValueMetaString( "constant" );

    try {

      FileObject file = KettleVFS.getFileObject( dataSetFilename );
      if ( !file.exists() ) {
        // This is fine.  We haven't put rows in yet.
        //
        return rows;
      }

      List<String> sortFields = location.getFieldOrder();

      // See how we mapped the fields
      //
      List<TransUnitTestFieldMapping> fieldMappings = location.getFieldMappings();
      int[] dataSetFieldIndexes = new int[ fieldMappings.size() ];
      for ( int i = 0; i < fieldMappings.size(); i++ ) {
        TransUnitTestFieldMapping fieldMapping = fieldMappings.get( i );
        String dataSetFieldName = fieldMapping.getDataSetFieldName();
        dataSetFieldIndexes[ i ] = setRowMeta.indexOfValue( dataSetFieldName );
      }

      try (
        Reader reader = new InputStreamReader( new BufferedInputStream( KettleVFS.getInputStream( file ) ) );
        CSVParser csvParser = new CSVParser( reader, CSVFormat.DEFAULT );
      ) {
        for ( CSVRecord csvRecord : csvParser ) {
          if ( csvRecord.getRecordNumber() > 1 ) {
            Object[] row = RowDataUtil.allocateRowData( dataSetFieldIndexes.length );

            // Only get certain values...
            //
            for ( int i = 0; i < dataSetFieldIndexes.length; i++ ) {
              int index = dataSetFieldIndexes[ i ];

              ValueMetaInterface valueMeta = setRowMeta.getValueMeta( index );
              String value = csvRecord.get( index );
              row[ i ] = valueMeta.convertDataFromString( value, constantValueMeta, null, null, ValueMetaInterface.TRIM_TYPE_NONE );
            }
            rows.add( row );
          }
        }
      }

      // Which fields are we sorting on (if any)
      //
      int[] sortIndexes = new int[ sortFields.size() ];
      for ( int i = 0; i < sortIndexes.length; i++ ) {
        sortIndexes[ i ] = outputRowMeta.indexOfValue( sortFields.get( i ) );
      }

      if ( outputRowMeta.isEmpty() ) {
        log.logError( "WARNING: No field mappings selected for data set '" + dataSet.getName() + "', returning empty set of rows" );
        return new ArrayList<>();
      }

      if ( !sortFields.isEmpty() ) {

        // Sort the rows...
        //
        Collections.sort( rows, new Comparator<Object[]>() {
          @Override public int compare( Object[] o1, Object[] o2 ) {
            try {
              return outputRowMeta.compare( o1, o2, sortIndexes );
            } catch ( KettleValueException e ) {
              throw new RuntimeException( "Unable to compare 2 rows", e );
            }
          }
        } );
      }

      return rows;

    } catch (
      Exception e ) {
      throw new KettleException( "Unable to get all rows for database data set '" + dataSet.getName() + "'", e );
    }

  }


  public static final void writeDataSetData( LoggingObjectInterface loggingObject, DataSetGroup dataSetGroup, String tableName,
                                             RowMetaInterface rowMeta, List<Object[]> rows ) throws KettleException {

    String dataSetFilename = getDataSetFilename( dataSetGroup, tableName );

    RowMetaInterface setRowMeta = rowMeta.clone(); // just making sure
    setValueFormats( setRowMeta );

    OutputStream outputStream = null;
    BufferedWriter writer = null;
    CSVPrinter csvPrinter = null;
    try {

      FileObject file = KettleVFS.getFileObject( dataSetFilename );
      outputStream = KettleVFS.getOutputStream( file, false );
      writer = new BufferedWriter( new OutputStreamWriter( outputStream ) );
      CSVFormat csvFormat = getCsvFormat( rowMeta );
      csvPrinter = new CSVPrinter( writer, csvFormat );

      for ( Object[] row : rows ) {
        List<String> strings = new ArrayList<>();
        for ( int i = 0; i < setRowMeta.size(); i++ ) {
          ValueMetaInterface valueMeta = setRowMeta.getValueMeta( i );
          String string = valueMeta.getString( row[ i ] );
          strings.add( string );
        }
        csvPrinter.printRecord( strings );
      }
      csvPrinter.flush();


    } catch ( Exception e ) {
      throw new KettleException( "Unable to write data set to file '" + dataSetFilename + "'", e );
    } finally {
      try {
        if ( csvPrinter != null ) {
          csvPrinter.close();
        }
        if ( writer != null ) {
          writer.close();
        }
        if ( outputStream != null ) {
          outputStream.close();
        }
      } catch ( IOException e ) {
        throw new KettleException( "Error closing file " + dataSetFilename + " : ", e );
      }
    }
  }

  public static CSVFormat getCsvFormat( RowMetaInterface rowMeta ) {
    return CSVFormat.DEFAULT.withHeader( rowMeta.getFieldNames() ).withQuote( '\"' ).withQuoteMode( QuoteMode.MINIMAL );
  }


  public static void createTable( DataSetGroup group, String tableName, RowMetaInterface rowMeta ) throws KettleDatabaseException {

    // Not needed with files

  }
}

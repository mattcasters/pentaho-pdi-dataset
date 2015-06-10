package org.pentaho.di.dataset.steps.datasetoutput;

import java.util.List;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.dataset.DataSet;
import org.pentaho.di.dataset.DataSetGroup;
import org.pentaho.di.dataset.util.DataSetConst;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.util.PentahoDefaults;
import org.w3c.dom.Node;

public class DataSetOutputMeta extends BaseStepMeta {

  private static final String TAG_DATA_SET_NAME = "data_set_name";
  private static final String TAG_TRUNCATE = "truncate";
  private static final String TAG_BATCH = "batch";
  private static final String TAG_FIELDS = "fields";
  private static final String TAG_FIELD = "field";
  private static final String TAG_IN_FIELD = "in";
  private static final String TAG_OUT_FIELD = "out";

  private DataSet dataSet;

  private boolean truncating;

  private boolean usingBatchInserts;

  private String[] inField;
  private String[] outField;

  public DataSetOutputMeta() {
    super();
    allocate( 0 );
  }

  public void allocate( int nrFields ) {
    inField = new String[nrFields];
    outField = new String[nrFields];
  }

  @Override
  public String getXML() throws KettleException {
    StringBuilder xml = new StringBuilder();
    xml.append( XMLHandler.addTagValue( TAG_DATA_SET_NAME, dataSet == null ? null : dataSet.getName() ) );
    xml.append( XMLHandler.addTagValue( TAG_TRUNCATE, truncating ) );
    xml.append( XMLHandler.addTagValue( TAG_BATCH, usingBatchInserts ) );

    xml.append( XMLHandler.openTag( TAG_FIELDS ) );
    for ( int i = 0; i < inField.length; i++ ) {
      xml.append( XMLHandler.openTag( TAG_FIELD ) );
      xml.append( XMLHandler.addTagValue( TAG_IN_FIELD, inField[i] ) );
      xml.append( XMLHandler.addTagValue( TAG_OUT_FIELD, outField[i] ) );
      xml.append( XMLHandler.closeTag( TAG_FIELD ) );
    }
    xml.append( XMLHandler.closeTag( TAG_FIELDS ) );

    return xml.toString();
  }

  @Override
  public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    try {

      String dataSetName = XMLHandler.getTagValue( stepnode, TAG_DATA_SET_NAME );
      dataSet = loadDataSet( databases, metaStore, dataSetName );

      truncating = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, TAG_TRUNCATE ) );
      usingBatchInserts = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, TAG_BATCH ) );
      List<Node> fieldNodes = XMLHandler.getNodes( XMLHandler.getSubNode( stepnode, TAG_FIELDS ), TAG_FIELD );
      allocate( fieldNodes.size() );
      for ( int i = 0; i < fieldNodes.size(); i++ ) {
        Node fieldNode = fieldNodes.get( i );
        inField[i] = XMLHandler.getTagValue( fieldNode, TAG_IN_FIELD );
        outField[i] = XMLHandler.getTagValue( fieldNode, TAG_OUT_FIELD );
      }
    } catch ( Exception e ) {
      throw new KettleXMLException( "Unable to load data set output details", e );
    }
  }

  private DataSet loadDataSet( List<DatabaseMeta> databases, IMetaStore metaStore, String dataSetName ) throws MetaStoreException {
    DataSet set = null;
    if ( !Const.isEmpty( dataSetName ) ) {

      MetaStoreFactory<DataSetGroup> groupFactory = new MetaStoreFactory<DataSetGroup>( DataSetGroup.class, metaStore, PentahoDefaults.NAMESPACE );
      groupFactory.addNameList( DataSetConst.DATABASE_LIST_KEY, databases );
      List<DataSetGroup> groups = groupFactory.getElements();

      MetaStoreFactory<DataSet> setFactory = new MetaStoreFactory<DataSet>( DataSet.class, metaStore, PentahoDefaults.NAMESPACE );
      setFactory.addNameList( DataSetConst.GROUP_LIST_KEY, groups );

      set = setFactory.loadElement( dataSetName );
    }
    return set;
  }

  @Override
  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step ) throws KettleException {
    rep.saveStepAttribute( id_transformation, id_step, TAG_DATA_SET_NAME, dataSet == null ? null : dataSet.getName() );
    rep.saveStepAttribute( id_transformation, id_step, TAG_TRUNCATE, truncating );
    rep.saveStepAttribute( id_transformation, id_step, TAG_BATCH, usingBatchInserts );
    for ( int i = 0; i < inField.length; i++ ) {
      rep.saveStepAttribute( id_transformation, id_step, i, TAG_IN_FIELD, inField[i] );
      rep.saveStepAttribute( id_transformation, id_step, i, TAG_OUT_FIELD, outField[i] );
    }
  }

  @Override
  public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases ) throws KettleException {

    try {
      String dataSetName = rep.getStepAttributeString( id_step, TAG_DATA_SET_NAME );
      dataSet = loadDataSet( databases, metaStore, dataSetName );
      truncating = rep.getStepAttributeBoolean( id_step, TAG_TRUNCATE );
      usingBatchInserts = rep.getStepAttributeBoolean( id_step, TAG_BATCH );
      int nrFields = rep.countNrStepAttributes( id_step, TAG_IN_FIELD );
      allocate( nrFields );
      for ( int i = 0; i < nrFields; i++ ) {
        inField[i] = rep.getStepAttributeString( id_step, i, TAG_IN_FIELD );
        outField[i] = rep.getStepAttributeString( id_step, i, TAG_OUT_FIELD );
      }
    } catch ( Exception e ) {
      throw new KettleXMLException( "Unable to load data set output details from the repository", e );
    }

  }

  public DataSet getDataSet() {
    return dataSet;
  }

  public void setDataSet( DataSet dataSet ) {
    this.dataSet = dataSet;
  }

  public String[] getInField() {
    return inField;
  }

  public void setInField( String[] inField ) {
    this.inField = inField;
  }

  public String[] getOutField() {
    return outField;
  }

  public void setOutField( String[] outField ) {
    this.outField = outField;
  }

  public boolean isUsingBatchInserts() {
    return usingBatchInserts;
  }

  public void setUsingBatchInserts( boolean usingBatchInserts ) {
    this.usingBatchInserts = usingBatchInserts;
  }

  public boolean isTruncating() {
    return truncating;
  }

  public void setTruncating( boolean truncating ) {
    this.truncating = truncating;
  }
}

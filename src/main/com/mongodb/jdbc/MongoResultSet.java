// MongoResultSet.java

/**
 *      Copyright (C) 2008 10gen Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.mongodb.jdbc;

import java.io.*;
import java.net.*;
import java.sql.*;
import java.util.*;
import java.sql.Date;
import java.math.*;

import com.mongodb.*;

public class MongoResultSet implements ResultSet {

    MongoResultSet( DBCursor cursor ){
        _cursor = cursor;
        _fields.init( cursor.getKeysWanted() );
    }

    public void clearWarnings(){
        // NO-OP
    }

    public void close(){
        _closed = true;
    }

    public boolean isClosed(){
        return _closed;
    }

    //  meta data
    
    public int getConcurrency(){
        return CONCUR_READ_ONLY;
    }

    public int getType(){
        return TYPE_FORWARD_ONLY;
    }

    public void setFetchDirection(int direction){
        if ( direction == getFetchDirection() )
            return;
        throw new UnsupportedOperationException();
    }

    public int getFetchDirection(){
        return 1;
    }

    public String getCursorName(){
        return "MongoResultSet: " + _cursor.toString();
    }

    public ResultSetMetaData getMetaData(){
        return new MongoResultSetMetadata(this._fields.getAllFieldNames());
    }

    public SQLWarning getWarnings(){
        throw new UnsupportedOperationException();
    }

    public void setFetchSize(int rows){
        throw new UnsupportedOperationException();        
    }

    public int getFetchSize(){
        throw new UnsupportedOperationException();        
    }


    public Statement getStatement(){
        throw new UnsupportedOperationException();       
    }
    
    public int getHoldability(){
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    // cursor moving methods

    public boolean absolute(int row){
        throw new UnsupportedOperationException();
    }

    public void afterLast(){
        throw new UnsupportedOperationException();
    }

    public void beforeFirst(){
        throw new UnsupportedOperationException();
    }

    public boolean first(){
        throw new UnsupportedOperationException();
    }

    public int getRow(){
        return _row;
    }

    public boolean isAfterLast(){
        throw new UnsupportedOperationException();
    }
    
    public boolean isBeforeFirst(){
        throw new UnsupportedOperationException();
    }
    public boolean isFirst(){
        throw new UnsupportedOperationException();
    }
    public boolean isLast(){
        throw new UnsupportedOperationException();
    }
    public boolean last(){
        throw new UnsupportedOperationException();
    }
    public void moveToCurrentRow(){
        throw new UnsupportedOperationException();
    }
    public void moveToInsertRow(){
        throw new UnsupportedOperationException();
    }
    public boolean previous(){
        throw new UnsupportedOperationException();
    }
    public void refreshRow(){
        throw new UnsupportedOperationException();
    }
    public boolean relative(int rows){
        throw new UnsupportedOperationException();
    }
    public boolean rowDeleted(){
        throw new UnsupportedOperationException();
    }
    public boolean rowInserted(){
        throw new UnsupportedOperationException();
    }
    public boolean rowUpdated(){
        throw new UnsupportedOperationException();
    }

    // modifications

    public void insertRow(){
        throw new UnsupportedOperationException();
    }
    
    public void cancelRowUpdates(){
        throw new UnsupportedOperationException();
    }
    
    public void deleteRow(){
        throw new UnsupportedOperationException();        
    }

    public void updateRow(){
        throw new UnsupportedOperationException();
    }
    
    // field updates

    public void updateArray(int columnIndex, Array x){
        throw new UnsupportedOperationException(); }
    public void updateArray(String columnName, Array x){
        throw new UnsupportedOperationException(); }

    public void updateAsciiStream(int columnIndex, InputStream x, int length){
        throw new UnsupportedOperationException(); }
    public void updateAsciiStream(String columnName, InputStream x, int length){
        throw new UnsupportedOperationException(); }
    public void updateAsciiStream(int columnIndex, InputStream x, long length){
        throw new UnsupportedOperationException(); }
    public void updateAsciiStream(String columnName, InputStream x, long length){
        throw new UnsupportedOperationException(); }
    public void updateAsciiStream(int columnIndex, InputStream x ){
        throw new UnsupportedOperationException(); }
    public void updateAsciiStream(String columnName, InputStream x ){
        throw new UnsupportedOperationException(); }

    public void updateBigDecimal(int columnIndex, BigDecimal x){
        throw new UnsupportedOperationException(); }
    public void updateBigDecimal(String columnName, BigDecimal x){
        throw new UnsupportedOperationException(); }

    public void updateBinaryStream(int columnIndex, InputStream x, int length){
        throw new UnsupportedOperationException(); }
    public void updateBinaryStream(String columnName, InputStream x, int length){
        throw new UnsupportedOperationException(); }
    public void updateBinaryStream(int columnIndex, InputStream x, long length){
        throw new UnsupportedOperationException(); }
    public void updateBinaryStream(String columnName, InputStream x, long length){
        throw new UnsupportedOperationException(); }
    public void updateBinaryStream(int columnIndex, InputStream x ){
        throw new UnsupportedOperationException(); }
    public void updateBinaryStream(String columnName, InputStream x ){
        throw new UnsupportedOperationException(); }

    public void updateBlob(int columnIndex, Blob x){
        throw new UnsupportedOperationException(); }
    public void updateBlob(String columnName, Blob x){
        throw new UnsupportedOperationException(); }
    public void updateBlob(int columnIndex, InputStream x){
        throw new UnsupportedOperationException(); }
    public void updateBlob(String columnName, InputStream x){
        throw new UnsupportedOperationException(); }
    public void updateBlob(int columnIndex, InputStream x, long l){
        throw new UnsupportedOperationException(); }
    public void updateBlob(String columnName, InputStream x, long l){
        throw new UnsupportedOperationException(); }

    public void updateBoolean(int columnIndex, boolean x){
        throw new UnsupportedOperationException(); }
    public void updateBoolean(String columnName, boolean x){
        throw new UnsupportedOperationException(); }

    public void updateByte(int columnIndex, byte x){
        throw new UnsupportedOperationException(); }
    public void updateByte(String columnName, byte x){
        throw new UnsupportedOperationException(); }

    public void updateBytes(int columnIndex, byte[] x){
        throw new UnsupportedOperationException(); }
    public void updateBytes(String columnName, byte[] x){
        throw new UnsupportedOperationException(); }

    public void updateCharacterStream(int columnIndex, Reader x, int length){
        throw new UnsupportedOperationException(); }
    public void updateCharacterStream(String columnName, Reader reader, int length){
        throw new UnsupportedOperationException(); }
    public void updateCharacterStream(int columnIndex, Reader x, long  length){
        throw new UnsupportedOperationException(); }
    public void updateCharacterStream(String columnName, Reader reader, long length){
        throw new UnsupportedOperationException(); }
    public void updateCharacterStream(int columnIndex, Reader x ){
        throw new UnsupportedOperationException(); }
    public void updateCharacterStream(String columnName, Reader reader ){
        throw new UnsupportedOperationException(); }

    public void updateClob(int columnIndex, Clob x){
        throw new UnsupportedOperationException(); }
    public void updateClob(String columnName, Clob x){
        throw new UnsupportedOperationException(); }
    public void updateClob(int columnIndex, Reader x){
        throw new UnsupportedOperationException(); }
    public void updateClob(String columnName, Reader x){
        throw new UnsupportedOperationException(); }
    public void updateClob(int columnIndex, Reader x, long l ){
        throw new UnsupportedOperationException(); }
    public void updateClob(String columnName, Reader x, long l ){
        throw new UnsupportedOperationException(); }

    public void updateDate(int columnIndex, Date x){
        throw new UnsupportedOperationException(); }
    public void updateDate(String columnName, Date x){
        throw new UnsupportedOperationException(); }

    public void updateDouble(int columnIndex, double x){
        throw new UnsupportedOperationException(); }
    public void updateDouble(String columnName, double x){
        throw new UnsupportedOperationException(); }

    public void updateFloat(int columnIndex, float x){
        throw new UnsupportedOperationException(); }
    public void updateFloat(String columnName, float x){
        throw new UnsupportedOperationException(); }

    public void updateInt(int columnIndex, int x){
        throw new UnsupportedOperationException(); }
    public void updateInt(String columnName, int x){
        throw new UnsupportedOperationException(); }

    public void updateLong(int columnIndex, long x){
        throw new UnsupportedOperationException(); }
    public void updateLong(String columnName, long x){
        throw new UnsupportedOperationException(); }

    public void updateNull(int columnIndex){
        throw new UnsupportedOperationException(); }
    public void updateNull(String columnName){
        throw new UnsupportedOperationException(); }

    public void updateObject(int columnIndex, Object x){
        throw new UnsupportedOperationException(); }
    public void updateObject(int columnIndex, Object x, int scale){
        throw new UnsupportedOperationException(); }
    public void updateObject(String columnName, Object x){
        throw new UnsupportedOperationException(); }
    public void updateObject(String columnName, Object x, int scale){
        throw new UnsupportedOperationException(); }

    public void updateRef(int columnIndex, Ref x){
        throw new UnsupportedOperationException(); }
    public void updateRef(String columnName, Ref x){
        throw new UnsupportedOperationException(); }

    public void updateRowId(int columnIndex, RowId x){
        throw new UnsupportedOperationException(); }
    public void updateRowId(String columnName, RowId x){
        throw new UnsupportedOperationException(); }

    public void updateShort(int columnIndex, short x){
        throw new UnsupportedOperationException(); }
    public void updateShort(String columnName, short x){
        throw new UnsupportedOperationException(); }

    public void updateSQLXML(int columnIndex, SQLXML xmlObject){
        throw new UnsupportedOperationException(); }
    public void updateSQLXML(String columnName, SQLXML xmlObject){
        throw new UnsupportedOperationException(); }

    public void updateString(int columnIndex, String x){
        throw new UnsupportedOperationException(); }
    public void updateString(String columnName, String x){
        throw new UnsupportedOperationException(); }

    public void updateTime(int columnIndex, Time x){
        throw new UnsupportedOperationException(); }
    public void updateTime(String columnName, Time x){
        throw new UnsupportedOperationException(); }

    public void updateTimestamp(int columnIndex, Timestamp x){
        throw new UnsupportedOperationException(); }
    public void updateTimestamp(String columnName, Timestamp x){
        throw new UnsupportedOperationException(); }

    // accessors
    public Array getArray(int i){
        return getArray( _find( i ) );
    }
    public Array getArray(String colName){
        throw new UnsupportedOperationException();
    }
    
    public InputStream getAsciiStream(int columnIndex){
        return getAsciiStream( _find( columnIndex ) );
    }
    public InputStream getAsciiStream(String columnName){
        throw new UnsupportedOperationException();
    }

    public BigDecimal getBigDecimal(int columnIndex){
        return getBigDecimal( _find( columnIndex ) );
    }
    public BigDecimal getBigDecimal(int columnIndex, int scale){
        return getBigDecimal( _find( columnIndex ) , scale );
    }
    public BigDecimal getBigDecimal(String columnName){
        throw new UnsupportedOperationException();
    }
    public BigDecimal getBigDecimal(String columnName, int scale){
        throw new UnsupportedOperationException();
    }

    public InputStream getBinaryStream(int columnIndex){
        return getBinaryStream( _find( columnIndex ) );
    }
    public InputStream getBinaryStream(String columnName){
        throw new UnsupportedOperationException();
    }

    public Blob getBlob(int i){
        return getBlob( _find( i ) );
    }
    public Blob getBlob(String colName){
        throw new UnsupportedOperationException();
    }

    public boolean getBoolean(int columnIndex){
        return getBoolean( _find( columnIndex ) );
    }
    public boolean getBoolean(String columnName){
        Object x = _cur.get( columnName );
        if ( x == null )
            return false;
        return (Boolean)x;
    }

    public byte getByte(int columnIndex){
        return getByte( _find( columnIndex ) );
    }
    public byte getByte(String columnName){
        throw new UnsupportedOperationException();
    }

    public byte[] getBytes(int columnIndex){
        return getBytes( _find( columnIndex ) );
    }
    public byte[] getBytes(String columnName){
        return (byte[])_cur.get( columnName );
    }

    public Reader getCharacterStream(int columnIndex){
        return getCharacterStream( _find( columnIndex ) );
    }
    public Reader getCharacterStream(String columnName){
        throw new UnsupportedOperationException();
    }

    public Clob getClob(int i){
        return getClob( _find( i ) );
    }
    public Clob getClob(String colName){
        throw new UnsupportedOperationException();
    }

    public Date getDate(int columnIndex){
        return getDate( _find( columnIndex ) );
    }
    public Date getDate(int columnIndex, Calendar cal){
        return getDate( _find( columnIndex ) , cal );
    }
    public Date getDate(String columnName){
        return (Date)_cur.get( columnName );
    }
    public Date getDate(String columnName, Calendar cal){
        throw new UnsupportedOperationException();        
    }
    
    public double getDouble(int columnIndex){
        return getDouble( _find( columnIndex ) );
    }
    public double getDouble(String columnName){
        return _getNumber( columnName ).doubleValue();
    }
    
    public float getFloat(int columnIndex){
        return getFloat( _find( columnIndex ) );
    }
    public float getFloat(String columnName){
        return _getNumber( columnName ).floatValue();
    }
    
    public int getInt(int columnIndex){
        return getInt( _find( columnIndex ) );
    }
    public int getInt(String columnName){
        return _getNumber( columnName ).intValue();
    }
    
    public long getLong(int columnIndex){
        return getLong( _find( columnIndex ) );
    }
    public long getLong(String columnName){
        return _getNumber( columnName ).longValue();
    }

    public short getShort(int columnIndex){
        return getShort( _find( columnIndex ) );
    }
    public short getShort(String columnName){
        return _getNumber( columnName ).shortValue();
    }


    Number _getNumber( String n ){
        Number x = (Number)(_cur.get(n ) );
        if ( x == null )
            return 0;
        return x;
    }


    public Object getObject(int columnIndex){
        if ( columnIndex == 0 )
            return _cur;
        return getObject( _find( columnIndex ) );
    }
    public Object getObject(int i, Map map){
        if ( i == 0 )
            return _cur;
        return getObject( _find( i ) , map );
    }
    public Object getObject(String columnName){
        return _cur.get( columnName );
    }
    public Object getObject(String colName, Map map){
        throw new UnsupportedOperationException();
    }
        
    public Ref getRef(int i){
        return getRef( _find( i ) );
    }
    public Ref getRef(String colName){
        throw new UnsupportedOperationException();
    }

    public RowId getRowId( int i ){
        return getRowId( _find( i ) );
    }
    public RowId getRowId( String name ){
        throw new UnsupportedOperationException();
    }
        

    public SQLXML getSQLXML(int columnIndex){
        return getSQLXML( _find( columnIndex ) );
    }
    public SQLXML getSQLXML(String columnName){
        throw new UnsupportedOperationException();
    }

    public String getString(int columnIndex){
        return getString( _find( columnIndex ) );
    }
    public String getString(String columnName){
        Object x = _cur.get( columnName );
        if ( x == null )
            return null;
        return x.toString();
    }

    public Time getTime(int columnIndex){
        return getTime( _find( columnIndex ) );
    }
    public Time getTime(int columnIndex, Calendar cal){
        return getTime( _find( columnIndex ) , cal );
    }
    public Time getTime(String columnName){
        throw new UnsupportedOperationException();
    }
    public Time getTime(String columnName, Calendar cal){
        throw new UnsupportedOperationException();
    }

    public Timestamp getTimestamp(int columnIndex){
        return getTimestamp( _find( columnIndex ) );
    }
    public Timestamp getTimestamp(int columnIndex, Calendar cal){
        return getTimestamp( _find( columnIndex ) , cal );
    }
    public Timestamp getTimestamp(String columnName){
        throw new UnsupportedOperationException();
    }
    public Timestamp getTimestamp(String columnName, Calendar cal){
        throw new UnsupportedOperationException();
    }

    public InputStream getUnicodeStream(int columnIndex){
        return getUnicodeStream( _find( columnIndex ) );
    }
    public InputStream getUnicodeStream(String columnName){
        throw new UnsupportedOperationException();
    }

    public URL getURL(int columnIndex)
        throws SQLException {
        return getURL( _find( columnIndex ) );
    }
    public URL getURL(String columnName)
        throws SQLException {
        try {
            return new URL( getString( columnName ) );
        }
        catch ( java.net.MalformedURLException m ){
            throw new SQLException( "bad url [" + getString( columnName ) + "]" );
        }
    }

    // N stuff

    public String getNString(int columnIndex){
        return getNString( _find( columnIndex ) );
    }
    public String getNString(String columnName){
        throw new UnsupportedOperationException();
    }
    public NClob getNClob(int columnIndex){
        return getNClob( _find( columnIndex ) );
    }
    public NClob getNClob(String columnName){
        throw new UnsupportedOperationException();
    }
    public Reader getNCharacterStream(int columnIndex){
        return getNCharacterStream( _find( columnIndex ) );
    }
    public Reader getNCharacterStream(String columnName){
        throw new UnsupportedOperationException();
    }

    public void updateNCharacterStream(int columnIndex, Reader x){ throw new UnsupportedOperationException(); }
    public void updateNCharacterStream(int columnIndex, Reader x, long length){ throw new UnsupportedOperationException(); }
    public void updateNCharacterStream(String columnLabel, Reader reader){ throw new UnsupportedOperationException(); }
    public void updateNCharacterStream(String columnLabel, Reader reader, long length){ throw new UnsupportedOperationException(); }
    public void updateNClob(int columnIndex, NClob nClob){ throw new UnsupportedOperationException(); }
    public void updateNClob(int columnIndex, Reader reader){ throw new UnsupportedOperationException(); }
    public void updateNClob(int columnIndex, Reader reader, long length){ throw new UnsupportedOperationException(); }
    public void updateNClob(String columnLabel, NClob nClob){ throw new UnsupportedOperationException(); }
    public void updateNClob(String columnLabel, Reader reader){ throw new UnsupportedOperationException(); }
    public void updateNClob(String columnLabel, Reader reader, long length){ throw new UnsupportedOperationException(); }
    public void updateNString(int columnIndex, String nString){ throw new UnsupportedOperationException(); }
    public void updateNString(String columnLabel, String nString){ throw new UnsupportedOperationException(); }

    public boolean wasNull(){
        throw new UnsupportedOperationException();
    }

    // column <-> int mapping
    
    public int findColumn( String columnName ){
        return _fields.get( columnName );
    }
    public String _find( int i ){
        return _fields.get( i );
    }

    // random stuff

    public <T> T unwrap(Class<T> iface)
        throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isWrapperFor(Class<?> iface)
        throws SQLException {
        throw new UnsupportedOperationException();
    }

    // moving throgh cursor
    
    public boolean next(){
        if ( ! _cursor.hasNext() ){
            return false;
        }
        _cur = _cursor.next();
        return true;
    }

    // members

    final DBCursor _cursor;
    final FieldLookup _fields = new FieldLookup();
    DBObject _cur;
    int _row = 0;
    boolean _closed = false;

    class FieldLookup {

        void init( DBObject o ){
            if ( o == null )
                return;
            for ( String key : o.keySet() )
                get( key );
        }
        
        int get( String s ){
            Integer i = _strings.get(s);
            if ( i == null ){
                i = _strings.size() + 1;
                _store( i , s );
            }
            return i;
        }
        
        String get( int i ){
            String s = _ids.get(i);
            if ( s != null )
                return s;
            
            init( _cur );
            
            s = _ids.get(i);
            if ( s != null )
                return s;
            throw new IllegalArgumentException( i + " is not a valid column id" );
        }
        
        void _store( Integer i , String s ){
            _ids.put( i , s );
            _strings.put( s , i );
        }

        Collection<String> getAllFieldNames() {
            return _strings.keySet();
        }
        final Map<Integer,String> _ids = new HashMap<Integer,String>();
        final Map<String,Integer> _strings = new HashMap<String,Integer>();
    }


}

// MongoPreparedStatement.java

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

import java.math.*;
import java.io.*;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.net.*;

import com.mongodb.*;

public class MongoPreparedStatement extends MongoStatement implements PreparedStatement {

    MongoPreparedStatement( MongoConnection conn , int type, int concurrency, int holdability , String sql )
        throws MongoSQLException {
        super( conn , type , concurrency , holdability );
        _sql = sql;
        _exec = new Executor( conn._db , sql );
    }

    public void addBatch(){
        throw new UnsupportedOperationException( "batch stuff not supported" );
    }
    
    // --- metadata ---

    public ResultSetMetaData getMetaData(){
        throw new UnsupportedOperationException();
    }
    public ParameterMetaData getParameterMetaData(){
        throw new UnsupportedOperationException();
    }

    public void clearParameters(){
        throw new UnsupportedOperationException();
    }
    
    // ----- actually do
    
    public boolean execute(){
        throw new RuntimeException( "execute not done" );
    }
    
    public ResultSet executeQuery(){
        throw new RuntimeException( "executeQuery not done" );
    }
    
    public int executeUpdate()
        throws MongoSQLException {
        _exec.setParams( _params );
        return _exec.writeop();
    }

    // ---- setters -----

    public void setArray(int idx, Array x){ _setnotdone(); }
    public void setAsciiStream(int idx, InputStream x){ _setnotdone(); } 
    public void setAsciiStream(int idx, InputStream x, int length){ _setnotdone(); } 
    public void setAsciiStream(int idx, InputStream x, long length){ _setnotdone(); } 
    public void setBigDecimal(int idx, BigDecimal x){ _setnotdone(); } 
    public void setBinaryStream(int idx, InputStream x){ _setnotdone(); } 
    public void setBinaryStream(int idx, InputStream x, int length){ _setnotdone(); } 
    public void setBinaryStream(int idx, InputStream x, long length){ _setnotdone(); } 
    public void setBlob(int idx, Blob x){ _setnotdone(); } 
    public void setBlob(int idx, InputStream inputStream){ _setnotdone(); } 
    public void setBlob(int idx, InputStream inputStream, long length){ _setnotdone(); } 
    public void setBoolean(int idx, boolean x){ _setnotdone(); } 
    public void setByte(int idx, byte x){ _setnotdone(); } 
    public void setBytes(int idx, byte[] x){ _setnotdone(); } 
    public void setCharacterStream(int idx, Reader reader){ _setnotdone(); } 
    public void setCharacterStream(int idx, Reader reader, int length){ _setnotdone(); } 
    public void setCharacterStream(int idx, Reader reader, long length){ _setnotdone(); } 
    public void setClob(int idx, Clob x){ _setnotdone(); } 
    public void setClob(int idx, Reader reader){ _setnotdone(); } 
    public void setClob(int idx, Reader reader, long length){ _setnotdone(); } 
    public void setDate(int idx, Date x){ _setnotdone(); } 
    public void setDate(int idx, Date x, Calendar cal){ _setnotdone(); } 
    public void setDouble(int idx, double x){ _setnotdone(); } 
    public void setFloat(int idx, float x){ _setnotdone(); } 
    public void setInt(int idx, int x){ _set( idx , x ); } 
    public void setLong(int idx, long x){ _set( idx , x ); } 
    public void setNCharacterStream(int idx, Reader value){ _setnotdone(); } 
    public void setNCharacterStream(int idx, Reader value, long length){ _setnotdone(); } 
    public void setNClob(int idx, NClob value){ _setnotdone(); } 
    public void setNClob(int idx, Reader reader){ _setnotdone(); } 
    public void setNClob(int idx, Reader reader, long length){ _setnotdone(); } 
    public void setNString(int idx, String value){ _setnotdone(); } 
    public void setNull(int idx, int sqlType){ _setnotdone(); } 
    public void setNull(int idx, int sqlType, String typeName){ _setnotdone(); } 
    public void setObject(int idx, Object x){ _set( idx , x ); }
    public void setObject(int idx, Object x, int targetSqlType){ _setnotdone(); } 
    public void setObject(int idx, Object x, int targetSqlType, int scaleOrLength){ _setnotdone(); } 
    public void setRef(int idx, Ref x){ _setnotdone(); } 
    public void setRowId(int idx, RowId x){ _setnotdone(); } 
    public void setShort(int idx, short x){ _set( idx , x ); }
    public void setSQLXML(int idx, SQLXML xmlObject){ _setnotdone(); } 
    public void setString(int idx, String x){ _set( idx , x ); } 
    public void setTime(int idx, Time x){ _setnotdone(); } 
    public void setTime(int idx, Time x, Calendar cal){ _setnotdone(); } 
    public void setTimestamp(int idx, Timestamp x){ _setnotdone(); } 
    public void setTimestamp(int idx, Timestamp x, Calendar cal){ _setnotdone(); } 
    public void setUnicodeStream(int idx, InputStream x, int length){ _setnotdone(); } 
    public void setURL(int idx, URL x){ _setnotdone(); } 

    void _setnotdone(){
        throw new UnsupportedOperationException( "setter not done" );
    }
    
    void _set( int idx , Object o ){
        while ( _params.size() <= idx )
            _params.add( null );
        _params.set( idx , o );
    }
    
    final String _sql;
    final Executor _exec;
    List _params = new ArrayList();
}

// MongoResultSetTest.java

package com.mongodb.jdbc;

import com.mongodb.*;
import org.testng.annotations.Test;

public class MongoResultSetTest extends Base {

    @Test
    public void testbasic1(){
        DBCollection c = _db.getCollection( "rs.basic1" );
        c.drop();

        c.insert( BasicDBObjectBuilder.start( "x" , 1 ).add( "y" , "foo" ).get() );
        c.insert( BasicDBObjectBuilder.start( "x" , 2 ).add( "y" , "bar" ).get() );
        
        MongoResultSet res = new MongoResultSet( c.find().sort( new BasicDBObject( "x" , 1 ) ) );
        assertTrue( res.next() );
        assertEquals( 1 , res.getInt("x" ) );
        assertEquals( "foo" , res.getString("y" ) );
        assertTrue( res.next() );
        assertEquals( 2 , res.getInt("x" ) );
        assertEquals( "bar" , res.getString("y" ) );
        assertFalse( res.next() );
    }

    @Test
    public void testorder1(){
        DBCollection c = _db.getCollection( "rs.basic1" );
        c.drop();

        c.insert( BasicDBObjectBuilder.start( "x" , 1 ).add( "y" , "foo" ).get() );
        c.insert( BasicDBObjectBuilder.start( "x" , 2 ).add( "y" , "bar" ).get() );
        
        MongoResultSet res = new MongoResultSet( c.find( new BasicDBObject() , BasicDBObjectBuilder.start("x",1).add("y",1).get() ).sort( new BasicDBObject( "x" , 1 ) ) );
        assertTrue( res.next() );
        assertEquals( 1 , res.getInt(1) );
        assertEquals( "foo" , res.getString(2) );
        assertTrue( res.next() );
        assertEquals( 2 , res.getInt(1) );
        assertEquals( "bar" , res.getString(2) );
        assertFalse( res.next() );

        res = new MongoResultSet( c.find( new BasicDBObject() , BasicDBObjectBuilder.start("y",1).add("x",1).get() ).sort( new BasicDBObject( "x" , 1 ) ) );
        assertTrue( res.next() );
        assertEquals( 1 , res.getInt(2) );
        assertEquals( "foo" , res.getString(1) );
        assertTrue( res.next() );
        assertEquals( 2 , res.getInt(2) );
        assertEquals( "bar" , res.getString(1) );
        assertFalse( res.next() );
    }

    
}

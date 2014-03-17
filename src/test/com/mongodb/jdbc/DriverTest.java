// DriverTest.java

package com.mongodb.jdbc;

import java.sql.*;

import org.testng.annotations.Test;

public class DriverTest extends com.mongodb.util.TestCase {
    @Test
    public void test1()
        throws Exception {

        Connection c = null;
        try {
            c = DriverManager.getConnection( "mongodb://localhost/test" );
        }
        catch ( Exception e ){}

        assertNull( c );

        MongoDriver.install();
        c = DriverManager.getConnection( "mongodb://localhost/test" );
    } 

}

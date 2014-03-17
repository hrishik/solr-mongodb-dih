// first.java

import java.sql.*;

public class first {

    static void print( String name , ResultSet res )
        throws SQLException {
        System.out.println( name );
        while ( res.next() ){
            System.out.println( "\t" + res.getString( "name" ) + "\t" + res.getInt( "age" ) + "\t" + res.getObject(0) );
        }
    }

    public static void main( String args[] )
        throws SQLException , ClassNotFoundException {
        
        Class.forName( "com.mongodb.jdbc.MongoDriver" );
        
        Connection c = DriverManager.getConnection( "mongodb://localhost/exampledb" );

        Statement stmt = c.createStatement();
        
        // drop old table
        stmt.executeUpdate( "drop table people" );

        // insert some data
        stmt.executeUpdate( "insert into people ( name , age ) values ( 'eliot' , 30 )" );
        stmt.executeUpdate( "insert into people ( name , age ) values ( 'sara' , 2 )" );
        stmt.executeUpdate( "insert into people ( name , age ) values ( 'jaime' , 28 )" );
        

        // print
        print( "not sorted" , stmt.executeQuery( "select name , age from people " ) );
        print( "sorted by age" , stmt.executeQuery( "select name , age from people order by age " ) );
        print( "sorted by age desc" , stmt.executeQuery( "select name , age from people order by age desc " ) );

        // update
        stmt.executeUpdate( "update people set age=32 where name='jaime'" );
        print( "sorted by age desc" , stmt.executeQuery( "select name , age from people order by age desc " ) );

    }

} 

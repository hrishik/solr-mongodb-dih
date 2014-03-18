// first.java

import java.sql.*;

public class DataLoad {

    static void print( String name , ResultSet res )
        throws SQLException {
        System.out.println( name );
        while ( res.next() ){
            System.out.println( "\t" + res.getString( "name" ) + "\t" + res.getInt( "price" ) + "\t" + res.getObject(0) );
        }
    }

    public static void main( String args[] )
        throws SQLException , ClassNotFoundException {
        
        Class.forName( "com.mongodb.jdbc.MongoDriver" );        
        Connection c = DriverManager.getConnection( "mongodb://localhost/exampledb" );
        Statement stmt = c.createStatement();
        
        // drop old table
        stmt.executeUpdate( "drop table grocery" );

        // insert some data
        stmt.executeUpdate( "insert into grocery ( name , price ) values ( 'chilli' , 30 )" );
        stmt.executeUpdate( "insert into grocery ( name , price ) values ( 'rice' , 2 )" );
        stmt.executeUpdate( "insert into grocery ( name , price ) values ( 'wheat' , 28 )" );
       
        // print
        print( "not sorted" , stmt.executeQuery( "select name , price from grocery " ) );
        print( "sorted by price" , stmt.executeQuery( "select name , price from grocery order by price " ) );
        print( "sorted by price desc" , stmt.executeQuery( "select name , price from grocery order by price desc " ) );
        // update
        stmt.executeUpdate( "update grocery set price=32 where name='jaime'" );
 
        ResultSet res = stmt.executeQuery( "select name , price from grocery order by price desc " );
        while ( res.next() ){
            System.out.println( "\t" + res.getString( "name" ) + "\t" + res.getInt( "price" ) + "\t" + res.getObject(0) );
        }
    }

} 

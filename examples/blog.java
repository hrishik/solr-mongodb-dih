// blog.java

import java.sql.*;

import com.mongodb.jdbc.*;

public class blog {

    static void print( String name , ResultSet res )
        throws SQLException {
        System.out.println( name );
        while ( res.next() ){
            System.out.println( "\t" + res.getInt( "num" ) + "\t" + res.getString( "title" ) + "\t" + res.getObject( "tags" ) );
        }
    }

    public static void main( String args[] )
        throws SQLException , ClassNotFoundException {
        
        Class.forName( "com.mongodb.jdbc.MongoDriver" );
        
        Connection c = DriverManager.getConnection( "mongodb://localhost/exampledb" );
        MongoConnection mc = (MongoConnection)c;

        Statement st = c.createStatement();
        st.executeUpdate( "drop table blogposts" );

        PreparedStatement ps = c.prepareStatement( "insert into blogposts ( title , tags , num ) values ( ? , ? , ? )" );
        ps.setString( 1 , "first post!" );
        ps.setObject( 2 , new String[]{ "fun" , "eliot" } );
        ps.setInt( 3 , 1 );
        ps.executeUpdate();

        ps.setString( 1 , "wow - this is cool" );
        ps.setObject( 2 , new String[]{ "eliot" , "bar" } );
        ps.setInt( 3 , 2 );
        ps.executeUpdate();
        ps.close();


        System.out.println( mc.getCollection( "blogposts" ).find().toArray() );
        
        print( "num should be 1 " , st.executeQuery( "select * from blogposts where tags='fun'" ) );
        print( "num should be 2 " , st.executeQuery( "select * from blogposts where tags='bar'" ) );

        // TODO indexing

    }

} 

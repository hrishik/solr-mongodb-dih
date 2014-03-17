// Base.java

package com.mongodb.jdbc;

import com.mongodb.*;
import org.testng.annotations.Test;

public abstract class Base extends com.mongodb.util.TestCase {
    
    final Mongo _mongo;
    final DB _db;

    public Base(){
        try {
            _mongo = new Mongo();
            _db = _mongo.getDB( "jdbctest" );
        }
        catch ( Exception e ){
            throw new RuntimeException( e );
        }
    }

    
}


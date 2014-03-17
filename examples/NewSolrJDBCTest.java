import java.sql.Connection;

import java.sql.DriverManager;
import java.sql.ResultSet;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.ArrayList;
import java.util.List;

public class NewSolrJDBCTest {
    public NewSolrJDBCTest() {
        super();
    }

    public void runJDBC() throws Exception {
        Class.forName("com.mongodb.jdbc.MongoDriver");
        Connection c = DriverManager.getConnection("mongodb://localhost/exampledb");
        Statement stmt = c.createStatement();
        String query = "select name , price from grocery order by price desc ";

        stmt = c.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(10);
        stmt.setMaxRows(10);
        System.out.println("Executing SQL: " + query);
        long start = System.currentTimeMillis();

        if (stmt.execute(query)) {
            ResultSet resultSet = stmt.getResultSet();
            System.out.println("Time taken for sql :" + (System.currentTimeMillis() - start));
            List<String> colNames = readFieldNames(resultSet.getMetaData());
        }
    }

    private List<String> readFieldNames(ResultSetMetaData metaData) throws SQLException {
        List<String> colNames = new ArrayList<String>();
        int count = metaData.getColumnCount();
        for (int i = 0; i < count; i++) {
            colNames.add(metaData.getColumnLabel(i + 1));
        }
        return colNames;
    }


    public static void main(String[] args) throws Exception{
        new NewSolrJDBCTest().runJDBC();
    }
}

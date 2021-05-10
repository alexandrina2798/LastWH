import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import utils.BeforeUtils;

import javax.xml.transform.Result;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DbClient {

    @Test
    public void countTest() throws SQLException, ClassNotFoundException {
        Connection connection = new BeforeUtils().getConnection();
        BeforeUtils.createData();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select * from public.animal");

        int expectedCount = 10;
        int realCount = 0;
        while (resultSet.next()) {
            realCount = resultSet.getRow();
        }
        Assertions.assertEquals(expectedCount, realCount);
        connection.close();
    }

    @Test
    public void insertTest() throws SQLException, ClassNotFoundException {
        Connection connection = new BeforeUtils().getConnection();
        BeforeUtils.createData();
        Statement statement = connection.createStatement();
        int row = 0;
        int expectedInsertedRows = 0;
        int realInsertedRows = 0;
        while (row < 10) {
            row++;
            try {
                realInsertedRows += statement.
                        executeUpdate("INSERT INTO public.animal (id, \"name\", age, \"type\", sex, place) " +
                                "VALUES(" + row + " , 'Лишний" + row + " ', 2, 1, 1, 1);\n");

            } catch (Exception JdbcSQLIntegrityConstraintViolationException) {
                System.out.println("Ошибка подключения к базе: " + JdbcSQLIntegrityConstraintViolationException);
            }
        }

        Assertions.assertEquals(expectedInsertedRows, realInsertedRows);
        connection.close();
    }

    @Test
    public void insertNullTest() throws SQLException, ClassNotFoundException {
        Connection connection = new BeforeUtils().getConnection();
        BeforeUtils.createData();
        Statement statement = connection.createStatement();
        int row = 0;
        int expectedInsertedRows = 0;
        int realInsertedRows = 0;
        try {
            realInsertedRows += statement.
                    executeUpdate("INSERT INTO public.workman (id, \"name\", age, \"position\") " +
                            "VALUES(10214, null, 23, 1);\n");
        } catch (Exception JdbcSQLIntegrityConstraintViolationException) {
            System.out.println("Ошибка подключения к базе: " + JdbcSQLIntegrityConstraintViolationException);
        }

        Assertions.assertEquals(expectedInsertedRows, realInsertedRows);
        connection.close();
    }

    @Test
    public void oneMorePlaceTest() throws SQLException, ClassNotFoundException {
        Connection connection = new BeforeUtils().getConnection();
        BeforeUtils.createData();
        Statement statement = connection.createStatement();
        int insertedRows = 0;
        try {
            insertedRows = statement.executeUpdate("INSERT INTO public.places (id, \"row\", place_num, \"name\") " +
                    "VALUES(112, 112, 18512, 'Тестовый загон');\n");
        } catch (Exception JdbcSQLIntegrityConstraintViolationException) {
            System.out.println("Ошибка подключения к базе: " + JdbcSQLIntegrityConstraintViolationException);
        }

        ResultSet resultSet = statement.executeQuery("select * from public.places");

        int expectedCount = 6;
        int realCount = 0;
        while (resultSet.next()) {
            realCount = resultSet.getRow();
        }

        Assertions.assertEquals(1, insertedRows);
        Assertions.assertEquals(realCount, expectedCount);

        try {
            statement.executeUpdate("delete from public.places where id = 112");
        } catch (Exception JdbcSQLIntegrityConstraintViolationException) {
            System.out.println("Ошибка подключения к базе: " + JdbcSQLIntegrityConstraintViolationException);
        }


        connection.close();
    }

    @Test
    public void zooTest() throws SQLException, ClassNotFoundException {
        Connection connection = new BeforeUtils().getConnection();
        BeforeUtils.createData();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select * from public.zoo");

        ArrayList<String> expectedZooList = new ArrayList<String>();
        ArrayList<String> actualZooList = new ArrayList<String>();
        expectedZooList.add("Центральный");
        expectedZooList.add("Северный");
        expectedZooList.add("Западный");

        int expectedCount = 3;
        int realCount = 0;
        while (resultSet.next()) {
            realCount = resultSet.getRow();
            actualZooList.add(resultSet.getString("name"));
        }
        Assertions.assertEquals(expectedCount, realCount);
        Assertions.assertEquals(expectedZooList, actualZooList);


        connection.close();
    }


}

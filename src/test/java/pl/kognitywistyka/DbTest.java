package pl.kognitywistyka;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Created by pwilkin on 07.04.2022.
 */
public class DbTest {

    @BeforeAll
    public static void prepareDb() {
        try (Connection c = DriverManager.getConnection("jdbc:hsqldb:file:testdb", "SA", "")) {
            try (Statement st = c.createStatement()) {
                st.execute("CREATE TABLE GOODS (ID INT IDENTITY, NAME VARCHAR(255), COST DECIMAL(8, 2), QUANTITY INT)");
            }
        } catch (Exception e) {
            Assertions.fail(e);
        }
    }

    @AfterAll
    public static void cleanupDb() {
        try (Connection c = DriverManager.getConnection("jdbc:hsqldb:file:testdb", "SA", "")) {
            try (Statement st = c.createStatement()) {
                st.execute("DROP TABLE GOODS");
            }
        } catch (Exception e) {
            Assertions.fail(e);
        }
    }

    @Test
    public void testConnectDb() {
        try (Connection c = DriverManager.getConnection("jdbc:hsqldb:file:testdb", "SA", "")) {
            Assertions.assertFalse(c.isClosed());
        } catch (Exception e) {
            Assertions.fail(e);
        }
    }

    @Test
    public void testCreateAndDestroyTable() {
        try (Connection c = DriverManager.getConnection("jdbc:hsqldb:file:testdb", "SA", "")) {
            try (Statement st = c.createStatement()) {
                st.execute("CREATE TABLE PERSON (ID INT IDENTITY, FIRST_NAME VARCHAR(255), LAST_NAME VARCHAR(255))");
                st.execute("DROP TABLE PERSON");
            }
        } catch (Exception e) {
            Assertions.fail(e);
        }
    }

    @Test
    public void testInsertAndSelectGoods() {
        try (Connection c = DriverManager.getConnection("jdbc:hsqldb:file:testdb", "SA", "")) {
            try (PreparedStatement st = c.prepareStatement("INSERT INTO GOODS (NAME, COST, QUANTITY) VALUES (?, ?, ?)")) {
                st.setString(1, "worek ziemniakow");
                st.setDouble(2, 6.30d);
                st.setInt(3, 14);
                st.execute();

                st.setString(1, "ketchup");
                st.setDouble(2, 8.20d);
                st.setInt(3, 30);
                st.execute();

                st.setString(1, "woda z kranu");
                st.setDouble(2, 0.0d);
                st.setObject(3, null);
                st.execute();
            }
            try (PreparedStatement st = c.prepareStatement("SELECT * FROM GOODS WHERE QUANTITY IS NULL")) {
                try (ResultSet rs = st.executeQuery()) {
                    while (rs.next()) {
                        rs.getString("NAME");
                    }
                }
            }
        } catch (Exception e) {
            Assertions.fail(e);
        }
    }

}

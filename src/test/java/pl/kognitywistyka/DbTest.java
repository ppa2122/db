package pl.kognitywistyka;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Created by pwilkin on 07.04.2022.
 */
public class DbTest {

    public static class Good {
        private int id;
        private String name;
        private double cost;
        private int quantity;

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public double getCost() {
            return cost;
        }

        public void setCost(double cost) {
            this.cost = cost;
        }

        public int getQuantity() {
            return quantity;
        }

        public void setQuantity(int quantity) {
            this.quantity = quantity;
        }
    }

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

    private void prepareData(Connection c) throws SQLException {
        try (PreparedStatement st = c.prepareStatement("INSERT INTO GOODS (NAME, COST, QUANTITY) VALUES (?, ?, ?)", Statement.RETURN_GENERATED_KEYS)) {
            Good good1 = new Good();
            good1.setName("worek ziemniakow");
            good1.setCost(6.30d);
            good1.setQuantity(14);

            st.setString(1, good1.getName());
            st.setDouble(2, good1.getCost());
            st.setInt(3, good1.getQuantity());
            st.execute();

            try (ResultSet rs = st.getGeneratedKeys()) {
                rs.next();
                good1.setId(rs.getInt(1));
            }

            st.setString(1, "ketchup");
            st.setDouble(2, 8.20d);
            st.setInt(3, 30);
            st.execute();

            st.setString(1, "woda z kranu");
            st.setDouble(2, 0.0d);
            st.setObject(3, null);
            st.execute();

            st.setString(1, null);
            st.setDouble(2, 500.0d);
            st.setInt(3, 10);
            st.execute();

            try (ResultSet rs = st.getGeneratedKeys()) {
                rs.next();
                Assertions.assertEquals(good1.getId() + 3, rs.getInt(1));
            }
        }
    }

    private void deleteData(Connection c) {
        try (Statement st = c.createStatement()) {
            st.execute("DELETE FROM GOODS");
        } catch (Exception e) {
            Assertions.fail(e);
        }
    }

    @Test
    public void testInsertAndSelectGoods() {
        try (Connection c = DriverManager.getConnection("jdbc:hsqldb:file:testdb", "SA", "")) {
            try {
                prepareData(c);
                try (PreparedStatement st = c.prepareStatement("SELECT * FROM GOODS WHERE QUANTITY IS NULL")) {
                    try (ResultSet rs = st.executeQuery()) {
                        int i = 0;
                        while (rs.next()) {
                            i++;
                            String name = rs.getString("NAME");
                            Assertions.assertEquals("woda z kranu", name);
                        }
                        Assertions.assertEquals(1, i);
                    }
                }
            } finally {
                deleteData(c);
            }
        } catch (Exception e) {
            Assertions.fail(e);
        }
    }

    @Test
    public void testInsertAndMigrateGoods() {
        try (Connection c = DriverManager.getConnection("jdbc:hsqldb:file:testdb", "SA", "")) {
            try {
                prepareData(c);
                try (PreparedStatement st = c.prepareStatement("SELECT * FROM GOODS")) {
                    List<Good> goods = new ArrayList<>();
                    try (ResultSet rs = st.executeQuery()) {
                        int i = 0;
                        while (rs.next()) {
                            i++;
                            int id = rs.getInt("ID");
                            String name = rs.getString("NAME");
                            double cost = rs.getDouble("COST");
                            int quantity = rs.getInt("QUANTITY");
                            Good good = new Good();
                            good.setId(id);
                            good.setName(name);
                            good.setCost(cost);
                            good.setQuantity(quantity);
                            goods.add(good);
                        }
                        Assertions.assertEquals(4, i);
                        Assertions.assertEquals(4, goods.size());
                        Assertions.assertEquals(8.2, goods.stream().filter(x -> "ketchup".equals(x.getName())).
                                findFirst().map(Good::getCost).orElse(0.0), 0.1);
                        Good unnamedGood = goods.stream().filter(x -> x.getQuantity() == 10).
                                findFirst().orElse(null);
                        Assertions.assertNotNull(unnamedGood);
                        Assertions.assertFalse("pomidor".equals(unnamedGood.getName()));
                    }
                }
            } finally {
                deleteData(c);
            }
        } catch (Exception e) {
            Assertions.fail(e);
        }
    }

    @Test
    public void testInsertAndUpdateGoods() {
        try (Connection c = DriverManager.getConnection("jdbc:hsqldb:file:testdb", "SA", "")) {
            try {
                prepareData(c);
                try (PreparedStatement st = c.prepareStatement("UPDATE GOODS SET COST = ? WHERE NAME = ?")) {
                    st.setDouble(1, 9.50d);
                    st.setString(2, "ketchup");
                    st.execute();
                }
                try (PreparedStatement st = c.prepareStatement("SELECT * FROM GOODS WHERE NAME=?")) {
                    st.setString(1, "ketchup");
                    try (ResultSet rs = st.executeQuery()) {
                        int i = 0;
                        while (rs.next()) {
                            double cost = rs.getDouble("COST");
                            Assertions.assertEquals(9.50d, cost, 0.01);
                        }
                    }
                }
            } finally {
                deleteData(c);
            }
        } catch (Exception e) {
            Assertions.fail(e);
        }
    }

    @Test
    public void testTransactionRollback() {
        try (Connection c = DriverManager.getConnection("jdbc:hsqldb:file:testdb", "SA", "")) {
            try {
                prepareData(c);
                c.setAutoCommit(false);
                try (PreparedStatement st = c.prepareStatement("UPDATE GOODS SET COST = ? WHERE NAME = ?")) {
                    st.setDouble(1, 9.50d);
                    st.setString(2, "ketchup");
                    st.execute();
                }
                try (PreparedStatement st = c.prepareStatement("SELECT * FROM GOODS WHERE NAME=?")) {
                    st.setString(1, "ketchup");
                    try (ResultSet rs = st.executeQuery()) {
                        int i = 0;
                        while (rs.next()) {
                            double cost = rs.getDouble("COST");
                            Assertions.assertEquals(9.50d, cost, 0.01);
                        }
                    }
                }
                c.rollback();
                try (PreparedStatement st = c.prepareStatement("SELECT * FROM GOODS WHERE NAME=?")) {
                    st.setString(1, "ketchup");
                    try (ResultSet rs = st.executeQuery()) {
                        int i = 0;
                        while (rs.next()) {
                            double cost = rs.getDouble("COST");
                            Assertions.assertEquals(8.20d, cost, 0.01);
                        }
                    }
                }
            } finally {
                c.setAutoCommit(true);
                deleteData(c);
            }
        } catch (Exception e) {
            Assertions.fail(e);
        }
    }

    @Test
    public void testTransactionCommit() {
        try (Connection c = DriverManager.getConnection("jdbc:hsqldb:file:testdb", "SA", "")) {
            try {
                prepareData(c);
                c.setAutoCommit(false);
                try (PreparedStatement st = c.prepareStatement("UPDATE GOODS SET COST = ? WHERE NAME = ?")) {
                    st.setDouble(1, 9.50d);
                    st.setString(2, "ketchup");
                    st.execute();
                }
                try (PreparedStatement st = c.prepareStatement("SELECT * FROM GOODS WHERE NAME=?")) {
                    st.setString(1, "ketchup");
                    try (ResultSet rs = st.executeQuery()) {
                        int i = 0;
                        while (rs.next()) {
                            double cost = rs.getDouble("COST");
                            Assertions.assertEquals(9.50d, cost, 0.01);
                        }
                    }
                }
                c.commit();
                try (PreparedStatement st = c.prepareStatement("SELECT * FROM GOODS WHERE NAME=?")) {
                    st.setString(1, "ketchup");
                    try (ResultSet rs = st.executeQuery()) {
                        int i = 0;
                        while (rs.next()) {
                            double cost = rs.getDouble("COST");
                            Assertions.assertEquals(9.50d, cost, 0.01);
                        }
                    }
                }
            } finally {
                c.setAutoCommit(true);
                deleteData(c);
            }
        } catch (Exception e) {
            Assertions.fail(e);
        }
    }

    /*@Test
    public void testTransactionIsolation() {
        try (Connection c = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "SA", "")) {
            try {
                try (Statement cr = c.createStatement()) {
                    cr.execute("CREATE TABLE GOODS (ID INT IDENTITY, NAME VARCHAR(255), COST DECIMAL(8, 2), QUANTITY INT)");
                }
                prepareData(c);
                c.setAutoCommit(false);
                c.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
                try (PreparedStatement st = c.prepareStatement("UPDATE GOODS SET COST = ? WHERE NAME = ?")) {
                    st.setDouble(1, 9.50d);
                    st.setString(2, "ketchup");
                    st.execute();
                }
                try (PreparedStatement st = c.prepareStatement("SELECT * FROM GOODS WHERE NAME=?")) {
                    st.setString(1, "ketchup");
                    try (ResultSet rs = st.executeQuery()) {
                        while (rs.next()) {
                            double cost = rs.getDouble("COST");
                            Assertions.assertEquals(9.50d, cost, 0.01);
                        }
                    }
                }
                try (Connection c2 = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "SA", "")) {
                    try (PreparedStatement st = c2.prepareStatement("SELECT * FROM GOODS WHERE NAME=?")) {
                        st.setString(1, "ketchup");
                        try (ResultSet rs = st.executeQuery()) {
                            while (rs.next()) {
                                double cost = rs.getDouble("COST");
                                Assertions.assertEquals(8.20d, cost, 0.01);
                            }
                        }
                    }
                }
                c.commit();
            } finally {
                c.setAutoCommit(true);
                deleteData(c);
                try (Statement cr = c.createStatement()) {
                    cr.execute("DROP TABLE GOODS");
                }
            }
        } catch (Exception e) {
            Assertions.fail(e);
        }
    }*/

}

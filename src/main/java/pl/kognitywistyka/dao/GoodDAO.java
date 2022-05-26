package pl.kognitywistyka.dao;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import pl.kognitywistyka.DbManager;
import pl.kognitywistyka.model.Good;

/**
 * Created by pwilkin on 26.05.2022.
 */
public class GoodDAO {

    private final DbManager db;

    public GoodDAO(DbManager db) {
        this.db = db;
    }

    private Good getGoodFromCursor(ResultSet rs) throws SQLException {
        Good good = new Good();
        good.setCost(rs.getDouble("COST"));
        good.setId(rs.getInt("ID"));
        good.setQuantity(rs.getInt("QUANTITY"));
        good.setName(rs.getString("NAME"));
        return good;
    }

    public List<Good> retrieveGoods() {
        Connection c = db.getValidatedConnection();
        List<Good> retVal = new ArrayList<>();
        try (Statement st = c.createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT * FROM GOODS")) {
                while (rs.next()) {
                    Good good = getGoodFromCursor(rs);
                    retVal.add(good);
                }
            }
            return retVal;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public List<Good> retrieveGoodsByName(String name) {
        Connection c = db.getValidatedConnection();
        List<Good> retVal = new ArrayList<>();
        try (PreparedStatement st = c.prepareStatement("SELECT * FROM GOODS WHERE NAME = ?")) {
            st.setString(1, name);
            try (ResultSet rs = st.executeQuery()) {
                while (rs.next()) {
                    Good good = getGoodFromCursor(rs);
                    retVal.add(good);
                }
            }
            return retVal;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void insertGood(Good good) {
        Connection c = db.getValidatedConnection();
        try (PreparedStatement st = c.prepareStatement("INSERT INTO GOODS (NAME, QUANTITY, COST) VALUES (?, ?, ?)", Statement.RETURN_GENERATED_KEYS)) {
            st.setString(1, good.getName());
            st.setInt(2, good.getQuantity());
            st.setDouble(3, good.getCost());
            st.execute();
            try (ResultSet rs = st.getGeneratedKeys()) {
                rs.next();
                good.setId(rs.getInt(1));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void updateGood(Good good) {
        if (good.getId() == null) {
            throw new IllegalArgumentException("Cannot update a non-stored object");
        }
        Connection c = db.getValidatedConnection();
        try (PreparedStatement st = c.prepareStatement("UPDATE GOODS SET NAME = ?, QUANTITY = ?, COST = ? WHERE ID = ?")) {
            st.setString(1, good.getName());
            st.setInt(2, good.getQuantity());
            st.setDouble(3, good.getCost());
            st.setInt(4, good.getId());
            st.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void upsertGood(Good good) {
        if (good.getId() == null) {
            insertGood(good);
        } else {
            updateGood(good);
        }
    }

    public void deleteGood(Good good) {
        if (good.getId() == null) {
            throw new IllegalArgumentException("Cannot delete a non-stored object");
        }
        Connection c = db.getValidatedConnection();
        try (PreparedStatement st = c.prepareStatement("DELETE FROM GOODS WHERE ID = ?")) {
            st.setInt(1, good.getId());
            st.execute();
            good.setId(null);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}

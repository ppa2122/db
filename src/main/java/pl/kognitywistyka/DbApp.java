package pl.kognitywistyka;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * Created by pwilkin on 26.05.2022.
 */
public class DbApp {

    private DbManager dbManager;

    public static void main(String[] args) {
        new DbApp().start();
    }

    public DbManager getDbManager() {
        return dbManager;
    }

    private void start() {
        this.dbManager = new DbManager();
        this.dbManager.prepareDbIfNeeded();
    }
}

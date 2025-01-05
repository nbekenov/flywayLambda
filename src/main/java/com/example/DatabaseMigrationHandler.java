package com.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.flywaydb.core.Flyway;
import org.postgresql.ds.PGSimpleDataSource;
import org.postgresql.ssl.NonValidatingFactory;
import java.sql.Connection;
import java.sql.SQLException;

import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.ds.AwsWrapperDataSource;

import java.util.Properties;
import java.util.Objects;
import java.util.Map;

public class DatabaseMigrationHandler implements RequestHandler<Map<String, String>, String> {

    // instance vars
    private final String dbHost;
    private final String dbPort;
    private final String dbName;
    private final String dbSchema;
    private final String dbUser;

    // property names
    static final String PROP_DB_HOST = "db.host";
    static final String PROP_DB_PORT = "db.port";
    static final String PROP_DB_NAME = "db.name";
    static final String PROP_DB_SCHEMA = "db.schema";
    static final String PROP_DB_USER = "db.user";

    // Constructor
    public DatabaseMigrationHandler() {
        this.dbHost = getSystemProperty(PROP_DB_HOST);
        this.dbPort = getSystemProperty(PROP_DB_PORT);
        this.dbName = getSystemProperty(PROP_DB_NAME);
        this.dbSchema = getSystemProperty(PROP_DB_SCHEMA);
        this.dbUser = getSystemProperty(PROP_DB_USER);
    }

    // method
    private String getSystemProperty(String propName){
        String err_msg = "Required system property '" + propName + "' not found";
        return Objects.requireNonNull(System.getProperty(propName), err_msg);
    }

    @Override
    public String handleRequest(Map<String, String> input, Context context) {
        DatabaseMigrationHandler handler = new DatabaseMigrationHandler();
        AwsWrapperDataSource dataSource = getDataSource();
        // Test database connection
        if (testConnection(dataSource)) {
            System.out.println("Database connection successful!");

            // Run Flyway migrations
            runMigrations(dataSource);
        } else {
            System.out.println("Failed to connect to the database.");
        }

        return "ok";
    }

    private boolean testConnection(AwsWrapperDataSource dataSource) {
        try (Connection connection = dataSource.getConnection()) {
            return connection != null;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    private void runMigrations(AwsWrapperDataSource dataSource) {
        try{
            Flyway flyway = Flyway.configure()
                    .dataSource(dataSource)
                    .createSchemas(true)
                    .schemas(this.dbSchema)
                    .load();
            flyway.migrate();

            System.out.println("Completed Database migration!");
        } catch (Exception e) {
            System.out.println("Database migration failed!");
            e.printStackTrace();
        }
    }

    private AwsWrapperDataSource getDataSource() {
        Properties targetDataSourceProps = new Properties();
        targetDataSourceProps.setProperty("ssl", "false");
        targetDataSourceProps.setProperty("sslfactory", NonValidatingFactory.class.getName());
        // Enable AWS IAM database authentication and configure driver property values
        targetDataSourceProps.setProperty(PropertyDefinition.PLUGINS.name, "iam");

        AwsWrapperDataSource ds = new AwsWrapperDataSource();
        ds.setJdbcProtocol("jdbc:aws-wrapper:postgresql:");
        ds.setTargetDataSourceClassName(PGSimpleDataSource.class.getName());
        ds.setServerName(this.dbHost);
        ds.setDatabase(this.dbName);
        ds.setServerPort(this.dbPort);
        ds.setUser(this.dbUser);
        ds.setTargetDataSourceProperties(targetDataSourceProps);

        return ds;
    }
}

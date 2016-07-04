package com.cberez.java;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.sql.*;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

public class App 
{
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    public static void main( String[] args ) throws SQLException, ParseException, InterruptedException {
        // Parse arguments
        Options opts = new Options();
        opts.addOption("h", "help", false, "Display this help");
        opts.addOption("s", "server", true, "The HiveServer2 address (default: localhost)");
        opts.addOption("p", "port", true, "The HiveServer2 port (default: 10000)");
        opts.addOption("hp", "hive-principal", true, "The principal for the HiveServer2 node (mandatory)");
        opts.addOption("pr", "principal", true, "The app principal");
        opts.addOption("kt", "keytab", true, "The app's keytab");
        opts.addOption("kc", "krb-conf", true, "Path to krb5.conf");

        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(opts, args);

        if (cmd.hasOption("h")) {
            HelpFormatter f = new HelpFormatter();
            f.printHelp("Usage", opts);
            System.exit(1);
        }

        if (!cmd.hasOption("hp")) {
            System.out.println("Missing hive principal");
            System.exit(1);
        }

        if (!cmd.hasOption("kt")) {
            System.out.println("Missing app's keytab");
            System.exit(1);
        }

        if (!cmd.hasOption("pr")) {
            System.out.println("Missing app's principal");
            System.exit(1);
        }

        // Load hive driver
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }

        final String server = cmd.getOptionValue("s", "localhost");
        final String port = cmd.getOptionValue("p", "10000");
        final String hive_pr = cmd.getOptionValue("hp");
        final String database = "default";
        final String table = "jdbc_example";
        final String principal = cmd.getOptionValue("pr");
        final String keytab = cmd.getOptionValue("kt");

        System.out.println("Login to KRB");
        try {
            // If specified, add the krb5.conf file
            if (cmd.hasOption("kc")) {
                System.setProperty("java.security.krb5.conf", cmd.getOptionValue("kc"));
            }

            // Set Kerberos auth
            Configuration conf = new Configuration();
            conf.set("hadoop.security.authentication", "Kerberos");

            // Authenticate to krb
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab);

            // Do rest as authenticated user
            ugi.doAs(new PrivilegedExceptionAction<Object>() {
                public Object run() throws Exception {
                    // Connect to hive w/ url : jdbc:hive2://server:port/db;principal=hive/serv@REALM
                    Connection conn = DriverManager.getConnection(String.format("jdbc:hive2://%s:%s/default;principal=%s", server, port, hive_pr), "", "");
                    Statement stmt = conn.createStatement();

                    // Clean
                    System.out.println("Cleaning DB");
                    stmt.execute(String.format("USE %s", database));
                    stmt.execute(String.format("DROP TABLE IF EXISTS %s", table));
                    stmt.execute(String.format("CREATE TABLE %s (key int, name string, country string)", table));

                    // Populate
                    System.out.println("Populating table");
                    StringBuilder dataset = new StringBuilder();
                    dataset.append("(1, 'Paris', 'France'), ");
                    dataset.append("(2, 'Lyon', 'France'), ");
                    dataset.append("(3, 'London', 'UK'), ");
                    dataset.append("(4, 'Madrid', 'Spain'), ");
                    dataset.append("(5, 'Barcelona', 'Spain'), ");
                    dataset.append("(6, 'Amsterdam', 'Netherland'), ");
                    dataset.append("(7, 'Warsaw', 'Poland'), ");
                    dataset.append("(8, 'Krakow', 'Poland') ");

                    stmt.execute(String.format("INSERT INTO TABLE %s.%s VALUES %s", database, table, dataset.toString()));

                    // Describe
                    String sql = String.format("DESCRIBE %s.%s", database, table);
                    System.out.println("\nExecuting: " + sql);
                    ResultSet res = stmt.executeQuery(sql);
                    while (res.next()) {
                        System.out.println(String.format("%s\t%s", res.getString(1), res.getString(2)));
                    }

                    // Select *
                    sql = String.format("SELECT * FROM %s.%s", database, table);
                    System.out.println("\nExecuting: " + sql);
                    res = stmt.executeQuery(sql);
                    while (res.next()) {
                        System.out.println(String.format("\t%s\t%s\t%s", res.getString(1), res.getString(2), res.getString(3)));
                    }

                    // Select * where
                    sql = String.format("SELECT * FROM %s.%s WHERE country='France'", database, table);
                    System.out.println("\nExecuting: " + sql);
                    res = stmt.executeQuery(sql);
                    while (res.next()) {
                        System.out.println(String.format("\t%s\t%s\t%s", res.getString(1), res.getString(2), res.getString(3)));
                    }

                    return null;
                }
            });
            
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}

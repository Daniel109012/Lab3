package com.napier.sem;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class App {

    /**
     * Connection to MySQL database.
     */
    private Connection con = null;

    /**
     * Connect to the MySQL database.
     */
    public void connect() {
        try {
            // Load Database driver
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            System.out.println("Could not load SQL driver");
            System.exit(-1);
        }

        // Connection to the database
        int retries = 10;
        for (int i = 0; i < retries; ++i) {
            System.out.println("Connecting to database...");
            try {
                // Wait a bit for db to start
                Thread.sleep(30000);
                // Connect to database
                con = DriverManager.getConnection("jdbc:mysql://db:3306/employees?useSSL=false", "root", "example");
                System.out.println("Successfully connected");
                // Exit for loop
                break;
            } catch (SQLException sqle) {
                System.out.println("Failed to connect to database attempt " + i);
                System.out.println(sqle.getMessage());
            } catch (InterruptedException ie) {
                System.out.println("Thread interrupted? Should not happen.");
            }
        }
    }

    public List<Employee> getEngineers() {
        String query = "SELECT employees.emp_no, employees.first_name, employees.last_name, salaries.salary " +
                "FROM employees, salaries, titles " +
                "WHERE employees.emp_no = salaries.emp_no " +
                "AND employees.emp_no = titles.emp_no " +
                "AND salaries.to_date = '9999-01-01' " +
                "AND titles.to_date = '9999-01-01' " +
                "AND titles.title = 'Engineer' " +
                "ORDER BY employees.emp_no ASC";

        List<Employee> engineers = new ArrayList<>();
        try (PreparedStatement preparedStatement = con.prepareStatement(query)) {
            ResultSet rset = preparedStatement.executeQuery();

            while (rset.next()) {
                Employee emp = new Employee();
                emp.emp_no = rset.getInt("emp_no");
                emp.first_name = rset.getString("first_name");
                emp.last_name = rset.getString("last_name");
                emp.salary = rset.getDouble("salary");
                engineers.add(emp);
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            System.out.println("Failed to get engineer details");
        }
        return engineers;
    }

    public void displayEmployee(Employee emp) {
        if (emp != null) {
            System.out.println(
                    emp.emp_no + " " +
                            emp.first_name + " " +
                            emp.last_name + "\n" +
                            "Salary: " + emp.salary + "\n"
            );
        }
    }

    /**
     * Disconnect from the MySQL database.
     */
    public void disconnect() {
        if (con != null) {
            try {
                // Close connection
                con.close();
                System.out.println("Connection closed.");
            } catch (Exception e) {
                System.out.println("Error closing connection to database.");
            }
        }
    }

    public static void main(String[] args) {
        // Create new Application
        App a = new App();

        // Connect to database
        a.connect();

        // Get Engineers
        List<Employee> engineers = a.getEngineers();

        // Display results
        for (Employee emp : engineers) {
            a.displayEmployee(emp);
        }

        // Disconnect from database
        a.disconnect();
    }

    class Employee {
        int emp_no;
        String first_name;
        String last_name;
        double salary;
    }
}

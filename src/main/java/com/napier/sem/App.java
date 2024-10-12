package com.napier.sem;

import java.sql.*;

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

    public Employee getEmployee(int ID) {
        String query = "SELECT e.emp_no, e.first_name, e.last_name, t.title, s.salary, d.dept_name, " +
                "m.first_name AS manager_first_name, m.last_name AS manager_last_name " +
                "FROM employees e " +
                "JOIN titles t ON e.emp_no = t.emp_no AND t.to_date = '9999-01-01' " +
                "JOIN salaries s ON e.emp_no = s.emp_no AND s.to_date = '9999-01-01' " +
                "JOIN dept_emp de ON e.emp_no = de.emp_no AND de.to_date = '9999-01-01' " +
                "JOIN departments d ON de.dept_no = d.dept_no " +
                "LEFT JOIN dept_manager dm ON d.dept_no = dm.dept_no AND dm.to_date = '9999-01-01' " +
                "LEFT JOIN employees m ON dm.emp_no = m.emp_no " +
                "WHERE e.emp_no = ? " +
                "ORDER BY e.emp_no";

        try (PreparedStatement preparedStatement = con.prepareStatement(query)) {
            preparedStatement.setInt(1, ID);
            ResultSet rset = preparedStatement.executeQuery();

            if (rset.next()) {
                Employee emp = new Employee();
                emp.emp_no = rset.getInt("emp_no");
                emp.first_name = rset.getString("first_name");
                emp.last_name = rset.getString("last_name");
                emp.title = rset.getString("title");
                emp.salary = rset.getDouble("salary");
                emp.dept_name = rset.getString("dept_name");
                emp.manager = rset.getString("manager_first_name") + " " + rset.getString("manager_last_name");
                return emp;
            } else {
                return null;
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            System.out.println("Failed to get employee details");
            return null;
        }
    }

    public void displayEmployee(Employee emp) {
        if (emp != null) {
            System.out.println(
                    emp.emp_no + " " +
                            emp.first_name + " " +
                            emp.last_name + "\n" +
                            emp.title + "\n" +
                            "Salary: " + emp.salary + "\n" +
                            emp.dept_name + "\n" +
                            "Manager: " + emp.manager + "\n"
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
        // Get Employee
        Employee emp = a.getEmployee(255530);
        // Display results
        a.displayEmployee(emp);

        // Disconnect from database
        a.disconnect();
    }

    class Employee {
        int emp_no;
        String first_name;
        String last_name;
        String title;
        double salary;
        String dept_name;
        String manager;
    }
}



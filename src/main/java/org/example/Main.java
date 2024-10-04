package org.example;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        System.out.println("Starting CSV to MySQL data import...");

        String URL = "jdbc:mysql://localhost:3306/";
        String USERNAME = "root";
        String PASSWORD = "";

        String sql = "INSERT INTO zipcodes (zip_code, city_name) VALUES (?, ?)";

        int batchSize = 20;


        try (Connection connection = DriverManager.getConnection(URL, USERNAME, PASSWORD)) {
            connection.setAutoCommit(false);


            File csvFile = new File("C:\\Users\\Jensk\\IdeaProjects\\asciiProjekt\\csvTomysql\\src\\main\\java\\org\\example\\postnummerfil-til-download-22-06-2021.csv");
            try (Scanner scanner = new Scanner(csvFile)) {
                PreparedStatement statement = connection.prepareStatement(sql);
                int count = 0;


                if (scanner.hasNextLine()) {
                    scanner.nextLine();
                }


                while (scanner.hasNextLine()) {
                    String line = scanner.nextLine();
                    String[] nextRecord = line.split(";");


                    if (nextRecord.length < 2) {
                        System.out.println("Skipping invalid row (wrong length): " + line);
                        continue;
                    }


                    String zipCode = nextRecord[0].trim();
                    String cityName = nextRecord[1].trim();


                    System.out.println("Zip Code: " + zipCode + " | Length: " + zipCode.length());


                    statement.setString(1, zipCode);
                    statement.setString(2, cityName);


                    statement.addBatch();
                    count++;


                    if (count % batchSize == 0) {
                        statement.executeBatch();
                    }
                }


                statement.executeBatch();


                connection.commit();


                statement.close();

                System.out.println("Data inserted successfully!");

            } catch (FileNotFoundException e) {
                System.err.println("CSV file not found: " + e.getMessage());
            } catch (Exception e) {
                e.printStackTrace();
                System.err.println("Error reading CSV or inserting data: " + e.getMessage());
                connection.rollback();
            }

        } catch (SQLException e) {
            e.printStackTrace();
            System.err.println("Database connection error: " + e.getMessage());
        }
    }
}


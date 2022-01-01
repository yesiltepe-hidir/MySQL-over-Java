package ceng.ceng351.cengvacdb;

import java.sql.*;

import ceng.ceng351.cengvacdb.User;
import ceng.ceng351.cengvacdb.Vaccine;
import ceng.ceng351.cengvacdb.Vaccination;
import ceng.ceng351.cengvacdb.AllergicSideEffect;
import ceng.ceng351.cengvacdb.Seen;
import ceng.ceng351.cengvacdb.QueryResult;




public class Main {
    public static void main(String[] args) {
        dropTables();
        createTables();

        User user1 = new User(31, "Ömer", 13, "xsa", "ydsa", "zasfaf");
        User user2 = new User(69, "Yiğit", 16, "aasfaf", "bafs", "cfasfa");
        User user3 = new User(16, "Hamdi", 19, "aasfaf", "bafs", "cfasfa");

        User[] users ;
        users = new User[3];

        users[0] = user1;
        users[1] = user2;
        users[2] = user3;

        insertUser(users);

        Vaccine vaccine1 = new Vaccine(31, "B12", "A");
        Vaccine vaccine2 = new Vaccine(69, "Covid19", "B");
        Vaccine vaccine3 = new Vaccine(52, "Grip", "C");
        Vaccine vaccine4 = new Vaccine(100, "adanavac", "D");

        Vaccine[] input_vaccine = new Vaccine[4];
        input_vaccine[0] = vaccine1;
        input_vaccine[1] = vaccine2;
        input_vaccine[2] = vaccine3;
        input_vaccine[3] = vaccine4;


        Vaccination vaccination1 = new Vaccination(31, 31, 2, "2021-11-11");
        //Vaccination vaccination2 = new Vaccination(52, 31, 2, "2021-11-11");
        //Vaccination vaccination3 = new Vaccination(69, 31, 2, "2021-11-11");
        Vaccination vaccination2 = new Vaccination(52, 69, 3, "2021-11-10");
        Vaccination vaccination3 = new Vaccination(100, 16, 1, "2021-10-11");


        Vaccination[] input_vaccination = new Vaccination[3];
        input_vaccination[0] = vaccination1;
        input_vaccination[1] = vaccination2;
        input_vaccination[2] = vaccination3;


        insertVaccine(input_vaccine);
        insertVaccination(input_vaccination);
        /*
        Vaccine[] res = getVaccinesNotAppliedAnyUser();

        for (Vaccine x: res){
            System.out.println(x.getCode() + " " + x.getType() + " " + x.getVaccineName());
        }
        */
        /*
        QueryResult.UserIDuserNameAddressResult[] res = getVaccinatedUsersforTwoDosesByDate("2021-11-11");
        for (QueryResult.UserIDuserNameAddressResult x: res){
            System.out.println(x.userID + " " + x.userName + " " + x.address);
        }
        */

        Vaccine[] res = getTwoRecentVaccinesDoNotContainVac();
        for (Vaccine x: res){
            System.out.println(x.getCode() + " " + x.getType() + " " + x.getVaccineName());
        }
    }

    public static Connection initialize() {
        try {
            String username = "e2381069";
            String password = "Vqt$a&*71pFN";
            String url = "jdbc:mysql://144.122.71.121:8080/db2381069?autoReconnect=true&useSSL=false";
            String driver = "com.mysql.cj.jdbc.Driver";
            Class.forName(driver);

            Connection conn = DriverManager.getConnection(url, username, password);
            System.out.println("Connection Established...");
            return conn;
        } catch (Exception e) {
            System.out.println("Couldn't be connected...");
            System.out.println(e);
            return null;
        }
    }

    public static int createTables() {
        int number_of_successful_created_tables = 5;

        try{
            Connection conn = initialize();
            String user_command = "CREATE TABLE IF NOT EXISTS User " +
                                  "(userID INTEGER, " +
                                  "userName VARCHAR(30), " +
                                  "age INTEGER, " +
                                  "address VARCHAR(150), " +
                                  "password VARCHAR(30), " +
                                  "status VARCHAR(15), " +
                                  "PRIMARY KEY (userID))";

            String vaccine_command = "CREATE TABLE IF NOT EXISTS Vaccine " +
                                     "(code INTEGER, " +
                                     "vaccinename VARCHAR(30), " +
                                     "type VARCHAR(30), " +
                                     "PRIMARY KEY (code))";

            String vaccination_command = "CREATE TABLE IF NOT EXISTS Vaccination " +
                                         "(code INTEGER, " +
                                         "userID INTEGER, " +
                                         "dose INTEGER, " +
                                         "vacdate DATE, " +
                                         "PRIMARY KEY (code, userID), " +
                                         "FOREIGN KEY (code) REFERENCES Vaccine(code), " +
                                         "FOREIGN KEY (userID) REFERENCES User(userID))";

            String alergy_command = "CREATE TABLE IF NOT EXISTS AlergicSideEffect " +
                                    "(effectcode INTEGER, " +
                                    "efectname VARCHAR(50), " +
                                    "PRIMARY KEY (effectcode))";

            String seen_command = "CREATE TABLE IF NOT EXISTS Seen " +
                                  "(effectcode INTEGER, " +
                                  "code INTEGER, " +
                                  "userID INTEGER, " +
                                  "date DATE, " +
                                  "degree VARCHAR(30), " +
                                  "PRIMARY KEY (effectcode, code, userID), " +
                                  "FOREIGN KEY (effectcode) REFERENCES AlergicSideEffect(effectcode), " +
                                  "FOREIGN KEY (code) REFERENCES Vaccination(code) ON DELETE CASCADE, " +
                                  "FOREIGN KEY (userID) REFERENCES User(userID))";

            PreparedStatement create_user = conn.prepareStatement(user_command);
            PreparedStatement create_vaccine = conn.prepareStatement(vaccine_command);
            PreparedStatement create_vaccination = conn.prepareStatement(vaccination_command);
            PreparedStatement create_alergy = conn.prepareStatement(alergy_command);
            PreparedStatement create_seen = conn.prepareStatement(seen_command);

            create_user.executeUpdate();
            create_vaccine.executeUpdate();
            create_vaccination.executeUpdate();
            create_alergy.executeUpdate();
            create_seen.executeUpdate();

            System.out.println("Tables Created Successfully...");

        } catch (Exception e){
            System.out.println("Table Creation Failed...");
            System.out.println(e);
        }

        return number_of_successful_created_tables;
    }

    public static int dropTables() {
        int number_of_dropped_tables = 0;
        try{
            Connection conn = initialize();
            String drop_command = "DROP TABLE IF EXISTS Seen, Vaccination, User, AlergicSideEffect, Vaccine";

            PreparedStatement drop_tables = conn.prepareStatement(drop_command);
            drop_tables.executeUpdate();

            System.out.println("Tables Dropped successfully...");

        } catch (Exception e){
            System.out.println("Table couldn't be dropped...");
            System.out.println(e);
        }

        return number_of_dropped_tables;
    }

    public static int insertUser(User[] users)
    {
        int number_of_added_rows = 0;

        try{
            Connection conn = initialize();
            for (User u: users){

                int userID = u.getUserID();
                String userName = u.getUserName();
                int age = u.getAge();
                String address = u.getAddress();
                String password = u.getPassword();
                String status = u.getStatus();

                String insert_command = "INSERT INTO User (userID, userName, age, address, password, status)" +
                                        "VALUES (?, ?, ?, ?, ?, ?)";



                PreparedStatement insert_user = conn.prepareStatement(insert_command);

                insert_user.setInt(1, userID);
                insert_user.setString(2, userName);
                insert_user.setInt(3, age);
                insert_user.setString(4, address);
                insert_user.setString(5, password);
                insert_user.setString(6, status);

                insert_user.executeUpdate();
                System.out.println("Inserted Successfully...");
            }
        } catch (Exception e) {
            System.out.println("Couldn't be inserted...");
            System.out.println(e);
        }

        return number_of_added_rows;
    }

    public static int  insertAllergicSideEffect(AllergicSideEffect[] sideEffects) {
        int number_of_added_rows = 0;

        try{
            Connection conn = initialize();
            for (AllergicSideEffect a: sideEffects){

                int effectcode = a.getEffectCode();
                String effectname = a.getEffectName();

                String insert_command = "INSERT INTO AlergicSideEffect (effectcode, effectname)" +
                                        "VALUES (?, ?)";


                PreparedStatement insert_alergic = conn.prepareStatement(insert_command);

                insert_alergic.setInt(1, effectcode);
                insert_alergic.setString(2, effectname);


                insert_alergic.executeUpdate();
                System.out.println("Inserted Successfully...");
            }
        } catch (Exception e) {
            System.out.println("Couldn't be inserted...");
            System.out.println(e);
        }

        return number_of_added_rows;
    }

    public static int insertVaccine(Vaccine[] vaccines) {
        int number_of_added_rows = 0;

        try{
            Connection conn = initialize();
            for (Vaccine v: vaccines){

                int code = v.getCode();
                String vaccinename = v.getVaccineName();
                String type = v.getType();

                String insert_command = "INSERT INTO Vaccine (code, vaccinename, type)" +
                                        "VALUES (?, ?, ?)";


                PreparedStatement insert_vaccines = conn.prepareStatement(insert_command);

                insert_vaccines.setInt(1, code);
                insert_vaccines.setString(2, vaccinename);
                insert_vaccines.setString(3, type);


                insert_vaccines.executeUpdate();
                System.out.println("Inserted Successfully...");
            }
        } catch (Exception e) {
            System.out.println("Couldn't be inserted...");
            System.out.println(e);
        }

        return number_of_added_rows;
    }

    public static int insertVaccination(Vaccination[] vaccinations) {
        int number_of_added_rows = 0;

        try{
            Connection conn = initialize();
            for (Vaccination v: vaccinations){

                int code = v.getCode();
                int userID = v.getUserID();
                int dose = v.getDose();
                String vacdate = v.getVacdate();

                String insert_command = "INSERT INTO Vaccination (code, userID, dose, vacdate)" +
                                        "VALUES (?, ?, ?, ?)";


                PreparedStatement insert_vaccinations = conn.prepareStatement(insert_command);

                insert_vaccinations.setInt(1, code);
                insert_vaccinations.setInt(2, userID);
                insert_vaccinations.setInt(3, dose);
                insert_vaccinations.setString(4, vacdate);

                insert_vaccinations.executeUpdate();
                System.out.println("Inserted Successfully...");
            }
        } catch (Exception e) {
            System.out.println("Couldn't be inserted...");
            System.out.println(e);
        }

        return number_of_added_rows;
    }

    public static int insertSeen(Seen[] seens) {
        int number_of_added_rows = 0;

        try{
            Connection conn = initialize();
            for (Seen s: seens){

                int effectcode = s.getEffectcode();
                int code = s.getCode();
                String userID = s.getUserID();
                String date = s.getDate();
                String degree = s.getDegree();

                String insert_command = "INSERT INTO Seen (effectcode, code, userID, date, degree)" +
                                        "VALUES (?, ?, ?, ?, ?)";


                PreparedStatement insert_seen = conn.prepareStatement(insert_command);

                insert_seen.setInt(1, effectcode);
                insert_seen.setInt(2, code);
                insert_seen.setString(3, userID);
                insert_seen.setString(4, date);
                insert_seen.setString(5, degree);

                insert_seen.executeUpdate();
                System.out.println("Inserted Successfully...");
            }
        } catch (Exception e) {
            System.out.println("Couldn't be inserted...");
            System.out.println(e);
        }

        return number_of_added_rows;
    }

    public static Vaccine[] getVaccinesNotAppliedAnyUser() {
        String query = "SELECT * FROM Vaccine V " +
                       "WHERE V.code NOT IN " +
                       "(SELECT T.code FROM Vaccination T) " +
                       "ORDER BY code";

        Connection conn = initialize();
        int rows = 0;

        try(Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(query);
            while (rs.next()) {
                rows = rows + 1;
            }
        } catch (Exception e)
        {
            System.out.println("Row number couldn't be found...");
            System.out.println(e);
        }

        Vaccine[] res = new Vaccine[rows];
        int idx = 0;
        try(Statement stmt = conn.createStatement()){
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()){
                int code = rs.getInt("code");
                String vaccinename = rs.getString("vaccinename");
                String type = rs.getString("type");

                Vaccine v = new Vaccine(code, vaccinename, type);
                res[idx] = v;
                idx = idx + 1;
            }
        } catch (Exception e)
        {
            System.out.println("Vaccines couldn't be added...");
            System.out.println(e);
        }



        return res;
    }

    public static QueryResult.UserIDuserNameAddressResult[] getVaccinatedUsersforTwoDosesByDate(String vacdate) {
        String query = "SELECT U.userID, U.userName, U.address " +
                       "FROM User U, Vaccination V " +
                       "WHERE U.userID = V.userID AND " +
                       "V.dose = 2 AND " +
                       "V.vacdate >= vacdate " +
                       "ORDER BY userID";

        Connection conn = initialize();
        int rows = 0;

        try(Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(query);
            while (rs.next()) {
                rows = rows + 1;
            }
        } catch (Exception e)
        {
            System.out.println("Row number couldn't be found...");
            System.out.println(e);
        }

        QueryResult.UserIDuserNameAddressResult[] res = new QueryResult.UserIDuserNameAddressResult[rows];
        int idx = 0;
        try(Statement stmt = conn.createStatement()){
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()){
                int userID = rs.getInt("userID");
                String userName = rs.getString("userName");
                String address = rs.getString("address");

                QueryResult.UserIDuserNameAddressResult v = new QueryResult.UserIDuserNameAddressResult(""+userID, userName, address);
                res[idx] = v;
                idx = idx + 1;
            }
        } catch (Exception e)
        {
            System.out.println("UserID, Name, Address couldn't be added...");
            System.out.println(e);
        }


        return res;
    }

    public static Vaccine[] getTwoRecentVaccinesDoNotContainVac() {
        String query =  "SELECT A.code, A.vaccinename, A.type " +
                        "FROM Vaccine A, Vaccination B " +
                        "WHERE A.code = B.code " +
                        "ORDER BY B.vacdate DESC";

        Connection conn = initialize();
        int rows = 0;

        try(Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(query);
            while (rs.next()) {
                rows = rows + 1;
            }
        } catch (Exception e)
        {
            System.out.println("Row number couldn't be found...");
            System.out.println(e);
        }

        Vaccine[] res = new Vaccine[2];
        int  total = 0;
        try(Statement stmt = conn.createStatement()){
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next() && total < 2){
                int code = rs.getInt("code");
                String vaccinename = rs.getString("vaccinename");
                String type = rs.getString("type");
                if (!vaccinename.contains("vac"))
                {
                    Vaccine v = new Vaccine(code, vaccinename, type);
                    res[total] = v;
                    total = total + 1;
                }

            }
        } catch (Exception e)
        {
            System.out.println("Vaccinename Error...");
            System.out.println(e);
        }

        return res;
    }
    // TODO: Check it again.
    public static QueryResult.UserIDuserNameAddressResult[] getUsersAtHasLeastTwoDoseAtMostOneSideEffect() {
        String query =  "SELECT U.userID, U.userName, U.address " +
                        "FROM User U, Vaccination V, Seen S " +
                        "WHERE  U.userID = V.userID AND U.userID = S.userID AND V.code = S.code AND " +
                        "V.dose > 1 AND " +
                        "U.userID NOT IN " +
                        "(SELECT U.userID " +
                        "FROM User U, Seen S "+
                        "WHERE  S.userID = U.userID " +
                        "GROUP BY U.userID "+
                        "HAVING COUNT(*) > 1) "+
                        "ORDER BY U.userID";

        Connection conn = initialize();
        int rows = 0;

        try(Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(query);
            while (rs.next()) {
                rows = rows + 1;
            }
        } catch (Exception e)
        {
            System.out.println("Row number couldn't be found...");
            System.out.println(e);
        }

        QueryResult.UserIDuserNameAddressResult[] res = new QueryResult.UserIDuserNameAddressResult[rows];
        int idx = 0;
        try(Statement stmt = conn.createStatement()){
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()){
                int userID = rs.getInt("userID");
                String userName = rs.getString("userName");
                String address = rs.getString("address");

                QueryResult.UserIDuserNameAddressResult v = new QueryResult.UserIDuserNameAddressResult(""+userID, userName, address);
                res[idx] = v;
                idx = idx + 1;
            }
        } catch (Exception e)
        {
            System.out.println("UserID, Name, Address couldn't be added...");
            System.out.println(e);
        }


        return res;
    }

    public static QueryResult.UserIDuserNameAddressResult[] getVaccinatedUsersWithAllVaccinesCanCauseGivenSideEffect(String effectname) {
        return new QueryResult.UserIDuserNameAddressResult[0];
    }

}

package ceng.ceng351.cengvacdb;

import java.sql.*;

public class CENGVACDB implements ICENGVACDB {
    private static Connection conn = null;

    @Override
    public void initialize() {
        try{
            String username = "e2381069";
            String password = "Vqt$a&*71pFN";
            String url = "jdbc:mysql://144.122.71.121:8080/db2381069?autoReconnect=true&useSSL=false";
            String driver = "com.mysql.cj.jdbc.Driver";
            Class.forName(driver);

            conn = DriverManager.getConnection(url, username, password);
            System.out.println("Connection Established...");

        } catch (Exception e){
            System.out.println("Couldn't be connected...");
            System.out.println(e);
        }
    }

    @Override
    public int createTables() {
        int number_of_successful_created_tables = 0;


            //initialize();
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
                    "PRIMARY KEY (code, userID, dose), " +
                    "FOREIGN KEY (code) REFERENCES Vaccine(code), " +
                    "FOREIGN KEY (userID) REFERENCES User(userID))";

            String alergy_command = "CREATE TABLE IF NOT EXISTS AllergicSideEffect " +
                    "(effectcode INTEGER, " +
                    "effectname VARCHAR(50), " +
                    "PRIMARY KEY (effectcode))";

            String seen_command = "CREATE TABLE IF NOT EXISTS Seen " +
                                "(effectcode INTEGER, " +
                                "code INTEGER, " +
                                "userID INTEGER, " +
                                "date DATE, " +
                                "degree VARCHAR(30), " +
                                "PRIMARY KEY (effectcode, code, userID), " +
                                "FOREIGN KEY (effectcode) REFERENCES AllergicSideEffect(effectcode), " +
                                "FOREIGN KEY (code) REFERENCES Vaccination(code) ON DELETE CASCADE, " +
                                "FOREIGN KEY (userID) REFERENCES User(userID))";

            try
            {
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
                number_of_successful_created_tables = 5;
            } catch (Exception e){
                System.out.println("Table Creation Failed...");
                System.out.println(e);
                return 0;
        }

        return number_of_successful_created_tables;
    }

    @Override
    public int dropTables() {
        int number_of_dropped_tables = 0;
        try{
            //initialize();
            String drop_command = "DROP TABLE  Seen, Vaccination, User, AllergicSideEffect, Vaccine";

            PreparedStatement drop_tables = conn.prepareStatement(drop_command);
            drop_tables.executeUpdate();
            number_of_dropped_tables = 5;
            System.out.println("Tables Dropped successfully...");

        } catch (Exception e){
            System.out.println("Table couldn't be dropped...");
            System.out.println(e);
        }

        return number_of_dropped_tables;
    }

    @Override
    public int insertUser(User[] users) {
        int number_of_added_rows = 0;

        try{
            //initialize();
            for (User u: users){
                number_of_added_rows += 1;
                int userID = u.getUserID();
                String userName = u.getUserName();
                int age = u.getAge();
                String address = u.getAddress();
                String password = u.getPassword();
                String status = u.getStatus();

                String insert_command = "INSERT INTO User (userID, userName, age, address, password, status) " +
                                        "VALUES (?, ?, ?, ?, ?, ?)";



                PreparedStatement insert_user = conn.prepareStatement(insert_command);

                insert_user.setInt(1, userID);
                insert_user.setString(2, userName);
                insert_user.setInt(3, age);
                insert_user.setString(4, address);
                insert_user.setString(5, password);
                insert_user.setString(6, status);

                insert_user.executeUpdate();

            }
        } catch (Exception e) {
            System.out.println("Couldn't be inserted into User...");
            System.out.println(e);
            return 0;
        }
        System.out.println("Inserted Successfully User...");
        return number_of_added_rows;
    }

    @Override
    public int insertAllergicSideEffect(AllergicSideEffect[] sideEffects) {
        int number_of_added_rows = 0;

        try{
            //initialize();
            for (AllergicSideEffect a: sideEffects){
                number_of_added_rows += 1;
                int effectcode = a.getEffectCode();
                String effectname = a.getEffectName();

                String insert_command = "INSERT INTO AllergicSideEffect (effectcode, effectname) " +
                                        "VALUES (?, ?)";


                PreparedStatement insert_alergic = conn.prepareStatement(insert_command);

                insert_alergic.setInt(1, effectcode);
                insert_alergic.setString(2, effectname);


                insert_alergic.executeUpdate();

            }
        } catch (Exception e) {
            System.out.println("Couldn't be inserted into Allergy...");
            System.out.println(e);
            return 0;
        }

        System.out.println("Inserted Successfully Allergy...");
        return number_of_added_rows;
    }

    @Override
    public int insertVaccine(Vaccine[] vaccines) {
        int number_of_added_rows = 0;

        try{
            //initialize();
            for (Vaccine v: vaccines){
                number_of_added_rows += 1;
                int code = v.getCode();
                String vaccinename = v.getVaccineName();
                String type = v.getType();

                String insert_command = "INSERT INTO Vaccine (code, vaccinename, type) " +
                                        "VALUES (?, ?, ?)";


                PreparedStatement insert_vaccines = conn.prepareStatement(insert_command);

                insert_vaccines.setInt(1, code);
                insert_vaccines.setString(2, vaccinename);
                insert_vaccines.setString(3, type);


                insert_vaccines.executeUpdate();

            }
        } catch (Exception e) {
            System.out.println("Couldn't be inserted Vaccine...");
            System.out.println(e);
            return  0;
        }
        System.out.println("Inserted Successfully Vaccine...");
        return number_of_added_rows;
    }

    @Override
    public int insertVaccination(Vaccination[] vaccinations) {
        int number_of_added_rows = 0;

        try{
            //initialize();
            for (Vaccination v: vaccinations){
                number_of_added_rows += 1;
                int code = v.getCode();
                int userID = v.getUserID();
                int dose = v.getDose();
                String vacdate = v.getVacdate();

                String insert_command = "INSERT INTO Vaccination (code, userID, dose, vacdate) " +
                                        "VALUES (?, ?, ?, ?)";


                PreparedStatement insert_vaccinations = conn.prepareStatement(insert_command);

                insert_vaccinations.setInt(1, code);
                insert_vaccinations.setInt(2, userID);
                insert_vaccinations.setInt(3, dose);
                insert_vaccinations.setString(4, vacdate);

                insert_vaccinations.executeUpdate();

            }
        } catch (Exception e) {
            System.out.println("Couldn't be inserted into vaccination...");
            System.out.println(e);
            return 0;
        }
        System.out.println("Inserted Successfully vaccination...");
        return number_of_added_rows;
    }

    @Override
    public int insertSeen(Seen[] seens) {
        int number_of_added_rows = 0;

        try{
            //initialize();
            for (Seen s: seens){
                number_of_added_rows += 1;
                int effectcode = s.getEffectcode();
                int code = s.getCode();
                String userID = s.getUserID();
                String date = s.getDate();
                String degree = s.getDegree();

                String insert_command = "INSERT INTO Seen (effectcode, code, userID, date, degree) " +
                                        "VALUES (?, ?, ?, ?, ?)";


                PreparedStatement insert_seen = conn.prepareStatement(insert_command);

                insert_seen.setInt(1, effectcode);
                insert_seen.setInt(2, code);
                insert_seen.setInt(3, Integer.parseInt(userID));
                insert_seen.setString(4, date);
                insert_seen.setString(5, degree);

                insert_seen.executeUpdate();

            }
        } catch (Exception e) {
            System.out.println("Couldn't be inserted into seen...");
            System.out.println(e);
            return 0;
        }
        System.out.println("Inserted Successfully seen...");
        return number_of_added_rows;
    }

    @Override
    public Vaccine[] getVaccinesNotAppliedAnyUser() {
        String query =  "SELECT DISTINCT * " +
                        "FROM Vaccine V " +
                        "WHERE V.code NOT IN " +
                        "(SELECT T.code FROM Vaccination T) " +
                        "ORDER BY code";

        //initialize();
        int rows = 0;

        try(Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(query);
            while (rs.next()) {
                rows = rows + 1;
            }
        } catch (Exception e)
        {
            System.out.println("Row number couldn't be found getVaccinesNotAppliedAnyUser...");
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
            System.out.println("Vaccines couldn't be added getVaccinesNotAppliedAnyUser...");
            System.out.println(e);
        }



        return res;
    }

    @Override
    public QueryResult.UserIDuserNameAddressResult[] getVaccinatedUsersforTwoDosesByDate(String vacdate) {
        String query = "SELECT DISTINCT U.userID, U.userName, U.address " +
                        "FROM User U, Vaccination V1, Vaccination V2 " +
                        "WHERE U.userID = V1.userID AND V2.userID = V1.userID AND " +
                        "V1.dose < V2.dose AND " +
                        "V1.vacdate >= '" + vacdate + "' " +
                        "GROUP BY U.userID " +
                        "HAVING COUNT(*) = 1 " +
                        "ORDER BY U.userID";

        System.out.println("Vacdate: " + vacdate);
        //initialize();
        int rows = 0;

        try(Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(query);
            while (rs.next()) {
                rows = rows + 1;
            }
        } catch (Exception e)
        {
            System.out.println("Row number couldn't be found getVaccinatedUsersforTwoDosesByDate...");
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
            System.out.println("UserID, Name, Address couldn't be added getVaccinatedUsersforTwoDosesByDate...");
            System.out.println(e);
        }


        return res;
    }

    @Override
    public Vaccine[] getTwoRecentVaccinesDoNotContainVac() {
        String query =  "SELECT V.code, V.vaccinename, V.type " +
                        "FROM  Vaccine V, Vaccination VC " +
                        "WHERE V.code = VC.code AND " +
                        "V.vaccinename NOT LIKE '%vac%' " +
                        "GROUP BY VC.code, VC.vacdate "+
                        "HAVING VC.vacdate = " +
                        "(SELECT MAX(Temp.vacdate) FROM Vaccination Temp WHERE Temp.code = VC.code) " +
                        "ORDER BY VC.vacdate DESC, V.code " +
                        "LIMIT 2";


        //initialize();

        Vaccine[] res = new Vaccine[2];
        int  total = 0;
        try(Statement stmt = conn.createStatement()){
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()){
                int code = rs.getInt("code");
                String vaccinename = rs.getString("vaccinename");
                String type = rs.getString("type");
                Vaccine v = new Vaccine(code, vaccinename, type);
                res[total] = v;
                total = total + 1;
            }
        } catch (Exception e)
        {
            System.out.println("Vaccinename Error getTwoRecentVaccinesDoNotContainVac...");
            System.out.println(e);
        }

        return res;
    }

    @Override
    public QueryResult.UserIDuserNameAddressResult[] getUsersAtHasLeastTwoDoseAtMostOneSideEffect() {
        String query =  "SELECT DISTINCT U.userID, U.userName, U.address " +
                        "FROM User U, Vaccination V1, Vaccination V2 " +
                        "WHERE U.userID = V1.userID AND V1.userID = V2.userID AND " +
                        "V1.dose < V2.dose AND " +
                        "V1.userID NOT IN " +
                        "(SELECT S.userID FROM Seen S GROUP BY S.userID HAVING COUNT(*) > 1) ORDER BY U.userID";


        //initialize();
        int rows = 0;

        try(Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(query);
            while (rs.next()) {
                rows = rows + 1;
            }
        } catch (Exception e)
        {
            System.out.println("Row number couldn't be found getUsersAtHasLeastTwoDoseAtMostOneSideEffect...");
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
            System.out.println("UserID, Name, Address couldn't be added getUsersAtHasLeastTwoDoseAtMostOneSideEffect...");
            System.out.println(e);
        }


        return res;
    }

    @Override
    public QueryResult.UserIDuserNameAddressResult[] getVaccinatedUsersWithAllVaccinesCanCauseGivenSideEffect(String effectname) {
        String query =  "SELECT DISTINCT U.userID, U.userName, U.address " +
                        "FROM User U " +
                        "WHERE NOT EXISTS " +
                                        "(SELECT  S.code " +
                                        "FROM Seen S, AllergicSideEffect A " +
                                        "WHERE S.effectcode = A.effectcode AND " +
                                        "A.effectname = '" + effectname + "' AND " +
                                        "S.code NOT IN " +
                                        "(SELECT S2.code " +
                                        "FROM Seen S2, AllergicSideEffect A2 " +
                                        "WHERE S2.effectcode = A2.effectcode AND " +
                                        "A2.effectname = '" + effectname + "' AND " +
                                        "S2.userID = U.userID)) " +
                        "ORDER BY U.userID;";
        int rows = 0;

        try(Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(query);
            while (rs.next()) {
                rows = rows + 1;
            }
        } catch (Exception e)
        {
            System.out.println("Row number couldn't be found getVaccinatedUsersWithAllVaccinesCanCauseGivenSideEffect...");
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
            System.out.println("UserID, Name, Address couldn't be added getVaccinatedUsersWithAllVaccinesCanCauseGivenSideEffect...");
            System.out.println(e);
        }

        System.out.println("getVaccinatedUsersWithAllVaccinesCanCauseGivenSideEffect Successful...");
        return res;
    }

    @Override
    public QueryResult.UserIDuserNameAddressResult[] getUsersWithAtLeastTwoDifferentVaccineTypeByGivenInterval(String startdate, String enddate) {
        String query =  "SELECT DISTINCT U.userID, U.userName, U.address " +
                        "FROM Vaccination V1, Vaccination V2, User U " +
                        "WHERE V1.userID = U.userID AND V2.userID = U.userID AND " +
                        "V1.code < V2.code AND " +
                        "V2.vacdate BETWEEN '" + startdate + "' AND '" + enddate + "' AND " +
                        "V1.vacdate BETWEEN '" + startdate + "' AND '" + enddate +"' ORDER BY U.userID";


        int rows = 0;

        try(Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(query);
            while (rs.next()) {
                rows = rows + 1;
            }
        } catch (Exception e)
        {
            System.out.println("Row number couldn't be found getUsersWithAtLeastTwoDifferentVaccineTypeByGivenInterval...");
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
            System.out.println("UserID, Name, Address couldn't be added getUsersWithAtLeastTwoDifferentVaccineTypeByGivenInterval...");
            System.out.println(e);
        }

        System.out.println("getUsersWithAtLeastTwoDifferentVaccineTypeByGivenInterval Successful...");
        return res;
    }

    @Override
    public AllergicSideEffect[] getSideEffectsOfUserWhoHaveTwoDosesInLessThanTwentyDays() {
        String query =  "SELECT DISTINCT A.effectcode, A.effectName " +
                        "FROM AllergicSideEffect A, Seen S, "+
                                "(SELECT V1.userID " +
                                "FROM Vaccination V1, Vaccination V2 " +
                                "WHERE V1.userID = V2.userID AND V1.dose < V2.dose "+
                                "GROUP BY V1.userID " +
                                "HAVING COUNT(*) = 1) AS Temp " +

                        "WHERE S.userID = Temp.userID  AND S.effectcode = A.effectcode AND S.code IN " +
                        "(SELECT V1.code " +
                        "FROM Vaccination V1, Vaccination V2 " +
                        "WHERE  Temp.userID = V1.userID AND V1.userID = V2.userID AND V1.dose < V2.dose "+
                        "AND DATEDIFF(V2.vacdate, V1.vacdate) < 20) ORDER BY A.effectcode";
        int rows = 0;

        try(Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(query);
            while (rs.next()) {
                rows = rows + 1;
            }
        } catch (Exception e)
        {
            System.out.println("Row number couldn't be found getSideEffectsOfUserWhoHaveTwoDosesInLessThanTwentyDays...");
            System.out.println(e);
        }

        AllergicSideEffect[] res = new AllergicSideEffect[rows];
        int idx = 0;
        try(Statement stmt = conn.createStatement()){
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()){
                int effectcode = rs.getInt("effectcode");
                String effectname = rs.getString("effectname");
                AllergicSideEffect v = new AllergicSideEffect(effectcode, effectname);
                res[idx] = v;
                idx = idx + 1;
            }
        } catch (Exception e)
        {
            System.out.println("Couldn't be added getSideEffectsOfUserWhoHaveTwoDosesInLessThanTwentyDays...");
            System.out.println(e);
        }

        return res;

    }

    @Override
    public double averageNumberofDosesofVaccinatedUserOverSixtyFiveYearsOld() {
        String query =  "SELECT AVG(Temp.maxdose) AS average " +
                        "FROM " +
                        "(SELECT V.userID, MAX(V.dose) AS maxdose " +
                        "FROM Vaccination V, User U " +
                        "WHERE V.userID = U.userID AND U.age > 65 " +
                        "GROUP BY V.userID) AS Temp ";


        try(Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            rs.next();
            double avg = rs.getDouble("average");
            System.out.println("averageNumberofDosesofVaccinatedUserOverSixtyFiveYearsOld Successful...");
            return avg;
        } catch (Exception e){
            System.out.println("Couldn't averageNumberofDosesofVaccinatedUserOverSixtyFiveYearsOld...");
            System.out.println(e);
            return 0;
        }

    }

    @Override
    public int updateStatusToEligible(String givendate) {
        String query =  "UPDATE User U SET U.status = 'eligible' " +
                        "WHERE U.status = 'Not_Eligible' AND U.userID IN " +
                        "(SELECT V.userID FROM " +
                        "(SELECT  V.userID, MAX(V.dose) as dose " +
                        "FROM Vaccination V " +
                        "GROUP BY V.userID) AS Temp, Vaccination V " +
                        "WHERE Temp.userID = V.userID AND  Temp.dose = V.dose AND " +
                        "DATEDIFF('"+givendate +"', V.vacdate) > 120)";
        System.out.println("Date: " + givendate);
        try(Statement s = conn.createStatement()) {
            int rs = s.executeUpdate(query);
            return rs;
        } catch (Exception e)
        {
            System.out.println("Couldn't updateStatusToEligible...");
            System.out.println(e);
        }

        return 0;
    }

    @Override
    public Vaccine deleteVaccine(String vaccineName) {
        String showVaccine =  "SELECT DISTINCT * FROM Vaccine  WHERE vaccinename = '"+ vaccineName + "'";
        String deleteVaccine = "DELETE FROM Vaccine WHERE vaccinename = '" + vaccineName + "'";
        Vaccine v = null;
        try(Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(showVaccine);
            rs.next();
            int code  = rs.getInt("code");
            String vaccinename  = rs.getString("vaccinename");
            String type  = rs.getString("type");
            v = new Vaccine(code, vaccinename, type);
            System.out.println("show Successful...");

        } catch (Exception e){
            System.out.println("Couldn't showed...");
            System.out.println(e);

        }

        try(Statement stmt = conn.createStatement()) {
            int ty = stmt.executeUpdate(deleteVaccine);
            System.out.println("deleteVaccine Successful...");

        } catch (Exception e){
            System.out.println("Couldn't deleteVaccine...");
            System.out.println(e);

        }

        return v;
    }
}

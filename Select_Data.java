

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class Select_Data {
  Connection con;

  ResultSet rs;

  Statement st;

  public ResultSet getResult(String query) {
    try {
      Class.forName("com.edb.Driver");

      //con = DriverManager.getConnection("jdbc:edb://10.150.224.20:5444/odisha_uat", "odisha_uat", "Cdac@123");
      //this.con = DriverManager.getConnection("jdbc:postgresql://10.150.172.204:5444/odisha_uat", "odisha_uat", "Cdac@123");

       //Prod
      // this.con = DriverManager.getConnection("jdbc:edb://10.150.224.20:5444/odisha_uat", "odisha_uat", "Cdac@123");

        // UAT
      this.con = DriverManager.getConnection("jdbc:edb://10.10.10.54:5447/odisha_uat", "odisha_uat", "Od15hauat");
//      this.con = DriverManager.getConnection("jdbc:edb://10.226.30.40:5444/odisha_uat", "odisha_uat", "Cdac@123");



      this.st = this.con.createStatement();
      this.rs = this.st.executeQuery(query);
    } catch (Exception ex) {
      System.out.println("error  DB :   " + ex);
    }
    return this.rs;
  }
}

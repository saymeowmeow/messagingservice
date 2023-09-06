

import java.sql.Connection;
import java.sql.DriverManager;

public class InsertData {
  Connection con;

  public Connection getCon() {
    try {
      Class.forName("com.edb.Driver");

            //this.con = DriverManager.getConnection("jdbc:postgresql://10.150.172.204:5444/odisha_uat", "odisha_uat", "Cdac@123");

      //Prod
     //  this.con = DriverManager.getConnection("jdbc:edb://10.150.224.20:5444/odisha_uat", "odisha_uat", "Cdac@123");
      //"jdbc:edb://10.226.30.40:5444/odisha_uat", "odisha_uat", "Cdac@123"

        // UAT
      this.con = DriverManager.getConnection("jdbc:edb://10.10.10.54:5447/odisha_uat", "odisha_uat", "Od15hauat");
//      this.con = DriverManager.getConnection("jdbc:edb://10.226.30.40:5444/odisha_uat", "odisha_uat", "Cdac@123");


    } catch (Exception ex) {
      System.out.println(" error in connection  : " + ex);
    }
    return this.con;
  }
}
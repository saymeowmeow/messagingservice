

import java.sql.Connection;
import java.sql.DriverManager;

public class InsertData {
  Connection con;

  public Connection getCon() {
    try {
      Class.forName("com.edb.Driver");

        this.con = DriverManager.getConnection("jdbc:edb://", "username", "password");

    } catch (Exception ex) {
      System.out.println(" error in connection  : " + ex);
    }
    return this.con;
  }
}

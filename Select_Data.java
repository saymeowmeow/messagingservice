
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

      this.con = DriverManager.getConnection("jdbc:edb://", "username", "password");

      this.st = this.con.createStatement();
      this.rs = this.st.executeQuery(query);
    } catch (Exception ex) {
      System.out.println("error  DB :   " + ex);
    }
    return this.rs;
  }
}

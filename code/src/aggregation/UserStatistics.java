package aggregation;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;

public class UserStatistics {
	
	private Connection con;
	private String url = "jdbc:mysql://localhost:3306/ads";
	private String driverName = "com.mysql.jdbc.Driver";
	private String user = "ads_account";
	private String password = "adsisgaaf";
	
	public UserStatistics(){
		this.setQueryLogTable();
	}
	
	protected Connection getConnection(){
		if(con==null)
			try {
				//Register the JDBC driver for MySQL.
				Class.forName(driverName);
				con = DriverManager.getConnection(url,user, password);
			} catch(Exception e){
				e.printStackTrace();
			}
			return con;
	}
	
	/**
	 * Zorgt ervoor dat de log database bestaat.
	 */
	public void setQueryLogTable(){
		
		Connection con = getConnection();
		Statement st;
		try {
			st = con.createStatement();
			
			String query = "CREATE IF NOT EXISTS TABLE `querylog`(`id` INT UNSIGNED NOT NULL AUTO_INCREMENT,  "
					+ "`timestamp` INT UNSIGNED NOT NULL,  "
					+ "`start` INT UNSIGNED NOT NULL,  "
					+ "`extent` INT UNSIGNED NOT NULL,  "
					+ "`factor` INT UNSIGNED NOT NULL,  "
					+ "PRIMARY KEY (`id`));";
			
			int correct = st.executeUpdate(query);
			System.out.println("Dit returned hij: " + correct);
			
			String query2 = "CREATE IF NOT EXISTS TABLE `factorlog`(`factor` INT UNSIGNED NOT NULL, `count` INT UNSIGNED NULL, PRIMARY KEY (`factor`));";
			int correct2 = st.executeUpdate(query2);
			
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}
	
	public void addUserStatistic(long start, long extent, long factor) {
		
		//Maakt een timestamp aan
		Date date = new Date();
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		String timestamp = dateFormat.format(date);					
		
		Statement stLog;
		try {
			stLog = con.createStatement();
			String queryLogUpdate = "INSERT INTO querylog (`timestamp`,`start`,`extent`,`factor`) VALUES('" + timestamp + "', " + start/1000 + ", " + extent/1000 + ", " + factor/1000 + ")";
			stLog.executeUpdate(queryLogUpdate);
			String factorLogUpdate = "INSERT INTO `factorlog` (`factor`,`count`) VALUES (" + factor + ",1) ON DUPLICATE KEY UPDATE `count`=`count`+1";
			stLog.executeUpdate(factorLogUpdate);
			
		} catch (SQLException e) {
			e.printStackTrace();
		}		
		
	}

}

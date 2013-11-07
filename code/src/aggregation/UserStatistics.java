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
	
	/**
	 * Maakt een connectie naar de database
	 * @return De connection
	 */
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
	 * Zorgt ervoor dat de logtabellen worden aangemaakt, mochten ze nog niet bestaan.
	 */
	public void setQueryLogTable(){
		
		Connection con = getConnection();
		Statement st;
		try {
			st = con.createStatement();
			
			//Maakt de eerste tabel aan die alle parameters opslaat.
			String query = "CREATE TABLE IF NOT EXISTS `querylog`("
					+ "`id` int(10) unsigned NOT NULL AUTO_INCREMENT,  "
					+ "`timestamp` varchar(23) NOT NULL,  "
					+ "`start` int(10) unsigned NOT NULL,  "
					+ "`extent` int(10) unsigned NOT NULL,  "
					+ "`factor` int(10) unsigned NOT NULL,  "
					+ "PRIMARY KEY (`id`)) "
					+ "ENGINE=InnoDB DEFAULT CHARSET=utf8;";
			
			int correct = st.executeUpdate(query);
			
			//Maakt de tweede tabel aan, die onthoudt welke factoren worden gevraagd en hoe vaak.
			String query2 = "CREATE TABLE IF NOT EXISTS `factorlog`(`factor` INT UNSIGNED NOT NULL, `count` INT UNSIGNED NULL, PRIMARY KEY (`factor`));";
			int correct2 = st.executeUpdate(query2);
			
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}
	
	/**
	 * Voegt de gebruikerstatistieken toe aan de tabellen.
	 * @param start Het startpunt van de grafiek.
	 * @param extent De grootte van het domein.
	 * @param factor De aggregatiefactor.
	 */
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

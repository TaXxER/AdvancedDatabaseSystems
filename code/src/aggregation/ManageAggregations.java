package aggregation;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

/**
 * This class manages the aggregation levels.
 * 
 * On creation, it retrieves the existing aggregation levels from the database.
 * It can calculate the optimal existing aggregation level to use. (determineDataset( givenFactor))
 * 
 * Through this class, new aggregation levels can be created and existing ones removed.
 * 
 * It has also functions to caclulate the percentage of the total size a factor
 * aggregation requires, just as the percentage of the total size of all current aggregations.
 * 
 * @author David
 *
 */

public class ManageAggregations {

	private Connection con;
	ArrayList<Long> aggregationFactors;
	
	// requires active connection.
	public ManageAggregations(Connection con){
		aggregationFactors = new ArrayList<Long>();
		this.con = con;
		addExistingDatasets();
		
		// Eerste keer uit commenten om aggregates te maken.
//		createAggregate(241884  );
//		createAggregate(120942  );
//		createAggregate(60471   );
//		createAggregate(30235   );
//		createAggregate(15118   );
//		createAggregate(7559    );
//		createAggregate(3780    );
//		createAggregate(1890    );
		
		// Eerste keer uit commenten om aggregates te maken.
//		removeAggregate(241884  );
//		removeAggregate(120942  );
//		removeAggregate(60471   );
//		removeAggregate(30235   );
//		removeAggregate(15118   );
//		removeAggregate(7559    );
//		removeAggregate(3780    );
//		removeAggregate(1890    );
		
	}
	
	
	public double factorPercentageSize(long givenFactorInSeconds){
		if(givenFactorInSeconds==1)
			return 0.0;
		
		// In theory, percentage of factor is 1/factor (because factor combines a factor amount of tuples.
		// however, our factor is in seconds while dataset is 1/min, therefore, it is 1/(factor/60);
		double tmp1 = (givenFactorInSeconds / 60);
		double tmp2 = 1 / tmp1;
		return tmp2;
		
	}
	
	public double totalAggregationsPercentageSize(){
		double resultPercentage = 0.0;
		
		for(long fac : aggregationFactors){
			double tmp = factorPercentageSize(fac);
			resultPercentage += tmp;
		}
		
		return resultPercentage;
		
	}

	// Method that determines the correct aggregated dataset.
	// First check: IS THERE factor > given factor && factor * 0.5 < given factor? (we show up to 300 points less)
	// OTherwise largest existing factor < given factor. 
	public long determineDataset(long givenFactor){
		long resultingFactor = 1;
		
		for(long fac : aggregationFactors){
			if (fac > givenFactor &&  (fac*0.5) < givenFactor){
				return fac;
			}
			if (fac < givenFactor &&  fac > resultingFactor){
				resultingFactor = fac;
			}
		}
		
		return resultingFactor;
	}
	
	public void createAggregate(long factorInSeconds){

		if(con==null) {
			System.out.println("ManageAggregations: no connection");
			return; 
		}
		Statement st;
		try {
				System.out.println("Creating aggregated dataset with factor: "+(factorInSeconds));
				String createTable = "create table dataset_"+(factorInSeconds)+" ( ID int, timed long, PEGEL double)";
				String insertData = "insert into dataset_"+(factorInSeconds)+" (ID, timed, PEGEL) select ID, timed, PEGEL from dataset_1 group by timed div "+(factorInSeconds);
				st = con.createStatement();
				long starttime = System.currentTimeMillis();
				st.execute(createTable);
				System.out.println("Create table time:"+(System.currentTimeMillis()-starttime));
				
				starttime = System.currentTimeMillis();
				Statement st2 = con.createStatement();
				st2.execute(insertData);
				System.out.println("insert into time:"+(System.currentTimeMillis()-starttime));
				
				// insert factor into aggregatedFactor list.
				aggregationFactors.add(factorInSeconds);
			} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void removeAggregate(long factorInSeconds){

		if(con==null) {
			System.out.println("ManageAggregations: no connection");
			return; 
		} else if (factorInSeconds == 1){
			System.out.println("ManageAggregations: dataset with factor 1 cannot be dropped");
			return;
		}
		Statement st;
		try {
				System.out.println("Removing aggregated dataset with factor: "+(factorInSeconds));
				String dropTable = "drop table dataset_"+(factorInSeconds);
				st = con.createStatement();
				st.execute(dropTable);
				
				// remove factor from aggregatedFactor list.
				aggregationFactors.remove(factorInSeconds);
			} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	// Retrieves existing aggregated datasets from database.
	public void addExistingDatasets(){
			
		if(con==null) {
			System.out.println("ManageAggregations: no connection");
			return; 
		} 
		Statement st;
		try {
				String getAggregateTables = "SELECT TABLE_NAME FROM information_schema.tables where TABLE_NAME like 'dataset%'";
				st = con.createStatement();
				ResultSet rs = st.executeQuery(getAggregateTables);
				
				while(rs.next()){
					// Retrieve string, take number from name, convert to long.
					aggregationFactors.add(Long.parseLong(rs.getString(1).substring(8)));
				}

			} catch (SQLException e) {
			e.printStackTrace();
		}
		System.out.println("ManageAggregations: added existing aggregation factors: "+aggregationFactors.toString());
	}

}






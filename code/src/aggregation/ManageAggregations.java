package aggregation;

import graph.JdbcYIntervalSeries;
import gurobi.GRB;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class manages the aggregation levels.
 * 
 * On creation, it retrieves the existing aggregation levels from the database.
 * It can calculate the optimal existing aggregation level to use. (determineDataset( givenFactor))
 * 
 * Through this class, new aggregation levels can be created and existing ones removed.
 * 
 * It has also functions to calculate the percentage of the total size a factor
 * aggregation requires, just as the percentage of the total size of all current aggregations.
 * 
 * @author David
 *
 */

public class ManageAggregations {
	public static final Integer MAX_RESOLUTION  	= JdbcYIntervalSeries.MAX_RESOLUTION;
	public static final Integer MIN_RESOLUTION		= MAX_RESOLUTION/2;
	
	private Connection con;
	private boolean preciseMode;
	Set<Long> existingFactors;
	Set<Long> preciseModeFactors;
	Set<Long> fastModeFactors;
	Map<Long, Triple> queryToFacResCosTupleMap = null;
	
	// requires active connection.
	public ManageAggregations(Connection con, boolean preciseMode){
		existingFactors = new HashSet<Long>();
		this.con = con;
		this.preciseMode = preciseMode;
		addExistingDatasets();
		
		// INSTELLEN: Wel of niet eerst alle aggregates verwijderen
		// removeAllAggregates();
		
		/*
		 *  For Precise mode
		 *  Choose one of the following ILP-optimized factor-arrays below by outcommenting
		 */		
		// Array met optimale factors voor priemqueries
		// List<Integer> preciseModeMinFactors = new ArrayList<Integer>(Arrays.asList(600, 2791, 2797, 2801, 2803, 2819, 2833, 2837, 2843, 2851, 2857, 2861, 2879, 2887, 2897, 2903, 2909, 2917, 2927, 2939, 2953, 2957, 2963, 2969, 2971, 2999, 3001, 3011, 3019, 3023, 3037, 3041, 3049, 3061, 3067, 3079, 3083, 3089, 3109, 3119, 3121, 3137, 3163, 3167, 3169, 3180, 3181, 3187, 3191, 3203, 3209, 3217, 3221, 3229, 3251, 3253, 3257, 3259, 3271, 3299, 3301, 3307, 3313, 3319, 3323, 3329, 3331, 3343, 3347, 3359, 3361, 3371, 3373, 3389, 3391, 3407, 3413, 3433, 3449, 3457, 3461, 3463, 3467, 3469, 3491, 3499, 3511, 3517, 3527, 3529, 3533, 3539, 3540, 3541, 3547, 3557, 3559, 3571, 3581, 3583, 3593, 3607, 3613, 3617, 3623, 3631, 3637, 3643, 3659, 3671, 3673, 3677, 3691, 3697, 3701, 3709, 3719, 3721, 3727, 3733, 3739, 3761, 3767, 3769, 3779, 3793, 3797, 3803, 3821, 3823, 3833, 3847, 3851, 3853, 3863, 3877, 3881, 3889, 3898, 3902, 3907, 3911, 3917, 3919, 3923, 3929, 3931, 3943, 3946, 3947, 3958, 3967, 3974, 3986, 3989, 3994, 3998, 4001, 4003, 4006, 4007, 4013, 4019, 4021, 4022, 4027));
		
		// Array met optimale factors voor priemqueries + traagste 10 queries na priem
		// List<Integer> preciseModeMinFactors = new ArrayList<Integer>(Arrays.asList(600, 2654, 2791, 2797, 2801, 2803, 2819, 2833, 2837, 2843, 2851, 2857, 2861, 2879, 2887, 2897, 2903, 2909, 2917, 2927, 2939, 2953, 2957, 2963, 2969, 2971, 2999, 3001, 3011, 3019, 3023, 3037, 3041, 3049, 3061, 3067, 3079, 3083, 3089, 3109, 3119, 3121, 3137, 3163, 3167, 3169, 3180, 3181, 3187, 3191, 3203, 3209, 3217, 3221, 3225, 3229, 3251, 3253, 3257, 3259, 3271, 3299, 3301, 3307, 3313, 3319, 3323, 3329, 3331, 3343, 3347, 3359, 3361, 3371, 3373, 3389, 3391, 3407, 3413, 3433, 3449, 3450, 3457, 3461, 3463, 3467, 3469, 3491, 3499, 3511, 3517, 3525, 3527, 3529, 3533, 3539, 3540, 3541, 3547, 3557, 3559, 3571, 3581, 3583, 3593, 3607, 3613, 3617, 3623, 3631, 3637, 3643, 3659, 3671, 3673, 3677, 3691, 3697, 3701, 3709, 3719, 3721, 3727, 3733, 3739, 3761, 3767, 3769, 3779, 3793, 3797, 3803, 3821, 3823, 3833, 3847, 3851, 3853, 3863, 3877, 3881, 3889, 3907, 3911, 3917, 3919, 3923, 3929, 3931, 3943, 3947, 3967, 3985, 3988, 3989, 4001, 4003, 4007, 4009, 4013, 4019, 4021, 4027, 4029, 4031));
		
		// 2e iteratie toevoeging traagste 10 queries
		//List<Integer> preciseModeMinFactors = new ArrayList<Integer>(Arrays.asList(600, 2654, 2851, 2857, 2861, 2879, 2887, 2897, 2900, 2903, 2909, 2917, 2927, 2939, 2953, 2957, 2963, 2969, 2971, 2999, 3001, 3011, 3019, 3023, 3037, 3041, 3049, 3061, 3067, 3075, 3079, 3083, 3089, 3109, 3119, 3121, 3137, 3163, 3167, 3169, 3181, 3187, 3191, 3203, 3209, 3217, 3221, 3225, 3229, 3251, 3253, 3257, 3259, 3271, 3299, 3301, 3307, 3313, 3319, 3323, 3329, 3331, 3343, 3347, 3359, 3361, 3371, 3373, 3389, 3391, 3407, 3413, 3433, 3449, 3457, 3461, 3463, 3467, 3469, 3491, 3499, 3511, 3517, 3525, 3527, 3529, 3533, 3539, 3540, 3541, 3547, 3557, 3559, 3571, 3581, 3583, 3593, 3607, 3613, 3617, 3623, 3631, 3637, 3643, 3659, 3671, 3673, 3677, 3691, 3697, 3701, 3709, 3719, 3721, 3727, 3733, 3739, 3761, 3767, 3769, 3779, 3793, 3797, 3803, 3821, 3823, 3833, 3847, 3851, 3853, 3863, 3877, 3881, 3889, 3902, 3907, 3911, 3917, 3919, 3923, 3929, 3931, 3943, 3946, 3947, 3958, 3964, 3966, 3967, 3974, 3979, 3985, 3986, 3988, 3989, 3994, 3998, 4001, 4003, 4006, 4007, 4009, 4013, 4019, 4021, 4022, 4027, 4029));
		
		// Array met optimale factors voor priemqueries en queries >= 3000
		Set<Integer> preciseModeMinFactors = new HashSet<Integer>(Arrays.asList(1, 300, 2200, 2220, 2300, 2500, 2580, 2600, 2810, 2820, 2900, 2925, 2930, 3062, 3075, 3086, 3098, 3100, 3106, 3118, 3134, 3142, 3158, 3166, 3180, 3194, 3202, 3214, 3218, 3226, 3238, 3242, 3252, 3254, 3274, 3314, 3324, 3326, 3334, 3338, 3386, 3394, 3398, 3418, 3442, 3446, 3466, 3482, 3494, 3506, 3518, 3540, 3554, 3566, 3574, 3578, 3602, 3606, 3622, 3642, 3646, 3660, 3662, 3678, 3694, 3702, 3714, 3715, 3716, 3719, 3722, 3727, 3733, 3734, 3739, 3742, 3746, 3747, 3748, 3754, 3755, 3758, 3761, 3764, 3767, 3769, 3777, 3778, 3779, 3785, 3786, 3788, 3793, 3797, 3802, 3803, 3805, 3812, 3814, 3821, 3823, 3826, 3831, 3833, 3837, 3845, 3846, 3847, 3849, 3851, 3853, 3858, 3862, 3863, 3865, 3866, 3867, 3868, 3873, 3877, 3880, 3881, 3882, 3884, 3889, 3891, 3898, 3902, 3903, 3907, 3908, 3909, 3911, 3917, 3918, 3919, 3921, 3923, 3929, 3931, 3932, 3935, 3943, 3946, 3947, 3954, 3957, 3958, 3963, 3964, 3966, 3967, 3974, 3981, 3985, 3986, 3988, 3989, 3994, 3998, 4001, 4003, 4006, 4007, 4013, 4019, 4021, 4022, 4027));
		this.preciseModeFactors = minFacsToSecFacs(preciseModeMinFactors);
		
		/*
		 *	For Fast mode
		 */
		this.fastModeFactors = new HashSet<Long>(Arrays.asList(1890L,3780L,7559L,15118L,30235L,60471L,120942L,241884L));
		
		// Remove all existing factors not needed for fast mode or precise mode
		Set<Long> deletionSet = new HashSet<Long>();
		deletionSet.addAll(existingFactors);
		deletionSet.removeAll(fastModeFactors);
		deletionSet.removeAll(preciseModeFactors);
		removeAggregates(deletionSet);
		
		// Create all aggregates not existing
		Set<Long> creationSet = new HashSet<Long>();
		creationSet.addAll(fastModeFactors);
		creationSet.addAll(preciseModeFactors);
		creationSet.removeAll(existingFactors);
		createAggregates(creationSet);
		
		Set<Long> secFactors = preciseMode ? preciseModeFactors : fastModeFactors;
		// Create aggregates
		createAggregates(secFactors);
		
		if(preciseMode)
			preCalculateQueryPlans();
	}
	
	public void setPreciseMode(boolean preciseMode){
		this.preciseMode = preciseMode;
		if(preciseMode && queryToFacResCosTupleMap==null)
			preCalculateQueryPlans();
	}
	
	private void preCalculateQueryPlans(){
		/*
		 *  For Precise Mode
		 *  Calculate each optimal factor for each possible query
		 */
		queryToFacResCosTupleMap = new HashMap<Long, Triple>();
		List<Integer> theoreticalQueryFacs = Aggregator.getTheoreticalFactors();
		List<Integer> minFacs = null;
		try {
			minFacs = new ArrayList<Integer>(secFacsToMinFacs(preciseModeFactors));
		} catch (Exception e) {
			e.printStackTrace();
		}
		for(int i=0;i<theoreticalQueryFacs.size();i++){
			// Set result variables
			double min_cost = GRB.INFINITY;
			int resolution = -1;
			int factor = -1;
			
			// Calculate results
			for(int j=0;j<minFacs.size();j++){
				double min_cost_inner = GRB.INFINITY;
				int resolution_inner = -1;
				for(int r=MIN_RESOLUTION;r<=MAX_RESOLUTION;r++){
					double res = (double) r;
					// query komt overeen met aantal geselecteerde datapunten. Q-waarden gaan uit van resolutie 600.
					// bij lagere resolutie meer datapunten per beeldpunt aggregeren om in totaal evenveel datapunten te plotten. 
					double res_adj_factor = theoreticalQueryFacs.get(i) * (MAX_RESOLUTION/res);
					double cost = GRB.INFINITY;
					// grafiek is alleen plotbaar wanneer een geheel aantal datapunten samen worden genomen
					if (res_adj_factor == Math.floor(res_adj_factor)){
						// 	costs the resolution adjusted factor divided by the largest aggregate that divides it
					    cost = res_adj_factor % minFacs.get(j) == 0 ? res_adj_factor/minFacs.get(j) : GRB.INFINITY;
					}
					
					if(cost < min_cost_inner){
						resolution_inner = r;
						min_cost_inner = cost;
					}
				}
				if(min_cost_inner < min_cost){
					min_cost = min_cost_inner;
					resolution = resolution_inner;
					factor = minFacs.get(j);
				}
			}
			Long givenSecFac  = new Long(theoreticalQueryFacs.get(i)) * 60;
			// Sec
			Long storedSecFac = factor==1 ? factor : new Long(factor) * 60;
			queryToFacResCosTupleMap.put(givenSecFac, new Triple(storedSecFac, resolution, min_cost));
			if(min_cost>3000.0)
				System.out.println("plot query "+givenSecFac+" with res "+resolution+" using factor "+storedSecFac+" for cost "+min_cost);
		}
	}
	
	public long determineDataset(long givenFactor){
		return preciseMode ? queryToFacResCosTupleMap.get(givenFactor).factor : determindeFastDataSet(givenFactor);
	}
	
	private long determindeFastDataSet(long givenFactor){
		long resultingFactor = 1;
		for(long fac : fastModeFactors){
			if (fac > givenFactor &&  (fac*0.5) < givenFactor){
				return fac;
			}
			if (fac < givenFactor &&  fac > resultingFactor){
				resultingFactor = fac;
			}
		}
		return resultingFactor;
	}
	
	private Set<Long> minFacsToSecFacs(Set<Integer> minFacs){
		Set<Long> secFacs = new HashSet<Long>();
		for(Integer minFac:minFacs)
			secFacs.add(new Long(minFac)*60);
		return secFacs;			
	}
	
	private Set<Integer> secFacsToMinFacs(Set<Long> secFacs) throws Exception{
		Set<Integer> minFacs = new HashSet<Integer>(); 
		for(Long secFac:secFacs){
			// Neglect secFac==1, because 1 second aggregation equals 1 datapoint aggregation
			if(secFac==1){
				minFacs.add(1);
				continue;
			}				
			if(secFac%60 != 0)
				throw new Exception("Unable to convert secFacs to minFacs: "+secFac+" not divisible by 60");
			minFacs.add((int)(secFac/60));
		}
		return minFacs;			
	}
	
	private void createAggregates(Set<Long> secFacs){
		for(long secFac:secFacs)
			createAggregate(secFac);
	}
	
	private void createAggregate(long factorInSeconds){
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
				existingFactors.add(factorInSeconds);
			} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	private void removeAggregates(Set<Long> secFacs){
		// Deepclone secFacs to prevent ConcurrentModificationException
		Set<Long> aggregatesToRemove = new HashSet<Long>();
		for(Long secFac:secFacs)
			aggregatesToRemove.add(secFac);
				
		for(long secFac:aggregatesToRemove)
			removeAggregate(secFac);
	}
	
	private void removeAggregate(long factorInSeconds){
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
				existingFactors.remove(factorInSeconds);
			} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	// Retrieves existing aggregated datasets from database.
	private void addExistingDatasets(){			
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
					existingFactors.add(Long.parseLong(rs.getString(1).substring(8)));
				}

			} catch (SQLException e) {
			e.printStackTrace();
		}
		System.out.println("ManageAggregations: added existing aggregation factors: "+existingFactors.toString());
	}
	
	private class Triple { 
	  public final Long factor; 
	  public final Integer resolution;
	  public final Double cost;
	  public Triple(Long x, Integer y, Double z) { 
	    this.factor = x; 
	    this.resolution = y; 
	    this.cost = z;
	  }
	} 

}
package aggregation;

import gurobi.GRB;
import gurobi.GRBEnv;
import gurobi.GRBException;
import gurobi.GRBLinExpr;
import gurobi.GRBModel;
import gurobi.GRBVar;

import java.util.ArrayList;
import java.util.List;

public class Aggregator {
	// max(timed) - min(timed)
	public static final Integer NUM_SECONDS 		= 145130880;
	// 2418848
	public static final Integer NUM_DATAPOINTS		= NUM_SECONDS / 60;
	public static final Integer MIN_RESOLUTION		= 300;
	public static final Integer MAX_RESOLUTION  	= 600;
	public static final Integer NUM_FACTORS	    	= (int) Math.ceil((double)NUM_DATAPOINTS/(double)MAX_RESOLUTION);
	public static final Double	STORAGE_CONSTRAINT	= 0.05;
	
	public static void main(String[] args){
		try{
			System.out.println("Preamble");
			// Calculate multiples of 60 up to NUM_FACTORS
			List<Integer> multiples = new ArrayList<Integer>();

			for(int i=1; i<=NUM_FACTORS ;i++){
				multiples.add(i);
			}
			
			// Define set of queries
			List<Integer> Q = new ArrayList<Integer>(multiples);			
			
			// Define set of possible pre-aggregation levels: all multiples of 60 between 60 and 241884
			List<Integer> A = new ArrayList<Integer>(multiples);
			System.out.println(A);
			
			System.out.println("Create model");
			// Model		
			GRBEnv env = new GRBEnv();
			GRBModel model = new GRBModel(env);
			model.set(GRB.StringAttr.ModelName, "factors");
		
			System.out.println("Define vars");
			// Costs for executing Qi with level Aj			
			Double[][] L = new Double[Q.size()][A.size()];
			// Binary containing whether Aj answers Qi
			GRBVar[][] X = new GRBVar[Q.size()][A.size()];
			// Binary whether Aj is part of the solution
			GRBVar[]   Y = new GRBVar[A.size()];
			
			// Start at 1, factor 1 is not a variable
			Y[0] = model.addVar(1.0, 1.0, 1.0, GRB.BINARY, "Y[0]");
			for(int j=1;j<A.size();j++){
				Y[j] = model.addVar(0.0, 1.0, 1.0, GRB.BINARY, "Y["+j+"]");
			}
			
			System.out.println("Calculate costs");
			// Objective function variables
			for(int i=0;i<Q.size();i++){
				for(int j=0;j<A.size();j++){
					double min_cost = GRB.INFINITY;
					int resolution = -1;
					for(int r=MIN_RESOLUTION;r<=MAX_RESOLUTION;r++){
						double res = (double) r;
						// query komt overeen met aantal geselecteerde datapunten. Q-waarden gaan uit van resolutie 600.
						// bij lagere resolutie meer datapunten per beeldpunt aggregeren om in totaal evenveel datapunten te plotten. 
						double res_adj_factor = Q.get(i) * (MAX_RESOLUTION/res);
						double cost = GRB.INFINITY;
						// grafiek is alleen plotbaar wanneer een geheel aantal datapunten samen worden genomen
						if (res_adj_factor == Math.floor(res_adj_factor)){
							// 	costs the resolution adjusted factor divided by the largest aggregate that divides it
						    cost = res_adj_factor % A.get(j) == 0 ? res_adj_factor/A.get(j) : GRB.INFINITY;
						}
						
						if(cost<min_cost){
							resolution = r;
							min_cost = cost;
						}
					}
					L[i][j] = min_cost;
//					System.out.println("L["+i+"]["+j+"] = "+L[i][j]);
					X[i][j] = model.addVar(0.0, 1.0, L[i][j], GRB.BINARY, "X["+i+"]["+j+"]");
				}
			}
			
			//System.out.println(L);
			
			System.out.println("Update model");
			model.update();
			
			// Objective function
			GRBLinExpr objective = new GRBLinExpr();
			for(int i=0;i<Q.size();i++){
				for(int j=0;j<A.size();j++){
					objective.addTerm(L[i][j], X[i][j]);
				}
			}

			model.setObjective(objective, GRB.MINIMIZE);
			
			System.out.println("Define constraints");
			// Constraint line 1
			// All queries i in Q
			for(int i=0;i<Q.size();i++){
				GRBLinExpr expr = new GRBLinExpr();
				// The total number of pre-aggregation levels that answer it
				for(int j=0;j<A.size();j++){
					expr.addTerm(1.0,X[i][j]);
				}
				// Must be exactly one
				model.addConstr(expr, GRB.EQUAL, 1.0, "line1");
			}
			
			// Constraint line 2
			// All pre-aggregation levels j in A
			for(int j=0;j<A.size();j++){
				GRBLinExpr expr = new GRBLinExpr();
				// The total number of 
				for(int i=0;i<Q.size();i++){
					expr.addTerm(1.0, X[i][j]);
				}
				GRBLinExpr max = new GRBLinExpr();
				max.addTerm((double)Q.size(), Y[j]);
				model.addConstr(expr, GRB.LESS_EQUAL, max, "line2");
			}
			
			// Constraint line 3
			GRBLinExpr expr = new GRBLinExpr();
			for(int j=0;j<A.size();j++){
				expr.addTerm(1.0/A.get(j), Y[j]);
			}
			model.addConstr(expr, GRB.LESS_EQUAL, 1+STORAGE_CONSTRAINT, "line3");
			
			// Integrate variables into model
			System.out.println("Update model");
			model.update();
			System.out.println("Start optimization");
			model.optimize();
			model.write("aggregator_model.sol");
			
			
			// Print results in console
			List<Integer> factors = new ArrayList<Integer>();
			double[] results = model.get(GRB.DoubleAttr.X, Y);
			for(int i=0;i<results.length;i++){
				if(results[i]==1.0)
					factors.add((i+1));
			}
			System.out.println("factors: "+factors);
			
			// Calculate the cheapest factor/datapoint combinations for each query
			for(int i=0;i<Q.size();i++){
				// Set result variables
				double min_cost = GRB.INFINITY;
				int resolution = -1;
				int factor = -1;
				
				// Calculate results
				for(int j=0;j<factors.size();j++){
					double min_cost_inner = GRB.INFINITY;
					int resolution_inner = -1;
					for(int r=MIN_RESOLUTION;r<=MAX_RESOLUTION;r++){
						double res = (double) r;
						// query komt overeen met aantal geselecteerde datapunten. Q-waarden gaan uit van resolutie 600.
						// bij lagere resolutie meer datapunten per beeldpunt aggregeren om in totaal evenveel datapunten te plotten. 
						double res_adj_factor = Q.get(i) * (MAX_RESOLUTION/res);
						double cost = GRB.INFINITY;
						// grafiek is alleen plotbaar wanneer een geheel aantal datapunten samen worden genomen
						if (res_adj_factor == Math.floor(res_adj_factor)){
							// 	costs the resolution adjusted factor divided by the largest aggregate that divides it
						    cost = res_adj_factor % factors.get(j) == 0 ? res_adj_factor/factors.get(j) : GRB.INFINITY;
						}
						
						if(cost < min_cost_inner){
							resolution_inner = r;
							min_cost_inner = cost;
						}
					}
					if(min_cost_inner < min_cost){
						min_cost = min_cost_inner;
						resolution = resolution_inner;
						factor = factors.get(j);
					}
				}
				System.out.println("plot query "+Q.get(i)+" with res "+resolution+" using factor "+factor+" for cost "+min_cost);
			}
			
		}catch(GRBException e){
			System.out.println("Error code: "+e.getErrorCode()+". "+e.getMessage());
		}
		
	}

}
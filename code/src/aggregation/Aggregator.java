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
	public static final Integer NUM_SECONDS 	= 145130880;
	public static final Integer MAX_RESOLUTION  = 600;
	public static final Integer NUM_FACTORS	    = NUM_SECONDS / MAX_RESOLUTION;
	public static void main(String[] args){
		try{
			// Calculate multiples of 60 up to NUM_FACTORS
			List<Integer> multiples = new ArrayList<Integer>();
			int b = 60;
			int counter = 60;
			while (counter <= NUM_FACTORS){
				multiples.add(counter);
				counter += b;			
			}
			
			// Define set of queries
			List<Integer> Q = new ArrayList<Integer>(multiples);
			
			// Define set of possible pre-aggregation levels: all multiples of 60 between 60 and 241884
			List<Integer> A = new ArrayList<Integer>(multiples);
			
			// Model		
			GRBEnv env = new GRBEnv();
			GRBModel model = new GRBModel(env);
			model.set(GRB.StringAttr.ModelName, "factors");
		
			// Costs for executing Qi with level Aj			
			Double[][] L = new Double[Q.size()][A.size()];
			// Binary containing whether Aj answers Qi
			GRBVar[][] X = new GRBVar[Q.size()][A.size()];
			// Binary whether Aj is part of the solution
			GRBVar[]   Y = new GRBVar[A.size()];
			
			for(int j=0;j<A.size();j++){
				Y[j] = model.addVar(0.0, 1.0, 1.0, GRB.BINARY, "Yj");
			}
			
			// Optimization function
			for(int i=0;i<Q.size();i++){
				for(int j=0;j<A.size();j++){
					L[i][j] = Q.get(j)%A.get(i) == 0 ? 0:GRB.INFINITY;
					X[i][j] = model.addVar(0.0, 1.0, L[i][j], GRB.BINARY, "Xij");
				}
			}
			
			// Constraint line 1
			// All queries i in Q
			for(int i=0;i<X.length;i++){
				GRBLinExpr expr = new GRBLinExpr();
				// The total number of pre-aggregation levels that answer it
				for(int j=0;j<X[i].length;j++){
					expr.addTerm(1.0,X[i][j]);
				}
				// Must be exactly one
				model.addConstr(expr, GRB.EQUAL, 1.0, "line1");
			}
			
			// Constraint line 2
			// All pre-aggregation levels j in A
			for(int j=0;j<X[0].length;j++){
				GRBLinExpr expr = new GRBLinExpr();
				// The total number of 
				for(int i=0;i<X.length;i++){
					expr.addTerm(1.0, X[i][j]);
				}
				GRBLinExpr max = new GRBLinExpr();
				max.addTerm((double)Q.size(), Y[j]);
				model.addConstr(expr, GRB.LESS_EQUAL, max, "line2");
			}
			
			// Integrate variables into model
			model.update();
			
			// Add constraints
			GRBLinExpr expr;
			
			
		}catch(GRBException e){
			System.out.println("Error code: "+e.getErrorCode()+". "+e.getMessage());
		}
		

	}

}

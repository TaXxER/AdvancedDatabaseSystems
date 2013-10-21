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
		
			
			// Optimization function
			Double[][] L = new Double[Q.size()][A.size()];
			GRBVar[][] X = new GRBVar[Q.size()][A.size()];
			for(int i=0;i<Q.size();i++){
				for(int j=0;j<A.size();j++){
					L[i][j] = Q.get(j)%A.get(i) == 0 ? 0:GRB.INFINITY;
					X[i][j] = model.addVar(0.0, 1.0, L[i][j], GRB.BINARY, "Xij");
				}
			}
			
			// Constraint line 1
			for(int i=0;i<X.length;i++){
				GRBLinExpr expr = new GRBLinExpr();
				for(int j=0;j<X[i].length;j++){
					expr.addTerm(1.0,X[i][j]);
				}
				model.addConstr(expr, GRB.EQUAL, 1.0, "line1");
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

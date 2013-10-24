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
	public static final Integer NUM_SECONDS 		= 145130880;
	public static final Integer MAX_RESOLUTION  	= 600;
	public static final Integer NUM_FACTORS	    	= NUM_SECONDS / MAX_RESOLUTION;
	public static final Double	STORAGE_CONSTRAINT	= 0.05;
	
	public static void main(String[] args){
		try{
			System.out.println("Preamble");
			// Calculate multiples of 60 up to NUM_FACTORS
			List<Integer> multiples = new ArrayList<Integer>();
			int b = 60;
			int counter = 60;
			while (counter <= NUM_FACTORS){
				multiples.add(counter/60);
				counter += b;			
			}
			
			// Define set of queries
			//List<Integer> Q = new ArrayList<Integer>(multiples);
			List<Integer> Q = new ArrayList<Integer>();
			Q.add(21);
			Q.add(2);
			Q.add(3);
			Q.add(4);
			Q.add(5);
			Q.add(6);
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
			
			// Objective function variables
			for(int i=0;i<Q.size();i++){
				for(int j=0;j<A.size();j++){
					L[i][j] = Q.get(i)%A.get(j) == 0 ? Q.get(i)/A.get(j) :GRB.INFINITY;
					X[i][j] = model.addVar(0.0, 1.0, L[i][j], GRB.BINARY, "X["+i+"]["+j+"]");
				}
			}
			
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
				System.out.println("storage: "+1.0/A.get(j));
				System.out.println("Aj: "+A.get(j));
			}
			model.addConstr(expr, GRB.LESS_EQUAL, 1+STORAGE_CONSTRAINT, "line3");
			
			// Integrate variables into model
			System.out.println("Update model");
			model.update();
			System.out.println("Start optimization");
			model.optimize();
			model.write("aggregator_model.sol");
//			System.out.println("Optimization finished");
//			
			List<Integer> factors = new ArrayList<Integer>();
			double[] results = model.get(GRB.DoubleAttr.X, Y);
			for(int i=0;i<results.length;i++){
				if(results[i]==1.0)
					factors.add((i+1));
			}
//			double[][] result = model.get(GRB.DoubleAttr.X, X);
//			for(int i=0;i<result.length;i++){
//				for(int j=0;j<result[i].length;j++)
//				System.out.println("result["+i+"]["+j+"]"+result[i][j]);
//			}
			System.out.println("factors: "+factors);
		}catch(GRBException e){
			System.out.println("Error code: "+e.getErrorCode()+". "+e.getMessage());
		}
		

	}

}

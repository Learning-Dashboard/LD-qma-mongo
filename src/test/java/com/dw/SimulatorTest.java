package com.dw;

import java.io.IOException;
import java.util.Collection;

import DTOs.FactorEvaluationDTO;
import simulation.Model;
import simulation.Simulator;
import util.Connection;

public class SimulatorTest {
	public static void main(String[] args)  {

		// Set correct values before running tests
		Connection.initConnection("", 0, "", null, null);
		Model model = Simulator.createModel( "test", "2023-10-14");
		Simulator.factorPrinter( model.getFactors() );

		model.setMetric("test-mB-2023-10-14", 0.5);
		Collection<FactorEvaluationDTO> factors = model.simulate();
		Simulator.factorPrinter( factors );

		model.setMetric("test-mB-2023-10-14", 0.0);
		factors = model.simulate();
		Simulator.factorPrinter( factors );
		Connection.closeConnection();
	}
	
}

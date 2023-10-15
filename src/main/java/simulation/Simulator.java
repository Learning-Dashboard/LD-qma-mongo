package simulation;

import DTOs.FactorEvaluationDTO;
import DTOs.MetricEvaluationDTO;
import evaluation.Factor;
import evaluation.Metric;
import org.bson.Document;
import util.Queries;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.List;

public class Simulator {

	// Convert String date (like 2019-01-15) into LocalDate
	final static DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
	
	/**
	* Fetch data for Model creation, check data is available, and create Model
	*
	* @param projectId The ID of the project which owns the metrics and factors of the Model
	* @param evaluationDate The date of the metrics and factors evaluation
	*
	* @return The created Model with the fetched data
	*/
	public static Model createModel( String projectId, String evaluationDate ) {
		LocalDate localEvaluationDate = LocalDate.parse(evaluationDate, dtf);

		List<MetricEvaluationDTO> metrics = Metric.getEvaluations( projectId, localEvaluationDate, localEvaluationDate );
		List<FactorEvaluationDTO> factors = Factor.getEvaluations( projectId, localEvaluationDate, localEvaluationDate );
		List<Document> relations = Queries.getFactorMetricsRelations(  projectId, evaluationDate );

		if ( metrics.size() == 0 )
			throw new IllegalArgumentException("No metrics found for projectId " + projectId + ", evaluationDate " + evaluationDate);
		if ( factors.size() == 0 )
			throw new IllegalArgumentException("No factors found for projectId " + projectId + ", evaluationDate " + evaluationDate);
		if ( relations.size()  == 0 )
			throw new IllegalArgumentException("No relations found for projectId " + projectId + ", evaluationDate " + evaluationDate);

		return new Model(metrics, factors, relations);
	}

	/**
	* Helper: print factor values
 	*
	* @param factors The list of factor evaluations to be printed
	*/
	public static void factorPrinter( Collection<FactorEvaluationDTO> factors ) {
		for ( FactorEvaluationDTO fedto : factors )
			System.out.println(fedto.getFactorEntryID(0) + ": " + fedto.getEvaluations().get(0).getValue());
		System.out.println();
	}

}

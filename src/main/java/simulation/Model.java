package simulation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.bson.Document;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;

import DTOs.ElemenEvaluationtDTO;
import DTOs.EvaluationDTO;
import DTOs.FactorEvaluationDTO;
import DTOs.MetricEvaluationDTO;
import util.Queries;

public class Model {
	
	Logger log = Logger.getLogger(this.getClass().getName());

	private Map<String, FactorEvaluationDTO> mapFactorIdDTO = new HashMap<>();
	private Map<String, MetricEvaluationDTO> mapMetricIdDTO = new HashMap<>();

	private Set<String> relationMetricIdSet = new HashSet<>();
	private Set<String> relationFactorIdSet = new HashSet<>();

	// Records which factorIds (unique) are influenced by a metricId
	private Map<String, Set<String>> influencedFactors = new HashMap<>();
	
	// FactorID -> MetricID -> Weight
	Map< String, Map< String, Double > > impacts = new HashMap<>();
	
	// Set of affected factors
	Set<String> changeFactors = new HashSet<>();
	
	/**
	* Create Model on metrics / factors (DTOs), and relations SearchResponse (qr-eval)
	* Assumption: MetricEvaluationDTOs and FactorEvaluationDTOs contain exactly one EvaluationDTO for a specific evaluationDate
	*
	* @param metrics List of MetricEvaluationDTOs for evaluationDate and projectId
	* @param factors List of FactorEvaluationDTOs for evaluationDate and projectId
	* @param relations SearchResponse of relation query for metric-factor relations, evaluationDate, and projectId
	*/
	public Model( List<MetricEvaluationDTO> metrics, List<FactorEvaluationDTO> factors, List<Document> relations ) {

		for ( MetricEvaluationDTO medto : metrics ) {
			if (medto.getEvaluations().size() > 0)
				this.mapMetricIdDTO.put(medto.getEvaluations().get(0).getID(), medto);
		}
		
		for ( FactorEvaluationDTO fedto : factors ) {
			if (fedto.getEvaluations().size() > 0)
				this.mapFactorIdDTO.put( fedto.getEvaluations().get(0).getID(), fedto );
		}

		readRelations(relations);
	}

	
	/**
	* Simulate change of a metric value:
	* - Lookup MetricEvaluationDTO in mapMetricIdDTO
	* - Change EvaluationDTO to simulated value
	* - Updates the list of changeFactors
	*
	* @param metricId The metricId
	* @param value The simulated value
	*/
	public void setMetric( String metricId, Double value ) {
		MetricEvaluationDTO medto = mapMetricIdDTO.get(metricId);
		if (medto == null)
			throw new IllegalArgumentException( "MetricEvaluationDTO not found: " + metricId );
		
		setEvaluationDTO(value, medto);
		Set<String> factors = influencedFactors.get(metricId);
		if (factors != null)
			this.changeFactors.addAll(factors);
	}
	
	/**
	* Simulate change of for a list of metrics:
	* - lookup MetricEvaluationDTO in mapMetricIdDTO
	* - change EvaluationDTO to simulated value
	* - updates the list of changeFactors
	* @param metrics The list of metrics to be simulated (metricId and simulated value)
	*/
	public void setMetrics (Map <String,Double> metrics){
		for (Map.Entry<String, Double> entry : metrics.entrySet())
			setMetric(entry.getKey(), entry.getValue());
	}
	
	/**
	* Simulate change of a metric value:
	* - Reevaluate influenced factors
	*
	* @return Recomputed factors
	*/
	public Collection<FactorEvaluationDTO> simulate() {	
		for ( String factorId : changeFactors ) {
			Double sumWeights = 0.0;
			Double sumValues = 0.0;
			
			for ( String sourceMetricId : impacts.get(factorId).keySet() ) {
				MetricEvaluationDTO source = mapMetricIdDTO.get(sourceMetricId);
				double metricValue;
				if (source.getEvaluations().size() > 0)
					metricValue = source.getEvaluations().get(0).getValue().doubleValue();
				else metricValue = 0.0;
				Double weight = impacts.get(factorId).get(sourceMetricId);
				sumWeights += weight;
				sumValues += metricValue * weight;
			}
			
			FactorEvaluationDTO fedto = mapFactorIdDTO.get(factorId);
			setEvaluationDTO(sumValues / sumWeights, fedto);
		}
		return mapFactorIdDTO.values();
	}
	
	/**
	* Return the actual Collection of FactorEvaluationDTOs
 	*
	* @return The map of evaluations associated to each factor
	*/
	public Collection<FactorEvaluationDTO> getFactors() {
		return mapFactorIdDTO.values();
	}
	
	/**
	* Read relations
	* - Build Maps influencedFactors and impact
	* - Build sets relationMetricIdSet and relationFactorIdSet
	*
	* @param relations The queried relations to be built
	*/
	private void readRelations(List<Document> relations) {
		// Process documents of relations search
		for ( Document doc : relations ) {
			String metricId = Queries.getStringFromObject(doc.get("sourceId"));
			String factorId = Queries.getStringFromObject(doc.get("targetId"));
			Double weight = getWeight(doc.get("weight"));
			relationMetricIdSet.add(metricId);
			relationFactorIdSet.add(factorId);

			// Fill map influencedFactors: metricId -> Set<factorId>
			if (influencedFactors.containsKey(metricId))
				influencedFactors.get(metricId).add(factorId);
			else {
				Set<String> factorSet = new HashSet<>();
				factorSet.add(factorId);
				influencedFactors.put(metricId, factorSet);
			}
			
			if ( !mapMetricIdDTO.containsKey(metricId) ) {
				// Relation between metric and factor, but no metricId in mapMetricIdDTO. skipped
				log.warning("Inconsistent relation: metricId " + metricId + " referenced by relation, " +
					"but not read from API. Relation-factorId: " + factorId );
				continue;
			}
			if ( !mapFactorIdDTO.containsKey(factorId) ) {
				// Relation between metric and factor, but no factorId  in mapFactorIdDTO. skipped.
				log.warning("Inconsistent relation: factor " + factorId + " referenced by relation-target, " +
					"but not read from API. Relation-metricId: " + metricId );
				continue;
			}

			if ( impacts.containsKey( factorId ) ) {
				Map<String,Double> innerMap = impacts.get( factorId );
				if ( innerMap.containsKey(metricId) ) {
					// More than one relation between metric and factor
					log.warning("relation: " + metricId + "->" + factorId
						+ " already added to impacts. skipped.");
				}
				else innerMap.put(metricId, weight);
			}
			else {
				Map<String, Double> innerMap = new HashMap<>();
				innerMap.put(metricId, weight);
				impacts.put(factorId, innerMap);
			}
		}
		validate();
	}

	/**
	* In some cases relations weight is an Integer instead of a Double
	*
	* @param weight The value of the weight as an Object
	*
	* @return The actual value of the weight as a Double
	*/
	private double getWeight(Object weight) {
		if ( weight instanceof Integer ) return ( (Integer) weight ).doubleValue();
		else return ( Double ) weight;
	}

	private void validate() {
		
		// Number of metrics / factors derived from relations are also delivered by Metric / Factor API
		if ( relationMetricIdSet.size() != mapMetricIdDTO.size() )
			log.warning( "metric count in relations: " + relationMetricIdSet.size() + ", in API: " + mapMetricIdDTO.size());
		if ( relationFactorIdSet.size() != mapFactorIdDTO.size() )
			log.warning("factor count in relations: " + relationFactorIdSet.size() + ", in API: " + mapFactorIdDTO.size());
		
		// MetricId (source of a relation) is contained in mapMetricIdDTO
		for ( String metricId : relationMetricIdSet ) {
			if ( !mapMetricIdDTO.containsKey(metricId) )
				log.warning("Metric in relations, but not delivered by Metric API: " + metricId);
		}
		
		// FactorId (target of a relation) is contained in relationFactorIdSet
		for ( String factorId : relationFactorIdSet ) {
			if ( !mapFactorIdDTO.containsKey(factorId) )
				log.warning("Factor in relations, but not delivered by Factor API: " + factorId);
		}
	}
	


	private void setEvaluationDTO(Double value, ElemenEvaluationtDTO elementEvaluationDTO) {
		if (elementEvaluationDTO.getEvaluations().size() > 0) {
			EvaluationDTO edto = elementEvaluationDTO.getEvaluations().get(0);
			EvaluationDTO simulated = new EvaluationDTO(edto.getID(), edto.getDatasource(),
				edto.getEvaluationDate(), value.floatValue(), edto.getRationale());
			List<EvaluationDTO> ledto = new ArrayList<>();
			ledto.add(simulated);
			elementEvaluationDTO.setEvaluations(ledto);
		}
	}
	
}

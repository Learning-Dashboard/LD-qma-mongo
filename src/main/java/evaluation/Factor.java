package evaluation;

import DTOs.*;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import util.Common;
import util.Constants;
import util.FormattedDates;
import util.Queries;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class Factor {
    private static Map<String, String> IDNames;

    /**
    * This method returns the list of the factors and the last evaluation.
    * The evaluation contains the evaluation date and value.
    *
    * @param projectId identifier of the project
    *
    * @return The list of factors evaluation
    */
    public static List<FactorEvaluationDTO> getEvaluations(String projectId) {
        List<FactorEvaluationDTO> ret;
        List<Document> sr = Queries.getLatest(projectId, Constants.QMLevel.factors);
        ret = Common.processFactorsBuckets(sr);
        return ret;
    }

    /**
    * This method returns the last evaluation of the factor passed as a parameter.
    * The evaluation contains the evaluation date and value.
    *
    * @param projectId identifier of the project
    * @param factorId identifier of the factor
    *
    * @return Factor evaluation
    */
    public static FactorEvaluationDTO getSingleEvaluation(String projectId, String factorId) {
        List<FactorEvaluationDTO> ret;
        FactorEvaluationDTO factorEvaluationDTO = null;
        List<Document> sr = Queries.getLatestElement(projectId, Constants.QMLevel.factors, factorId);
        ret = Common.processFactorsBuckets(sr);
        if (!ret.isEmpty()) factorEvaluationDTO = ret.get(0);
        return factorEvaluationDTO;
    }

    /**
    * This method returns the list of the factors and the evaluations belonging to a specific period defined by the
    * parameters from and to. The evaluation contains the evaluation date and value.
    *
    * @param projectId identifier of the project
    * @param from initial date from the range we are querying
    * @param to final date from the range we are querying
    *
    * @return The list of factors evaluations
    */
    public static List<FactorEvaluationDTO> getEvaluations(String projectId, LocalDate from, LocalDate to) {
        List<FactorEvaluationDTO> ret;
        List<Document> sr = Queries.getRanged(Constants.QMLevel.factors, projectId, from, to);
        ret = Common.processFactorsBuckets(sr);
        return ret;
    }

    /**
    * The external repository have two identifiers for each element, the field used by the repository
    * (hard ID) and the id and evaluation date used by the "users".
    *
    * @param projectId identifier of the project
    * @param qualityFactorID identifier of the quality factor
    * @param evaluationDate date when the evaluation has been computed
    *
    * @return The hard ID of the factor
    */
    public static String getHardID(String projectId, String qualityFactorID, LocalDate evaluationDate) {
        if (projectId.isEmpty()) return qualityFactorID + "-" + FormattedDates.formatDate(evaluationDate);
        else return projectId + "-" + qualityFactorID + "-" + FormattedDates.formatDate(evaluationDate);
    }

    /**
    * This method updates the value of an quality factors in a given date, if it doesn't exist
    * a new quality factor is created with the given data.
    *
    * @param projectId identifier of the project
    * @param factorID identifier of the quality factor
    * @param factorName name of the quality factor
    * @param factorDescription description of the quality factor
    * @param value evaluation value
    * @param evaluationDate date when the evaluation has been computed
    * @param estimation in case we have an estimation (probabilities and probable values), instead of a single value
    */
    public static UpdateResult setFactorEvaluation(String projectId,
                                                   String factorID,
                                                   String factorName,
                                                   String factorDescription,
                                                   Float value,
                                                   String info,
                                                   LocalDate evaluationDate,
                                                   EstimationEvaluationDTO estimation,
                                                   List<String> missingMetrics,
                                                   long datesMismatch,
                                                   List<String> indicators) {
        UpdateResult response;
        String elastic_entry_ID=getHardID(projectId, factorID, evaluationDate);
        response = Queries.setFactorValue(
            Constants.QMLevel.factors, elastic_entry_ID,
            projectId, factorID, factorName,
            factorDescription, evaluationDate,
            value, info, estimation,
            missingMetrics, datesMismatch,
            indicators);
        return response;

    }

    /**
    * This method updates the value of the strategic indicators' relation for a list of factor.
    *
    * @param factors DTO with the factor information
    */
    public static UpdateResult setStrategicIndicatorRelation(List<FactorEvaluationDTO> factors) {
        UpdateResult response = null;
        for (FactorEvaluationDTO factor: factors)
            response = Queries.setFactorStrategicIndicatorRelation(factor);
        return response;
    }


    /**
    * This method returns the list of the factors, for each factor it returns the list of metrics associated to this
    * factor and the last metric evaluation. The evaluation contains the evaluation date and value.
    *
    * @param projectId identifier of the project
    *
    * @return The list of factors' evaluations, for each factor it contains the list of
    *         the evaluation of the metrics used to compute the factor
    */
    public static List<FactorMetricEvaluationDTO> getMetricsEvaluations(String projectId) {
        List<FactorMetricEvaluationDTO> ret = new ArrayList<>();
        Map<String, String> IDNames = getFactorsIDNames(projectId);
        for (String factorID : IDNames.keySet()) {
            FactorMetricEvaluationDTO factorMetrics = getMetricsEvaluations(projectId, factorID);
            ret.add(factorMetrics);
        }
        resetFactorsIDNames();
        return ret;
    }

    /**
    * This method returns the list of the factors, for each factor it returns the list of metrics associated to this
    * factor. For each metric, it returns the evaluations belonging to the period defined by the parameters from and
    * to. The evaluation contains the evaluation date and value.
    *
    * @param projectId identifier of the project
    * @param from initial date from the range we are querying
    * @param to final date from the range we are querying
    *
    * @return The list of factors' evaluations, for each factor it contains the list of
    *         the evaluation of the metrics used to compute the factor
    */
    public static List<FactorMetricEvaluationDTO> getMetricsEvaluations(String projectId, LocalDate from, LocalDate to) {
        List<FactorMetricEvaluationDTO> ret = new ArrayList<>();
        Map<String, String> IDNames = getFactorsIDNames(projectId);
        for (String factorID : IDNames.keySet()) {
            FactorMetricEvaluationDTO factorMetrics = getMetricsEvaluations(projectId, factorID, from, to);
            ret.add(factorMetrics);
        }
        resetFactorsIDNames();
        return ret;
    }

    /**
    * This method returns the list of metrics associated to the factor evaluation passed as parameter and the last
    * metric evaluation. The evaluation contains the evaluation date and value.
    *
    * @param projectId identifier of the project
    * @param factorID identifier of the factor
    *
    * @return The list of factors' evaluations, for each factor it contains the list of the evaluation of the metrics
    *         used to compute the factor
    */
    public static FactorMetricEvaluationDTO getMetricsEvaluations(String projectId, String factorID) {
        //Map<String, String> IDNames = getFactorsIDNames(projectId);
        //String factorName = Queries.getStringFromStringMapOrDefault(IDNames, factorID, factorID);
        List<Document> sr = Queries.getLatest(Constants.QMLevel.metrics, projectId,  factorID);
        List<MetricEvaluationDTO> metricsEval = Common.processMetricsBuckets(sr);
        FactorEvaluationDTO factorEvaluationDTO = getSingleEvaluation(projectId, factorID);
        return new FactorMetricEvaluationDTO(factorEvaluationDTO, metricsEval);
    }

    /**
    * This method returns the list of metrics associated to the factor evaluation passed as parameter. For each metric,
    * it returns the evaluations belonging to the period defined by the parameters from and to. The evaluation contains
    * the evaluation date and value.
    *
    * @param projectId identifier of the projectId
    * @param factorID identifier of the factor
    * @param from initial date from the range we are querying
    * @param to final date from the range we are querying
    *
    * @return The list of factors' evaluations, for each factor it contains the list of the evaluation of the metrics
    *         used to compute the factor
    */
    public static FactorMetricEvaluationDTO getMetricsEvaluations(String projectId, String factorID, LocalDate from, LocalDate to) {
        //Map<String, String> IDNames = getFactorsIDNames(projectId);
        //String factorName = Queries.getStringFromStringMapOrDefault(IDNames, factorID, factorID);
        List<Document> sr = Queries.getRanged(Constants.QMLevel.metrics, projectId, factorID, from, to);
        List<MetricEvaluationDTO> metricsEval = Common.processMetricsBuckets(sr);
        FactorEvaluationDTO factorEvaluationDTO = getSingleEvaluation(projectId, factorID);
        return new FactorMetricEvaluationDTO(factorEvaluationDTO,metricsEval);
    }

    private static Map<String, String> getFactorsIDNames(String projectId) {
        if (IDNames == null) {
            IDNames = Common.getIDNames(projectId, Constants.QMLevel.factors);
            return IDNames;
        }
        return IDNames;
    }

    public static void resetFactorsIDNames() {
        IDNames = null;
    }

}

package evaluation;

import DTOs.MetricEvaluationDTO;
import com.mongodb.client.result.UpdateResult;
import org.bson.BsonValue;
import org.bson.Document;
import util.Common;
import util.Constants;
import util.FormattedDates;
import util.Queries;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import java.io.IOException;
import java.time.LocalDate;
import java.util.List;

public class Metric {

    /**
    * This method returns the list of the metrics and their last evaluation.
    * The evaluation contains the evaluation date and value.
    *
    * @param projectId identifier of the project
    *
    * @return List of metric evaluations
    */
    public static List<MetricEvaluationDTO> getEvaluations(String projectId) {
        List<MetricEvaluationDTO> ret;
        List<Document> sr = Queries.getLatest(projectId, Constants.QMLevel.metrics);
        ret = Common.processMetricsBuckets(sr);
        return ret;
    }

    /**
    * This method returns the last evaluation of the metric passed as a parameter.
    * The evaluation contains the evaluation date and value.
    *
    * @param projectId identifier of the project
    * @param metricId identifier of the metric
    *
    * @return Metric evaluation
    */
    public static MetricEvaluationDTO getSingleEvaluation(String projectId, String metricId) {
        List<MetricEvaluationDTO> ret;
        MetricEvaluationDTO metricEvaluationDTO = null;
        List<Document> sr = Queries.getLatestElement(projectId, Constants.QMLevel.metrics, metricId);
        ret = Common.processMetricsBuckets(sr);
        if (!ret.isEmpty()) metricEvaluationDTO = ret.get(0);
        return metricEvaluationDTO;
    }

    /**
    * This method returns the list of the metrics and the evaluations belonging to a specific period defined by the
    * parameters from and to. The evaluation contains the evaluation date and value.
    *
    * @param projectId identifier of the project
    * @param from initial date from the range we are querying
    * @param to final date from the range we are querying
    *
    * @return List of metric evaluations
    */
    public static List<MetricEvaluationDTO> getEvaluations(String projectId, LocalDate from, LocalDate to) {
        List<MetricEvaluationDTO> ret;
        List<Document> sr = Queries.getRanged(Constants.QMLevel.metrics, projectId, from, to);
        ret = Common.processMetricsBuckets(sr);
        return ret;
    }

    /**
    * This method returns the last evaluation of the metric passed as a parameter.
    * The evaluation contains the evaluation date and value.
    *
    * @param projectId identifier of the project
    * @param metricId identifier of the metric
    *
    * @return Metric evaluation
    */
    public static MetricEvaluationDTO getSingleEvaluation(String projectId, String metricId, LocalDate from, LocalDate to) {
        List<MetricEvaluationDTO> ret;
        MetricEvaluationDTO metricEvaluationDTO = null;
        List<Document> sr = Queries.getRangedElement(projectId, Constants.QMLevel.metrics, metricId, from, to);
        ret = Common.processMetricsBuckets(sr);
        if (!ret.isEmpty()) metricEvaluationDTO = ret.get(0);
        return metricEvaluationDTO;
    }

    /**
    * This method updates the value of the quality factors relation for a list of metrics.
    *
    * @param metrics DTO with the metric information
    */
    public static UpdateResult setQualityFactorsRelation(List<MetricEvaluationDTO> metrics) {
        UpdateResult response = null;
        for (MetricEvaluationDTO metric: metrics)
            response = Queries.setMetricQualityFactorRelation(metric);
        return response;
    }

}

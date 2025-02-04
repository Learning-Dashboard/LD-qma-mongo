package evaluation;

import DTOs.MetricEvaluationDTO;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import util.Common;
import util.Constants;
import util.Queries;

import java.time.LocalDate;
import java.util.List;

public class Metric {

    /**
    * This method returns the list of the metrics and their last evaluation.
    * The evaluation contains the evaluation date and value.
    *
    * @param projectId Identifier of the project.
    *
    * @return List of metric evaluations.
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
    * @param projectId Identifier of the project.
    * @param metricId Identifier of the metric.
    *
    * @return Metric evaluation.
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
    * @param projectId Identifier of the project.
    * @param from Initial date from the range we are querying.
    * @param to Final date from the range we are querying.
    *
    * @return List of metric evaluations.
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
    * @param projectId Identifier of the project.
    * @param metricId Identifier of the metric.
    *
    * @return Metric evaluation.
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
    * @param metrics DTO with the metric information.
    */
    public static UpdateResult setQualityFactorsRelation(List<MetricEvaluationDTO> metrics) {
        UpdateResult response = null;
        for (MetricEvaluationDTO metric: metrics)
            response = Queries.setMetricQualityFactorRelation(metric);
        return response;
    }

}

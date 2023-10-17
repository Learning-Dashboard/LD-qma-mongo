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

public class StrategicIndicator {
    private static Map<String, String> IDNames;

    /**
    * This method returns the list of the strategic indicators and the last evaluation.
    * The evaluation contains the evaluation date and value.
    *
    * @param projectId Identifier of the project.
    *
    * @return The list of strategic indicators' evaluations.
    */
    public static List<StrategicIndicatorEvaluationDTO> getEvaluations(String projectId) {
        List<StrategicIndicatorEvaluationDTO> ret;
        List<Document> sr = Queries.getLatest(projectId,Constants.QMLevel.strategic_indicators);
        ret = Common.processStrategicIndicatorsBuckets(sr);
        return ret;
    }

    /**
    * This method returns the last evaluation of the strategic indicator passed as a parameter.
    * The evaluation contains the evaluation date and value.
    *
    * @param projectId Identifier of the project.
    * @param strategicIndicatorId Identifier of the strategic indicator.
    *
    * @return Strategic indicator evaluation.
    */
    public static StrategicIndicatorEvaluationDTO getSingleEvaluation(String projectId, String strategicIndicatorId) {
        List<StrategicIndicatorEvaluationDTO> ret;
        StrategicIndicatorEvaluationDTO strategicIndicatorEvaluationDTO = null;
        List<Document> sr = Queries.getLatestElement(projectId, Constants.QMLevel.strategic_indicators, strategicIndicatorId);
        ret = Common.processStrategicIndicatorsBuckets(sr);
        if (!ret.isEmpty()) strategicIndicatorEvaluationDTO = ret.get(0);
        return strategicIndicatorEvaluationDTO;
    }

    /**
    * This method returns the list of the strategic indicators and the evaluations belonging to the specific period
    * defined by the parameters from and to. The evaluation contains the evaluation date and value.
    *
    * @param projectId Identifier of the project.
    * @param from Initial date from the range we are querying.
    * @param to Final date from the range we are querying.
    *
    * @return The list of strategic indicators' evaluations.
    */
    public static List<StrategicIndicatorEvaluationDTO> getEvaluations(String projectId, LocalDate from, LocalDate to) {
        List<StrategicIndicatorEvaluationDTO> ret;
        List<Document> sr = Queries.getRanged(Constants.QMLevel.strategic_indicators, projectId, from, to);
        ret = Common.processStrategicIndicatorsBuckets(sr);
        return ret;
    }

    /**
    * The external repository have two identifiers for each element, the field used by the repository (hard ID) and the
    * id and evaluation date used by the "users".
    *
    * @param projectId Identifier of the project.
    * @param strategicIndicatorID Identifier of the strategic indicator.
    * @param evaluationDate Date when the evaluation has been computed.
    */
    public static String getHardID(String projectId, String strategicIndicatorID, LocalDate evaluationDate) {
        return strategicIndicatorID + "-" + FormattedDates.formatDate(evaluationDate);
    }

    /**
    * This method updates the value of an strategic indicators in a given date, if it doesn't exist
    * a new strategic indicator is created with the given data.
    *
    * @param projectId Identifier of the project.
    * @param strategicIndicatorID Identifier of the strategic indicator.
    * @param strategicIndicatorName Name of the strategic indicator.
    * @param strategicIndicatorDescription Description of the strategic indicator.
    * @param value Evaluation value.
    * @param evaluationDate Date when the evaluation has been computed.
    * @param estimation In case we have a estimation (probabilities and probable values), instead of a single value.
    */
    public static UpdateResult setStrategicIndicatorEvaluation(String projectId,
                                                               String strategicIndicatorID,
                                                               String strategicIndicatorName,
                                                               String strategicIndicatorDescription,
                                                               Float value,
                                                               String info,
                                                               LocalDate evaluationDate,
                                                               EstimationEvaluationDTO estimation,
                                                               List<String> missingFactors,
                                                               long datesMismatch) {
        UpdateResult response;
        String elastic_entry_ID = getHardID(projectId, strategicIndicatorID, evaluationDate);
        response = Queries.setStrategicIndicatorValue(
                Constants.QMLevel.strategic_indicators,
                elastic_entry_ID,
                projectId,
                strategicIndicatorID,
                strategicIndicatorName,
                strategicIndicatorDescription,
                evaluationDate,
                value,
                info,
                estimation,
                missingFactors,
                datesMismatch);
        return response;
    }

    /**
    * This method returns the list of the strategic indicators. For each strategic indicator, it returns the list of
    * factors associated to it and their last evaluation. The evaluation contains the evaluation date and value.
    *
    * @param projectId Identifier of the project.
    *
    * @return The list of strategic indicators' evaluations, for each strategic indicator it returns the evaluation
    *         of the factors impacting on this strategic indicator.
    */
    public static List<StrategicIndicatorFactorEvaluationDTO> getFactorsEvaluations(String projectId) {
        List<StrategicIndicatorFactorEvaluationDTO> ret = new ArrayList<>();
        Map<String, String> IDNames = getIndicatorsIDNames(projectId);
        for (String indicatorID : IDNames.keySet()) {
            StrategicIndicatorFactorEvaluationDTO indicatorFactors = getFactorsEvaluations(projectId, indicatorID);
            ret.add(indicatorFactors);
        }
        resetIndicatorsIDNames();
        return ret;
    }

    /**
    * This method returns the list of the strategic indicators. For each strategic indicator, it returns the list of
    * factors associated to it and their evaluations belonging to the period defined by the parameters from and to.
    * The evaluation contains the evaluation date and value.
    *
    * @param projectId Identifier of the project.
    * @param from Initial date from the range we are querying.
    * @param to Final date from the range we are querying.
    *
    * @return The list of strategic indicators' evaluations, for each strategic indicator it returns the evaluation
    *         of the factors impacting on this strategic indicator.
    */
    public static List<StrategicIndicatorFactorEvaluationDTO> getFactorsEvaluations(String projectId, LocalDate from, LocalDate to) {
        List<StrategicIndicatorFactorEvaluationDTO> ret = new ArrayList<>();
        Map<String, String> IDNames = getIndicatorsIDNames(projectId);
        for (String indicatorID : IDNames.keySet()) {
            StrategicIndicatorFactorEvaluationDTO indicatorFactors = getFactorsEvaluations(projectId, indicatorID, from, to);
            ret.add(indicatorFactors);
        }
        resetIndicatorsIDNames();
        return ret;
    }

    /**
    * This method returns the list of factors associated to the strategic indicator evaluation passed as a parameter
    * and their last evaluation. The evaluation contains the evaluation date and value.
    *
    * @param projectId Identifier of the project.
    * @param strategicIndicatorID Identifier of the strategic indicator.
    *
    * @return The strategic indicator evaluations, for this strategic indicator it returns the evaluation
    *         of the factors impacting on this strategic indicator.
    */
    public static StrategicIndicatorFactorEvaluationDTO getFactorsEvaluations(String projectId, String strategicIndicatorID) {
        List<Document> sr = Queries.getLatest(Constants.QMLevel.factors, projectId, strategicIndicatorID);
        List<FactorEvaluationDTO> factorsEval = Common.processFactorsBuckets(sr);
        StrategicIndicatorEvaluationDTO strategicIndicatorEvaluationDTO = getSingleEvaluation(projectId, strategicIndicatorID);
        return new StrategicIndicatorFactorEvaluationDTO(strategicIndicatorEvaluationDTO, factorsEval);
    }

    /**
    * This method returns the list of factors associated to the strategic indicator evaluation passed as parameter and
    * their evaluations belonging to the period defined by the parameters from and to. The evaluation contains the
    * evaluation date and value.
    *
    * @param projectId Identifier of the project.
    * @param strategicIndicatorID Identifier of the strategic indicator.
    * @param from Initial date from the range we are querying.
    * @param to Final date from the range we are querying.
    *
    * @return The strategic indicator evaluations, for this strategic indicator it returns the evaluation
    *         of the factors impacting on this strategic indicator.
    */
    public static StrategicIndicatorFactorEvaluationDTO getFactorsEvaluations(String projectId,
                                                                              String strategicIndicatorID,
                                                                              LocalDate from,
                                                                              LocalDate to) {
        List<Document> sr = Queries.getRanged(Constants.QMLevel.factors, projectId, strategicIndicatorID, from,to);
        List<FactorEvaluationDTO> factorsEval = Common.processFactorsBuckets(sr);
        StrategicIndicatorEvaluationDTO strategicIndicatorEvaluationDTO = getSingleEvaluation(projectId, strategicIndicatorID);
        return new StrategicIndicatorFactorEvaluationDTO(strategicIndicatorEvaluationDTO, factorsEval);
    }

    /**
    * This method returns the list of metrics associated to every factor of a strategic indicator passed as parameter
    * and the last metric evaluation. The evaluation contains the evaluation date and value.
    *
    * @param projectId Identifier of the project.
    * @param strategicIndicatorID Identifier of the strategic indicator.
    *
    * @return The list of the evaluation of the factors impacting on this strategic indicator. For each factor,
    *         the evaluation of the metrics used for computing this factor.
    */
    public static List<FactorMetricEvaluationDTO> getMetricsEvaluations(String projectId, String strategicIndicatorID) {
        List <FactorMetricEvaluationDTO> ret = new ArrayList<>();
        StrategicIndicatorFactorEvaluationDTO siFactors = getFactorsEvaluations(projectId, strategicIndicatorID);
        for (FactorEvaluationDTO factor : siFactors.getFactors())
            ret.add(new FactorMetricEvaluationDTO(factor, Factor.getMetricsEvaluations(factor.getProject(), factor.getID()).getMetrics()));
        return ret;
    }

    /**
    * This method returns the list of metrics associated to every factor of a strategic indicator passed as parameter
    * and their evaluations belonging to the period defined by the parameters from and to. The evaluation contains the
    * evaluation date and value.
    *
    * @param projectId Identifier of the project.
    * @param strategicIndicatorID Identifier of the strategic indicator.
    * @param from Starting date of the evaluations.
    * @param to Ending date of the evaluations.
    *
    * @return The list of the evaluation of the factors impacting on this strategic indicator. For each factor,
    *         the evaluation of the metrics used for computing this factor.
    */
    public static List<FactorMetricEvaluationDTO> getMetricsEvaluations(String projectId,
                                                                        String strategicIndicatorID,
                                                                        LocalDate from,
                                                                        LocalDate to) {
        List <FactorMetricEvaluationDTO> ret = new ArrayList<>();
        StrategicIndicatorFactorEvaluationDTO siFactors = getFactorsEvaluations(projectId, strategicIndicatorID, from, to);
        for (FactorEvaluationDTO factor : siFactors.getFactors())
            ret.add(new FactorMetricEvaluationDTO(factor, Factor.getMetricsEvaluations(factor.getProject(),factor.getID(), from, to).getMetrics()));
        return ret;
    }

    /**
    * This method returns a map containing, for each existing strategic indicator in a project, its identifier and name.
    *
    * @param projectId Identifier of the projectId.
    *
    * @return A Map<ID, name> for each existing strategic indicator in the project.
    */
    private static Map<String, String> getIndicatorsIDNames(String projectId) {
        if (IDNames == null) {
            IDNames = Common.getIDNames(projectId, Constants.QMLevel.strategic_indicators);
            return IDNames;
        }
        return IDNames;
    }

    /**
    * Reset the map which stores the strategic indicators' identifiers and names.
    */
    public static void resetIndicatorsIDNames() {
        IDNames = null;
    }

}

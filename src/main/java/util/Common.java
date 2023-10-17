package util;

import DTOs.*;
import org.bson.Document;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Common {

    //NOTE: processMetricsBuckets, processFactorsBuckets and processStrategicIndicatorsBuckets are almost the same,
    //but are separated as in the future there can be changes in the DTOs that require specific processing

    /**
    * Create a list of MetricEvaluationDTO from a List<Document>,
    * containing metrics' evaluations.
    *
    * @param result The Document list containing the metrics' evaluations.
    *
    * @return The equivalent list of MetricEvaluationDTO.
    */
    public static List<MetricEvaluationDTO> processMetricsBuckets(List<Document> result) {
        List<MetricEvaluationDTO> ret = new ArrayList<>();
        for (Document res : result) { //Para cada métrica

            MetricEvaluationDTO metricEval = new MetricEvaluationDTO();
            List<EvaluationDTO> evals = new ArrayList<>();
            List<?> documentsList = (List<?>) res.get("documents");

            for (Object docObject : documentsList) {
                if (docObject instanceof Document) {

                    Document doc = (Document) docObject;
                    EvaluationDTO eval = new EvaluationDTO(
                        Queries.getStringFromObject(doc.get("_id")),
                        Queries.getStringFromObject(doc.get(Constants.DATA_SOURCE)),
                        Queries.getStringFromObject(doc.get(Constants.EVALUATION_DATE)),
                        Queries.getStringFromObject(doc.get(Constants.VALUE)),
                        Queries.getStringFromObject(doc.get(Constants.RATIONALE))
                    );

                    evals.add(eval);
                    metricEval.setID(Queries.getStringFromObject(doc.get(Constants.METRIC_ID)));
                    metricEval.setName(Queries.getStringFromObjectOrDefault(doc.get(Constants.NAME), metricEval.getID()));
                    metricEval.setDescription(Queries.getStringFromObjectOrDefault(doc.get(Constants.DESCRIPTION), ""));
                    metricEval.setProject(Queries.getStringFromObjectOrDefault(doc.get(Constants.PROJECT), ""));
                    metricEval.setFactors(Queries.getArrayListFromObject(doc.get(Constants.ARRAY_FACTORS)));

                }
            }
            metricEval.setEvaluations(evals);
            ret.add(metricEval);
        }
        return ret;
    }

    /**
    * Create a list of FactorEvaluationDTO from a List<Document>,
    * containing factors' evaluations.
    *
    * @param result The Document list containing the factors' evaluations.
    *
    * @return The equivalent list of FactorEvaluationDTO.
    */
    public static List<FactorEvaluationDTO> processFactorsBuckets(List<Document> result) {
        List<FactorEvaluationDTO> ret = new ArrayList<>();
        for (Document res : result) {

            FactorEvaluationDTO factorEval = new FactorEvaluationDTO();
            List<EvaluationDTO> evals = new ArrayList<>();
            List<?> documentsList = (List<?>) res.get("documents");

            for (Object docObject : documentsList) {
                if (docObject instanceof Document) {

                    Document doc = (Document) docObject;
                    EvaluationDTO eval = new EvaluationDTO(
                        Queries.getStringFromObject(doc.get("_id")),
                        Queries.getStringFromObject(doc.get(Constants.DATA_SOURCE)),
                        Queries.getStringFromObject(doc.get(Constants.EVALUATION_DATE)),
                        Queries.getStringFromObject(doc.get(Constants.VALUE)),
                        Queries.getStringFromObject(doc.get(Constants.RATIONALE))
                    );

                    if (Queries.getIntFromObject( doc.get(Constants.DATES_MISMATCH) ) != null)
                        eval.setMismatchDays(Queries.getIntFromObject( doc.get(Constants.DATES_MISMATCH) ));
                    if (Queries.getArrayListFromObject(doc.get(Constants.MISSING_METRICS)) != null)
                        eval.setMissingElements(Queries.getArrayListFromObject( doc.get(Constants.MISSING_METRICS) ));

                    evals.add(eval);
                    factorEval.setID(Queries.getStringFromObject(doc.get(Constants.FACTOR_ID)));
                    factorEval.setName(Queries.getStringFromObjectOrDefault(doc.get(Constants.NAME), factorEval.getID()));
                    factorEval.setDescription(Queries.getStringFromObjectOrDefault(doc.get(Constants.DESCRIPTION), ""));
                    factorEval.setProject(Queries.getStringFromObjectOrDefault(doc.get(Constants.PROJECT), ""));
                    factorEval.setStrategicIndicators(Queries.getArrayListFromObject(doc.get(Constants.ARRAY_STRATEGIC_INDICATORS)));

                }
            }
            factorEval.setEvaluations(evals);
            ret.add(factorEval);
        }
        return ret;
    }

    /**
    * Create a list of StrategicIndicatorEvaluationDTO from a List<Document>,
    * containing strategic indicators' evaluations.
    *
    * @param result The Document list containing the strategic indicators' evaluations.
    *
    * @return The equivalent list of StrategicIndicatorEvaluationDTO.
    */
    public static List<StrategicIndicatorEvaluationDTO> processStrategicIndicatorsBuckets(List<Document> result) {
        List<StrategicIndicatorEvaluationDTO> ret = new ArrayList<>();
        for (Document res : result) {

            StrategicIndicatorEvaluationDTO siEval = new StrategicIndicatorEvaluationDTO();
            List<EvaluationDTO> evals = new ArrayList<>();
            List<EstimationEvaluationDTO> estimations = new ArrayList<>();
            List<?> documentsList = (List<?>) res.get("documents");

            for (Object docObject : documentsList) {
                if (docObject instanceof Document) {

                    Document doc = (Document) docObject;
                    EvaluationDTO eval = new EvaluationDTO(
                        Queries.getStringFromObject(doc.get("_id")),
                        Queries.getStringFromObject(doc.get(Constants.DATA_SOURCE)),
                        Queries.getStringFromObject(doc.get(Constants.EVALUATION_DATE)),
                        Queries.getStringFromObject(doc.get(Constants.VALUE)),
                        Queries.getStringFromObject(doc.get(Constants.RATIONALE))
                    );

                    if (Queries.getIntFromObject( doc.get(Constants.DATES_MISMATCH) ) != null)
                        eval.setMismatchDays(Queries.getIntFromObject( doc.get(Constants.DATES_MISMATCH) ));
                    if (Queries.getArrayListFromObject(doc.get(Constants.MISSING_FACTORS)) != null)
                        eval.setMissingElements(Queries.getArrayListFromObject( doc.get(Constants.MISSING_FACTORS) ));

                    evals.add(eval);
                    List<QuadrupletDTO<Integer, String, Float, Float>> estimation = new ArrayList<>();
                    ArrayList<Object> allEstimations = Queries.getArrayListFromObject( doc.get(Constants.ESTIMATION) );

                    if (allEstimations != null) {
                        for (Object e : allEstimations) {
                            if (e instanceof Document) {

                                Document docE = (Document) e;
                                Float upperThreshold;
                                try {
                                    upperThreshold = Queries.getFloatFromObject(docE.get(Constants.ESTIMATION_UPPER_THRESHOLD));
                                }
                                catch (NumberFormatException nfe) {
                                    upperThreshold = null;
                                }

                                estimation.add(new QuadrupletDTO<>(
                                    Queries.getIntFromObject(docE.get(Constants.ESTIMATION_ID)),
                                    Queries.getStringFromObject(docE.get(Constants.ESTIMATION_LABEL)),
                                    Queries.getFloatFromObject(docE.get(Constants.ESTIMATION_VALUE)),
                                    upperThreshold)
                                );
                            }
                        }
                        estimations.add(new EstimationEvaluationDTO(estimation));
                    }
                    else estimations.add(null);
                    siEval.setID(Queries.getStringFromObject(doc.get(Constants.STRATEGIC_INDICATOR_ID)));
                    siEval.setName(Queries.getStringFromObjectOrDefault(doc.get(Constants.NAME), siEval.getID()));
                    siEval.setDescription(Queries.getStringFromObjectOrDefault(doc.get(Constants.DESCRIPTION), ""));
                    siEval.setProject(Queries.getStringFromObjectOrDefault(doc.get(Constants.PROJECT), ""));
                }
            }
            siEval.setEvaluations(evals);
            siEval.setEstimation(estimations);
            ret.add(siEval);
        }
        return ret;
    }

    /**
    * Get the IDs and names of all the existing entities in each QMLevel.
    *
    * @param projectId The ID of the project.
    * @param QMLevel The QMLevel (metrics, factors or strategic_indicators).
    *
    * @return A Map of <ID, name> of the existing entities, for the given project and QMLevel.
    */
    public static Map<String, String> getIDNames(String projectId, Constants.QMLevel QMLevel) {
        Map<String, String> IDNames = new HashMap<>();
        List<Document> response = Queries.getLatest(projectId, QMLevel);

        for (Document res : response) {
            String key = Queries.getStringFromObject(res.get("_id"));
            List<?> documentsList = (List<?>) res.get("documents");

            for (Object docObject : documentsList) {
                if (docObject instanceof Document) {
                    Document doc = (Document) docObject;
                    String name = Queries.getStringFromObject(doc.get("name"));
                    IDNames.putIfAbsent(key, name);
                }
            }

        }
        return IDNames;
    }

}

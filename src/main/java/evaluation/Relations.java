package evaluation;

import DTOs.Relations.RelationDTO;
import DTOs.Relations.SourceRelationDTO;
import DTOs.Relations.TargetRelationDTO;
import org.bson.Document;
import util.Constants;
import util.Queries;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Relations {

    /**
    * This method returns the existing relations from the last 15 days.
    *
    * @param projectID Identifier of the project.
    *
    * @return A list of this relations (RelationDTO).
    */
    public static ArrayList<RelationDTO> getRelations(String projectID) {
        List<Document> sr = Queries.getLatestRelationsDate(projectID);
        if (sr.size() > 0) {
            Document doc = sr.get(0);
            Object evalDate = doc.get(Constants.EVALUATION_DATE);
            LocalDate date = LocalDate.parse(Queries.getStringFromObject(evalDate));
            return getRelations(projectID, date);
        }
        LocalDate date = LocalDate.now();
        return getRelations(projectID, date);
    }

    /**
    * This method returns the existing relations from the 15 days prior to the set ending date.
    *
    * @param projectID Identifier of the project.
    * @param dateTo The ending date of the 15 days time period (included).
    *
    * @return A list of this relations (RelationDTO).
    */
    public static ArrayList<RelationDTO> getRelations(String projectID, LocalDate dateTo) {
        LocalDate dateFrom = dateTo.minusDays(15);
        ArrayList<RelationDTO> relationDTO = new ArrayList<>();

        List<Document> responseRelations = Queries.getRelations(dateFrom, dateTo, projectID);
        Map<String, Boolean> processedElements = new HashMap<>();

        for (Document doc : responseRelations) { // Results come sorted by date
            String sourceID = getRelationLabel(Queries.getStringFromObjectOrDefault(doc.get(Constants.SOURCEID), ""));
            String targetID = getRelationLabel(Queries.getStringFromObjectOrDefault(doc.get(Constants.TARGETID), ""));
            String sourceTargetKey = sourceID+"->"+targetID;
            String targetType = Queries.getStringFromObjectOrDefault(doc.get(Constants.TARGETTPYE), "");

            if (!processedElements.containsKey(sourceTargetKey) && checkTargetType(targetType)) {
                String sourceValue = Queries.getStringFromObjectOrDefault(doc.get(Constants.VALUE), "");
                String targetValue = Queries.getStringFromObjectOrDefault(doc.get(Constants.TARGETVALUE), "");
                String sourceType = Queries.getStringFromObjectOrDefault(doc.get(Constants.SOURCETYPE), "");
                String sourceCategory = Queries.getStringFromObjectOrDefault(doc.get(Constants.SOURCELABEL), "");
                String weight = Queries.getStringFromObjectOrDefault(doc.get(Constants.WEIGHT), "");

                SourceRelationDTO sourceDTO = new SourceRelationDTO(sourceID, sourceValue, sourceCategory, sourceType);
                TargetRelationDTO targetDTO = new TargetRelationDTO(targetID, targetValue, targetType);
                relationDTO.add(new RelationDTO(weight, sourceDTO, targetDTO));
                processedElements.put(sourceTargetKey, true);
            }
        }
        return relationDTO;
    }

    /**
    * This method returns the element identifier present in some fields from the relations documents.
    *
    * @param elementID The value of the fields sourceId or targetId.
    *
    * @return The identifier of the element (string) extrated from the provided parameter.
    */
    private static String getRelationLabel(String elementID) {
        try {
            return elementID.split("-")[1];
        }
        catch (Exception e) {
            return "ERROR";
        }
    }

    /**
    * This method checks if the Target Type is correct.
    *
    * @param targetType The Target Type to be checked.
    *
    * @return A boolean that indicates if the Target Type is indeed correct.
    */
    private static boolean checkTargetType(String targetType) {
        return (targetType.equals(Constants.FACTOR_TYPE) ||
                targetType.equals(Constants.STRATEGIC_INDICATOR_TYPE));
    }

    /**
    * This method creates a document in a certain Relations index in the database.
    * Factor -> Strategic Indicator.
    *
    * @return A boolean that indicates if the operation was performed correctly.
    */
    public static boolean setStrategicIndicatorFactorRelation(String projectID, String[] factorID,
                                                              String strategicIndicatorID, LocalDate evaluationDate,
                                                              double[] weight, double[] sourceValue,
                                                              String[] sourceCategories, String targetValue) {
        return Queries.setFactorSIRelationIndex(projectID, factorID, weight, sourceValue, sourceCategories,
               strategicIndicatorID, evaluationDate, targetValue);
    }

    /**
    * This method creates a document in a certain Relations index in the database.
    * Metric -> Factor.
    *
    * @return A boolean that indicates if the operation was performed correctly.
    */
    public static boolean setQualityFactorMetricRelation(String projectID, String[] metrics,
                                                              String qualityFactorID, LocalDate evaluationDate,
                                                              double[] weight, double[] sourceValue,
                                                              String[] sourceCategories, String targetValue) {
        return Queries.setMetricQFRelationIndex(projectID, metrics, weight, sourceValue, sourceCategories,
               qualityFactorID, evaluationDate, targetValue);
    }
}

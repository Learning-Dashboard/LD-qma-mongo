package util;

import DTOs.*;
import com.mongodb.MongoException;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.BulkWriteUpsert;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static util.Constants.*;

public class Queries {

    public static String getStringFromMap(Map<String, Object> map, String k) {
        return String.valueOf( map.get(k) );
    }

    public static ArrayList getArrayListFromMap(Map<String, Object> map, String k) {
        return (ArrayList) map.get(k);
    }

    public static Integer getIntFromMap(Map<String, Object> map, String k) {
        return (Integer) map.get(k);
    }

    public static Float getFloatFromMap(Map<String, Object> map, String k) {
        return Float.valueOf( String.valueOf( map.get(k) ) );
    }

    public static String getStringFromObject(Object o) {
        return String.valueOf(o);
    }

    public static ArrayList getArrayListFromObject(Object o) {
        return (ArrayList) o;
    }

    public static Integer getIntFromObject(Object o) {
        return (Integer) o;
    }

    public static Float getFloatFromObject(Object o) {
        return Float.valueOf( String.valueOf(o) );
    }

    public static String safeGetFromStringArray(String[] array, int index) {
        if (array != null && (index >= 0) && (index < array.length)) return array[index];
        else return "";
    }

    public static double safeGetFromDoubleArray(double[] array, int index) {
        if ((index >= 0) && (index < array.length)) return array[index];
        else return 0;
    }

    public static String getStringFromMapOrDefault(Map<String, Object> map, String k, String def) {
        String valueOfMap = String.valueOf(map.get(k));
        if (valueOfMap.equals("null")) return def;
        else return valueOfMap;
    }

    public static String getStringFromObjectOrDefault(Object o, String def) {
        if (o == null) return def;
        String valueOfObject = String.valueOf(o);
        if (valueOfObject.equals("null")) return def;
        else if (valueOfObject.isEmpty()) return def;
        else return valueOfObject;
    }

    public static String getStringFromStringMapOrDefault(Map<String, String> map, String k, String def) {
        String valueOfMap = String.valueOf(map.get(k));
        if (valueOfMap.equals("null")) return def;
        else return valueOfMap;
    }

    /**
    * Get the name of a collection in the database.
    *
    * @param elementIndexName The name of the entities in the collection (metrics, factors...).
    * @param projectID The ID of the project.
    *
    * @return The name of the collection.
    */
    private static String getIndexName(String elementIndexName, String projectID) {
        String index = elementIndexName;
        if (projectID != null && !projectID.isEmpty() &&
            !projectID.equalsIgnoreCase("EMPTY") &&
            !projectID.equalsIgnoreCase("\"\""))
            index = index.concat("." + projectID);
        return index;
    }

    private static String getFactorsIndex(String projectId) {
        return getIndexName(INDEX_FACTORS, projectId);
    }

    private static String getStrategicIndicatorsIndex(String projectId) {
        return getIndexName(INDEX_STRATEGIC_INDICATORS, projectId);
    }

    private static String getMetricsIndex(String projectId) {
        return getIndexName(INDEX_METRICS, projectId);
    }

    private static String getRelationsIndex(String projectId) {
        return getIndexName(INDEX_RELATIONS, projectId);
    }

    /**
    * Get the latest evaluations of the entities in a certain QMLevel.
    *
    * @param QMLevel The QMLevel (metrics, factors or strategic_indicators).
    * @param projectId The ID of the project.
    * @param parent The parent of the entities to retrieve (if any).
    *
    * @return A Document list containing the latest evaluations, one evaluation for each entity.
    */
    public static List<Document> getLatest(QMLevel QMLevel, String projectId, String parent) {
        MongoDatabase database = Connection.getMongoDatabase();
        collectionExists(getIndex(projectId, QMLevel));
        MongoCollection<Document> collection = database.getCollection( getIndex(projectId, QMLevel) );
        Bson parentQuery = getLatestParentQueryBuilder(parent, QMLevel);

        List<Bson> pipeline = Arrays.asList(
                Aggregates.match(parentQuery),
                Aggregates.sort(Sorts.descending(EVALUATION_DATE)),
                Aggregates.group("$" + getIDtoGroup(QMLevel), Accumulators.push("documents", "$$ROOT")),
                Aggregates.limit(10000),
                Aggregates.project(
                    Projections.fields(
                        Projections.computed( "documents",
                        new Document( "$slice", Arrays.asList("$documents", 1) ) )
                    )
                ),
                Aggregates.sort(Sorts.ascending("documents." + getIDtoGroup(QMLevel)))
        );

        List<Document> result = collection.aggregate(pipeline).into( new ArrayList<>() );
        for (Document document : result) System.out.println(document);
        return result;
    }

    /**
    * Get the latest evaluations of the entities in a certain QMLevel.
    *
    * @param QMLevel The QMLevel (metrics, factors or strategic_indicators).
    * @param projectId The ID of the project.
    *
    * @return A Document list containing the latest evaluations, one evaluation for each entity.
    */
    public static List<Document> getLatest(String projectId, QMLevel QMLevel) {
        return getLatest(QMLevel, projectId,"all");
    }

    /**
    * Get the latest evaluation of a specific entity.
    *
    * @param QMLevel The QMLevel (metrics, factors or strategic_indicators).
    * @param projectId The ID of the project.
    * @param elementId The ID of the entity to get the evaluation from.
    *
    * @return A Document list containing the latest evaluation of said entity.
    */
    public static List<Document> getLatestElement(String projectId, QMLevel QMLevel, String elementId) {
        MongoDatabase database = Connection.getMongoDatabase();
        collectionExists(getIndex(projectId, QMLevel));
        MongoCollection<Document> collection = database.getCollection( getIndex(projectId, QMLevel) );
        String group = getIDtoGroup(QMLevel);

        List<Bson> pipeline = Arrays.asList(
                Aggregates.match(Filters.eq(group, elementId)),
                Aggregates.sort(Sorts.descending(EVALUATION_DATE)),
                Aggregates.group("$" + group, Accumulators.push("documents", "$$ROOT")),
                Aggregates.limit(10000),
                Aggregates.project(
                        Projections.fields(
                                Projections.computed( "documents",
                                        new Document( "$slice", Arrays.asList("$documents", 1) ) )
                        )
                ),
                Aggregates.sort(Sorts.ascending("documents." + group))
        );

        List<Document> result = collection.aggregate(pipeline).into( new ArrayList<>() );
        for (Document document : result) System.out.println(document);
        return result;
    }

    /**
    * Get the name of the collection for each project and QMLevel.
    *
    * @param QMLevel The QMLevel (metrics, factors or strategic_indicators).
    * @param projectId The ID of the project.
    *
    * @return The name of the collection.
    */
    private static String getIndex(String projectId, QMLevel QMLevel) {
        String index = "";
        switch (QMLevel) {
            case strategic_indicators:
                index = getStrategicIndicatorsIndex(projectId);
                break;
            case factors:
                index = getFactorsIndex(projectId);
                break;
            case metrics:
                index = getMetricsIndex(projectId);
                break;
            case relations:
                index = getRelationsIndex(projectId);
                break;
        }
        return index;
    }

    /**
    * Get the name of the attribute which stores the ID of each entity in a collection.
    *
    * @param QMLevel The QMLevel (metrics, factors or strategic_indicators).
    *
    * @return The name of the attribute (metric, factor or strategic_indicator).
    */
    private static String getIDtoGroup(QMLevel QMLevel) {
        String group = "";
        switch (QMLevel) {
            case strategic_indicators:
                group = STRATEGIC_INDICATOR_ID;
                break;
            case factors:
                group = FACTOR_ID;
                break;
            case metrics:
                group = METRIC_ID;
                break;
        }
        return group;
    }

    /**
    * Construct a query to filter the entities' evaluations which belong to a certain parent.
    * Parent means you are searching for elements related to others, e.g. the factors for a specific SI.
    *
    * @param parent The ID of the parent the entity has to be associated with.
    * @param QMLevel The QMLevel (metrics, factors or strategic_indicators).
    *
    * @return The query which filters the evaluations using this condition.
    */
    private static Bson getLatestParentQueryBuilder(String parent, QMLevel QMLevel) {
        Bson query;
        if (parent.equals("all")) query = new Document();
        else {
            if (QMLevel == Constants.QMLevel.metrics) query = Filters.in(ARRAY_FACTORS, parent);
            else query = Filters.in(ARRAY_STRATEGIC_INDICATORS, parent);
        }
        return query;
    }

    /**
    * Construct a query to filter the entities' evaluations which belong to a certain parent,
    * and that are also included in the specified time range.
    * Parent means you are searching for elements related to others, e.g. the factors for a specific SI.
    *
    * @param parent The ID of the parent the entity has to be associated with.
    * @param QMLevel The QMLevel (metrics, factors or strategic_indicators).
    * @param dateFrom The starting date of the filtering time range.
    * @param dateTo The ending date of the filtering time range.
    *
    * @return The query which filters the evaluations using this condition.
    */
    private static Bson getRangedParentQueryBuilder(String parent, QMLevel QMLevel, LocalDate dateFrom, LocalDate dateTo) {
        String from = FormattedDates.formatDate(dateFrom);
        String to = FormattedDates.formatDate(dateTo);
        List<Bson> andFilters = new ArrayList<>();

        Bson dateRangeFilter = Filters.and (
            Filters.gte(EVALUATION_DATE, from),
            Filters.lte(EVALUATION_DATE, to)
        );

        if (parent.equals("all")) andFilters.add(dateRangeFilter);
        else {
            if (QMLevel == Constants.QMLevel.metrics) {
                Bson factorsFilter = Filters.in(ARRAY_FACTORS, parent);
                andFilters.add(factorsFilter);
                andFilters.add(dateRangeFilter);
            }
            else {
                Bson strategicIndicatorsFilter = Filters.in(ARRAY_STRATEGIC_INDICATORS, parent);
                andFilters.add(strategicIndicatorsFilter);
                andFilters.add(dateRangeFilter);
            }
        }

        return Filters.and(andFilters);
    }

    /**
    * Get the evaluations that belong to a specified time range, for a certain QMLevel.
    *
    * @param QMLevel The QMLevel (metrics, factors or strategic_indicators).
    * @param projectId The ID of the project.
    * @param parent The ID of the parent the entity has to be associated with.
    * @param dateFrom The starting date of the filtering time range.
    * @param dateTo The ending date of the filtering time range.
    *
    * @return The Document list containing the filtered evaluations.
    */
    public static List<Document> getRanged(QMLevel QMLevel, String projectId , String parent, LocalDate dateFrom, LocalDate dateTo) {
        MongoDatabase database = Connection.getMongoDatabase();
        collectionExists(getIndex(projectId, QMLevel));
        MongoCollection<Document> collection = database.getCollection( getIndex(projectId, QMLevel) );

        List<Bson> pipeline = Arrays.asList(
                Aggregates.match(getRangedParentQueryBuilder(parent, QMLevel, dateFrom, dateTo)),
                Aggregates.sort(Sorts.ascending(EVALUATION_DATE)),
                Aggregates.group("$" + getIDtoGroup(QMLevel), Accumulators.push("documents", "$$ROOT")),
                Aggregates.limit(10000),
                Aggregates.project(
                    Projections.fields(
                        Projections.computed( "documents",
                            new Document( "$slice", Arrays.asList("$documents", 10000) ) )
                    )
                ),
                Aggregates.sort(Sorts.ascending("documents." + getIDtoGroup(QMLevel)))
        );

        List<Document> result = collection.aggregate(pipeline).into( new ArrayList<>() );
        for (Document document : result) System.out.println(document);
        return result;
    }

    /**
    * Get the evaluations that belong to a specified time range, for a certain QMLevel.
    *
    * @param QMLevel The QMLevel (metrics, factors or strategic_indicators).
    * @param projectId The ID of the project.
    * @param dateFrom The starting date of the filtering time range.
    * @param dateTo The ending date of the filtering time range.
    *
    * @return The Document list containing the filtered evaluations.
    */
    public static List<Document> getRanged(QMLevel QMLevel, String projectId, LocalDate dateFrom, LocalDate dateTo) {
        return getRanged(QMLevel, projectId,"all", dateFrom, dateTo);
    }

    /**
    * Get the evaluations that belong to a specified time range, for a specific entity.
    *
    * @param QMLevel The QMLevel (metrics, factors or strategic_indicators).
    * @param projectId The ID of the project.
    * @param elementId The ID of the entity we want to retrieve the evaluations from.
    * @param from The starting date of the filtering time range.
    * @param to The ending date of the filtering time range.
    *
    * @return The Document list containing the filtered evaluations.
    */
    public static List<Document> getRangedElement(String projectId, QMLevel QMLevel, String elementId, LocalDate from, LocalDate to) {
        MongoDatabase database = Connection.getMongoDatabase();
        collectionExists(getIndex(projectId, QMLevel));
        MongoCollection<Document> collection = database.getCollection( getIndex(projectId, QMLevel) );
        String group = getIDtoGroup(QMLevel);

        List<Bson> pipeline = Arrays.asList(
                Aggregates.match(Filters.eq(group, elementId)),
                Aggregates.match(Filters.gte(EVALUATION_DATE, from.toString())),
                Aggregates.match(Filters.lte(EVALUATION_DATE, to.toString())),
                Aggregates.sort(Sorts.descending(EVALUATION_DATE)),
                Aggregates.group("$" + group, Accumulators.push("documents", "$$ROOT")),
                Aggregates.limit(10000),
                Aggregates.project(
                    Projections.fields(
                        Projections.computed( "documents",
                            new Document( "$slice", Arrays.asList("$documents", 10000) ) )
                    )
                ),
                Aggregates.sort(Sorts.ascending("documents." + group))
        );

        List<Document> result = collection.aggregate(pipeline).into( new ArrayList<>() );
        for (Document document : result) System.out.println(document);
        return result;
    }

    /**
    * Get the relations that belong to a specified time range.
    *
    * @param projectId The ID of the project.
    * @param dateFrom The starting date of the filtering time range.
    * @param dateTo The ending date of the filtering time range.
    *
    * @return The Document list containing the filtered relations.
    */
    public static List<Document> getRelations(LocalDate dateFrom, LocalDate dateTo, String projectId) {
        MongoDatabase database = Connection.getMongoDatabase();
        collectionExists(getRelationsIndex(projectId));
        MongoCollection<Document> collection = database.getCollection( getRelationsIndex(projectId) );

        List<Bson> pipeline = Arrays.asList(
                Aggregates.match(Filters.gte(EVALUATION_DATE, dateFrom.toString())),
                Aggregates.match(Filters.lte(EVALUATION_DATE, dateTo.toString())),
                Aggregates.sort(Sorts.descending(EVALUATION_DATE)),
                Aggregates.limit(1000)
        );

        List<Document> result = collection.aggregate(pipeline).into( new ArrayList<>() );
        for (Document document : result) System.out.println(document);
        return result;
    }

    /**
    * Get the latest relations (the last evaluation of each existing relation).
    *
    * @param projectId The ID of the project.
    *
    * @return The Document list containing the latest relations.
    */
    public static List<Document> getLatestRelationsDate(String projectId) {
        MongoDatabase database = Connection.getMongoDatabase();
        collectionExists(getRelationsIndex(projectId));
        MongoCollection<Document> collection = database.getCollection( getRelationsIndex(projectId) );

        List<Bson> pipeline = Arrays.asList(
                Aggregates.match(new Document()),
                Aggregates.sort(Sorts.descending(EVALUATION_DATE)),
                Aggregates.limit(1)
        );

        List<Document> result = collection.aggregate(pipeline).into( new ArrayList<>() );
        for (Document document : result) System.out.println(document);
        return result;
    }

    /**
    * Update the information of an existing Strategic Indicator.
    *
    * @param QMLevel The QMLevel of the entity, which in this case is Strategic Indicators.
    */
    public static UpdateResult setStrategicIndicatorValue(QMLevel QMLevel,
                                                          String hardID,
                                                          String projectId,
                                                          String strategicIndicatorID,
                                                          String strategicIndicatorName,
                                                          String strategicIndicatorDescription,
                                                          LocalDate evaluationDate,
                                                          Float value,
                                                          String info,
                                                          EstimationEvaluationDTO estimation,
                                                          List<String> missingFactors,
                                                          long datesMismatch) {

        MongoDatabase database = Connection.getMongoDatabase();
        collectionExists(getIndex(projectId, QMLevel));
        MongoCollection<Document> collection = database.getCollection( getIndex(projectId, QMLevel) );
        Document updateDoc;

        if (estimation != null) {
            List<Document> estimationArray = new ArrayList<>();
            for (QuadrupletDTO<Integer, String, Float, Float> e : estimation.getEstimation()) {
                Document estimationDoc = new Document()
                    .append(ESTIMATION_ID, e.getFirst())
                    .append(ESTIMATION_LABEL, e.getSecond())
                    .append(ESTIMATION_VALUE, e.getThird())
                    .append(ESTIMATION_UPPER_THRESHOLD, e.getFourth());
                estimationArray.add(estimationDoc);
            }

            updateDoc = new Document("$set", new Document()
                .append("_id", hardID)
                .append(PROJECT, projectId)
                .append(STRATEGIC_INDICATOR_ID, strategicIndicatorID)
                .append(EVALUATION_DATE, evaluationDate.toString())
                .append(DATA_SOURCE, "QRapids Dashboard")
                .append(NAME, strategicIndicatorName)
                .append(DESCRIPTION, strategicIndicatorDescription)
                .append(VALUE, value)
                .append(RATIONALE, info)
                .append(MISSING_FACTORS, missingFactors)
                .append(DATES_MISMATCH, (int) datesMismatch)
                .append(ESTIMATION, estimationArray));
        }

        else {
            updateDoc = new Document("$set", new Document()
                .append("_id", hardID)
                .append(PROJECT, projectId)
                .append(STRATEGIC_INDICATOR_ID, strategicIndicatorID)
                .append(EVALUATION_DATE, evaluationDate.toString())
                .append(DATA_SOURCE, "QRapids Dashboard")
                .append(NAME, strategicIndicatorName)
                .append(DESCRIPTION, strategicIndicatorDescription)
                .append(VALUE, value)
                .append(RATIONALE, info)
                .append(MISSING_FACTORS, missingFactors)
                .append(DATES_MISMATCH, (int) datesMismatch));
        }

        Document filter = new Document("_id", hardID);
        UpdateOptions updateOptions = new UpdateOptions().upsert(true);
        return collection.updateOne(filter, updateDoc, updateOptions);
    }

    /**
    * Update the information of an existing Factor.
    *
    * @param QMLevel The QMLevel of the entity, which in this case is Factors.
    */
    public static UpdateResult setFactorValue (QMLevel QMLevel,
                                               String hardID,
                                               String projectId,
                                               String factorID,
                                               String factorName,
                                               String factorDescription,
                                               LocalDate evaluationDate,
                                               Float value,
                                               String info,
                                               EstimationEvaluationDTO estimation,
                                               List<String> missingMetrics,
                                               long datesMismatch,
                                               List<String> indicators) {

        MongoDatabase database = Connection.getMongoDatabase();
        collectionExists(getIndex(projectId, QMLevel));
        MongoCollection<Document> collection = database.getCollection( getIndex(projectId, QMLevel) );
        Document updateDoc;

        if (estimation != null) {
            List<Document> estimationArray = new ArrayList<>();
            for (QuadrupletDTO<Integer, String, Float, Float> e : estimation.getEstimation()) {
                Document estimationDoc = new Document()
                    .append(ESTIMATION_ID, e.getFirst())
                    .append(ESTIMATION_LABEL, e.getSecond())
                    .append(ESTIMATION_VALUE, e.getThird())
                    .append(ESTIMATION_UPPER_THRESHOLD, e.getFourth());
                estimationArray.add(estimationDoc);
            }

            updateDoc = new Document("$set", new Document()
                .append("_id", hardID)
                .append(PROJECT, projectId)
                .append(FACTOR_ID, factorID)
                .append(EVALUATION_DATE, evaluationDate.toString())
                .append(DATA_SOURCE, "QRapids Dashboard")
                .append(NAME, factorName)
                .append(DESCRIPTION, factorDescription)
                .append(VALUE, value)
                .append(RATIONALE, info)
                .append(MISSING_METRICS, missingMetrics)
                .append(DATES_MISMATCH, (int) datesMismatch)
                .append(ARRAY_STRATEGIC_INDICATORS, indicators)
                .append(ESTIMATION, estimationArray));
        }

        else {
            updateDoc = new Document("$set", new Document()
                .append("_id", hardID)
                .append(PROJECT, projectId)
                .append(FACTOR_ID, factorID)
                .append(EVALUATION_DATE, evaluationDate.toString())
                .append(DATA_SOURCE, "QRapids Dashboard")
                .append(NAME, factorName)
                .append(DESCRIPTION, factorDescription)
                .append(VALUE, value)
                .append(RATIONALE, info)
                .append(MISSING_METRICS, missingMetrics)
                .append(DATES_MISMATCH, (int) datesMismatch)
                .append(ARRAY_STRATEGIC_INDICATORS, indicators));
        }

        Document filter = new Document("_id", hardID);
        UpdateOptions updateOptions = new UpdateOptions().upsert(true);
        return collection.updateOne(filter, updateDoc, updateOptions);
    }

    /**
    * Function that updates the factors' index with the information of the strategic indicators using
    * a concrete factor evaluation. These entries already exist in the factors' index.
    *
    * @param factor The FactorEvaluationDTO that contains the Strategic Indicators associated to that factor.
    */
    public static UpdateResult setFactorStrategicIndicatorRelation(FactorEvaluationDTO factor) {
        MongoDatabase database = Connection.getMongoDatabase();
        String indexName = getIndex( factor.getProject(), QMLevel.factors );
        collectionExists(indexName);
        MongoCollection<Document> collection = database.getCollection(indexName);

        UpdateResult response = null;
        int index = 0;

        if (!factor.getEvaluations().isEmpty()) {
            for (EvaluationDTO ignored : factor.getEvaluations()) {
                String factorID = factor.getFactorEntryID(index++);
                Document updateDoc = new Document("$set", new Document()
                    .append(ARRAY_STRATEGIC_INDICATORS, factor.getStrategicIndicators()));
                Document filter = new Document("_id", factorID);
                UpdateOptions updateOptions = new UpdateOptions().upsert(true);
                response = collection.updateOne(filter, updateDoc, updateOptions);
            }
        }
        return response;
    }

    /**
    * Function that updates the metrics' index with the information of the quality factor using
    * a concrete metric evaluation. These entries already exist in the metrics' index.
    *
    * @param metric The MetricEvaluationDTO that contains the Factors associated to that metric.
    */
    public static UpdateResult setMetricQualityFactorRelation(MetricEvaluationDTO metric) {
        String indexName = getIndex(metric.getProject(), QMLevel.metrics);
        MongoDatabase database = Connection.getMongoDatabase();
        collectionExists(indexName);
        MongoCollection<Document> collection = database.getCollection(indexName);

        UpdateResult response = null;
        int index = 0;

        if (!metric.getEvaluations().isEmpty()) {
            for (EvaluationDTO ignored : metric.getEvaluations()) {
                String metricID = metric.getMetricEntryID(index++);
                Document updateDoc = new Document("$set", new Document()
                    .append(ARRAY_FACTORS, metric.getFactors()));

                Document filter = new Document("_id", metricID);
                UpdateOptions updateOptions = new UpdateOptions().upsert(true);
                response = collection.updateOne(filter, updateDoc, updateOptions);
            }
        }
        return response;
    }

    /**
    * Create or update the relations between Factors and Strategic Indicators.
    *
    * @return A boolean indicating if the operation could be performed correctly.
    */
    public static boolean setFactorSIRelationIndex(String projectID, String[] factorID, double[] weight,
                                                   double[] sourceValue, String[] sourceCategories,
                                                   String strategicIndicatorID, LocalDate evaluationDate,
                                                   String targetValue) {

        BulkWriteOptions bulkWriteOptions = new BulkWriteOptions().ordered(false);
        List<WriteModel<Document>> writes = new ArrayList<>();
        String indexName = getRelationsIndex(projectID);
        MongoDatabase database = Connection.getMongoDatabase();
        collectionExists(indexName);
        MongoCollection<Document> collection = database.getCollection(indexName);

        for (int i = 0; i < factorID.length; i++) {
            String sourceID = String.join("-", projectID, factorID[i], evaluationDate.toString());
            String targetID = String.join("-", projectID, strategicIndicatorID, evaluationDate.toString());
            String relation = String.join("-", projectID, factorID[i]) + "->" +
                String.join("-", strategicIndicatorID, evaluationDate.toString());

            Document updateDoc = buildBulkWriteRequest(projectID, evaluationDate.toString(), relation, false, sourceID, targetID,
                safeGetFromDoubleArray(sourceValue, i), safeGetFromStringArray(sourceCategories, i),
                safeGetFromDoubleArray(weight, i), targetValue);

            Document filter = new Document("_id", relation);
            UpdateOptions updateOptions = new UpdateOptions().upsert(true);
            UpdateOneModel<Document> updateOneModel = new UpdateOneModel<>(filter, updateDoc, updateOptions);
            writes.add(updateOneModel);
        }

        BulkWriteResult bulkWriteResult = collection.bulkWrite(writes, bulkWriteOptions);
        for (BulkWriteUpsert upsert : bulkWriteResult.getUpserts())
            System.out.println("Upserted document ID: " + upsert.getId());
        int insertedDocs = bulkWriteResult.getMatchedCount() + bulkWriteResult.getUpserts().size();
        return bulkWriteResult.wasAcknowledged() &&
                factorID.length == insertedDocs;
    }

    /**
    * Create or update the relations between Metrics and Factors.
    *
    * @return A boolean indicating if the operation could be performed correctly.
    */
    public static boolean setMetricQFRelationIndex(String projectID, String[] metricID, double[] weight,
                                                   double[] sourceValue, String[] sourceCategories,
                                                   String qualityFactorID, LocalDate evaluationDate,
                                                   String targetValue) {

        BulkWriteOptions bulkWriteOptions = new BulkWriteOptions().ordered(false);
        List<WriteModel<Document>> writes = new ArrayList<>();
        String indexName = getRelationsIndex(projectID);
        MongoDatabase database = Connection.getMongoDatabase();
        collectionExists(indexName);
        MongoCollection<Document> collection = database.getCollection(indexName);

        for (int i = 0; i < metricID.length; i++) {
            String sourceID = String.join("-", projectID, metricID[i], evaluationDate.toString());
            String targetID = String.join("-", projectID, qualityFactorID, evaluationDate.toString());
            String relation = String.join("-", projectID, metricID[i]) + "->" +
                String.join("-", qualityFactorID, evaluationDate.toString());

            Document updateDoc = buildBulkWriteRequest(projectID, evaluationDate.toString(), relation, true, sourceID, targetID,
                safeGetFromDoubleArray(sourceValue, i), safeGetFromStringArray(sourceCategories, i),
                safeGetFromDoubleArray(weight, i), targetValue);

            Document filter = new Document("_id", relation);
            UpdateOptions updateOptions = new UpdateOptions().upsert(true);
            UpdateOneModel<Document> updateOneModel = new UpdateOneModel<>(filter, updateDoc, updateOptions);
            writes.add(updateOneModel);
        }

        BulkWriteResult bulkWriteResult = collection.bulkWrite(writes, bulkWriteOptions);
        for (BulkWriteUpsert upsert : bulkWriteResult.getUpserts())
            System.out.println("Upserted document ID: " + upsert.getId());
        int insertedDocs = bulkWriteResult.getMatchedCount() + bulkWriteResult.getUpserts().size();
        return bulkWriteResult.wasAcknowledged() &&
               metricID.length == insertedDocs;
    }

    /**
    * Build a BulkWriteRequest to create or update documents in the Relations collection.
    *
    * @return The created BulkWriteRequest as a Document.
    */
    public static Document buildBulkWriteRequest(String projectID,
                                                 String evaluationDate,
                                                 String relation,
                                                 Boolean metrics,
                                                 String sourceID,
                                                 String targetID,
                                                 double value,
                                                 String sourceCategory,
                                                 double weight,
                                                 String targetValue) {

        String sourceType, targetType;
        if (metrics) {
            sourceType = METRIC_TYPE;
            targetType = FACTOR_TYPE;
        }
        else {
            sourceType = FACTOR_TYPE;
            targetType = STRATEGIC_INDICATOR_TYPE;
        }

        return new Document("$set", new Document()
            .append(EVALUATION_DATE, evaluationDate)
            .append(PROJECT, projectID)
            .append(RELATION, relation)
            .append(SOURCEID, sourceID)
            .append(SOURCETYPE, sourceType)
            .append(TARGETID, targetID)
            .append(TARGETTPYE, targetType)
            .append(VALUE, value)
            .append(WEIGHT, weight)
            .append(TARGETVALUE, targetValue)
            .append(SOURCELABEL, sourceCategory));
    }

    /**
    * Get the currently existing collections in the database.
    *
    * @return A list containing the collections' names.
    */
    public static List<String> getCollections() {
        MongoDatabase database = Connection.getMongoDatabase();
        List<String> collectionNames = new ArrayList<>();
        for (String name : database.listCollectionNames()) collectionNames.add(name);
        return collectionNames;
    }

    private static void collectionExists(String collectionName) throws MongoException {
        MongoDatabase database = Connection.getMongoDatabase();
        List<String> collections = database.listCollectionNames().into(new ArrayList<>());
        if (!collections.contains(collectionName))
            throw new MongoException("Collection '" + collectionName + "' does not exist");
    }

    /**
    * Get the existing relations' (Metric -> Factor) documents in a provided date.
    *
    * @param projectId The ID of the project.
    * @param evaluationDate The date we want the relations to belong to.
    *
    * @return A list of Documents which contains the filtered relations.
    */
	public static List<Document> getFactorMetricsRelations(String projectId, String evaluationDate) {
        MongoDatabase database = Connection.getMongoDatabase();
        String indexName = INDEX_RELATIONS + "." + projectId;
        collectionExists(indexName);
        MongoCollection<Document> collection = database.getCollection(indexName);

        List<Bson> pipeline = Arrays.asList(
                Aggregates.match(Filters.eq(PROJECT, projectId)),
                Aggregates.match(Filters.eq(EVALUATION_DATE, evaluationDate)),
                Aggregates.match(Filters.eq(TARGETTPYE, "factors")),
                Aggregates.limit(1000)
        );

        List<Document> result = collection.aggregate(pipeline).into( new ArrayList<>() );
        for (Document document : result) System.out.println(document);
        return result;
	}

    /**
    * Create the Strategic Indicators' index, if it does not exist already.
    *
    * @param projectID The ID of the project.
    *
    * @return A boolean indicating if the index was created correctly.
    */
    public static boolean prepareSIIndex(String projectID) {
        MongoDatabase database = Connection.getMongoDatabase();
        String collectionName = getIndexName(STRATEGIC_INDICATOR_TYPE, projectID);

        if (database.listCollectionNames().into(new ArrayList<>()).contains(collectionName)) {
            System.out.println("INDEX ALREADY EXISTS: " + collectionName);
            return false;
        }

        try {
            ValidationOptions validationOptions = new ValidationOptions().validator(STRATEGIC_INDICATORS_MAPPING);
            CreateCollectionOptions options = new CreateCollectionOptions().validationOptions(validationOptions);
            database.createCollection(collectionName, options);
        } catch (MongoException e) {
            e.printStackTrace();
            System.out.println("INDEX COULD NOT BE CREATED: " + collectionName);
            return false;
        }

        for (String name : database.listCollectionNames())
            if (name.equals(collectionName)) {
                System.out.println("INDEX CREATED: " + collectionName);
                return true;
            }
        System.out.println("INDEX COULD NOT BE CREATED: " + collectionName);
        return false;
    }

    /**
     * Create the Factors' index, if it does not exist already.
     *
     * @param projectID The ID of the project.
     *
     * @return A boolean indicating if the index was created correctly.
     */
    public static boolean prepareQFIndex(String projectID) {
        MongoDatabase database = Connection.getMongoDatabase();
        String collectionName = getIndexName(FACTOR_TYPE, projectID);

        if (database.listCollectionNames().into(new ArrayList<>()).contains(collectionName)) {
            System.out.println("INDEX ALREADY EXISTS: " + collectionName);
            return false;
        }

        try {
            ValidationOptions validationOptions = new ValidationOptions().validator(FACTORS_MAPPING);
            CreateCollectionOptions options = new CreateCollectionOptions().validationOptions(validationOptions);
            database.createCollection(collectionName, options);
        } catch (MongoException e) {
            e.printStackTrace();
            System.out.println("INDEX COULD NOT BE CREATED: " + collectionName);
            return false;
        }

        for (String name : database.listCollectionNames())
            if (name.equals(collectionName)) {
                System.out.println("INDEX CREATED: " + collectionName);
                return true;
            }
        System.out.println("INDEX COULD NOT BE CREATED: " + collectionName);
        return false;
    }

}
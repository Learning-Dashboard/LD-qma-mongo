package util;

import DTOs.EstimationEvaluationDTO;
import DTOs.EvaluationDTO;
import DTOs.FactorEvaluationDTO;
import DTOs.MetricEvaluationDTO;
import DTOs.QuadrupletDTO;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.BulkWriteUpsert;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.time.LocalDate;
import java.util.*;

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
        else return 0d;
    }

    public static String getStringFromMapOrDefault(Map<String, Object> map, String k, String def) {
        String valueOfMap = String.valueOf(map.get(k));
        if (valueOfMap.equals("null")) return def;
        else return valueOfMap;
    }

    public static String getStringFromObjectOrDefault(Object o, String def) {
        String valueOfObject = String.valueOf(o);
        if (o == null) return def;
        else if (valueOfObject.equals("null")) return def;
        else if (valueOfObject.isEmpty()) return def;
        else return valueOfObject;
    }

    public static String getStringFromStringMapOrDefault(Map<String, String> map, String k, String def) {
        String valueOfMap = String.valueOf(map.get(k));
        if (valueOfMap.equals("null")) return def;
        else return valueOfMap;
    }

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

    public static List<Document> getLatest(QMLevel QMLevel, String projectId, String parent) {
        MongoDatabase database = Connection.getMongoDatabase();
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
        for (Document document : result) System.out.println(document.toJson());
        return result;
    }

    public static List<Document> getLatest(String projectId, QMLevel QMLevel) {
        return getLatest(QMLevel, projectId,"all");
    }

    public static List<Document> getLatestElement(String projectId, QMLevel QMLevel, String elementId) {
        MongoDatabase database = Connection.getMongoDatabase();
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
        for (Document document : result) System.out.println(document.toJson());
        return result;
    }

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


    // Parent means you are searching for elements related to others, e.g. the factors for a specific SI
    private static Bson getLatestParentQueryBuilder(String parent, QMLevel QMLevel) {
        Bson query;
        if (parent.equals("all")) query = new Document();
        else {
            if (QMLevel == Constants.QMLevel.metrics) query = Filters.elemMatch(ARRAY_FACTORS, Filters.eq(parent));
            else query = Filters.elemMatch(ARRAY_STRATEGIC_INDICATORS, Filters.eq(parent));
        }
        return query;
    }

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
                Bson factorsFilter = Filters.elemMatch(ARRAY_FACTORS, Filters.eq(parent));
                andFilters.add(factorsFilter);
                andFilters.add(dateRangeFilter);
            }
            else {
                Bson strategicIndicatorsFilter = Filters.elemMatch(ARRAY_STRATEGIC_INDICATORS, Filters.eq(parent));
                andFilters.add(strategicIndicatorsFilter);
                andFilters.add(dateRangeFilter);
            }
        }

        return Filters.and(andFilters);
    }

    public static List<Document> getRanged(QMLevel QMLevel, String projectId , String parent, LocalDate dateFrom, LocalDate dateTo) {
        MongoDatabase database = Connection.getMongoDatabase();
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

    public static List<Document> getRanged(QMLevel QMLevel, String projectId, LocalDate dateFrom, LocalDate dateTo) {
        return getRanged(QMLevel, projectId,"all", dateFrom, dateTo);
    }

    public static List<Document> getRangedElement(String projectId, QMLevel QMLevel, String elementId, LocalDate from, LocalDate to) {
        MongoDatabase database = Connection.getMongoDatabase();
        MongoCollection<Document> collection = database.getCollection( getIndex(projectId, QMLevel) );
        String group = getIDtoGroup(QMLevel);

        List<Bson> pipeline = Arrays.asList(
                Aggregates.match(Filters.eq(group, elementId)),
                Aggregates.match(Filters.gte(EVALUATION_DATE, from)),
                Aggregates.match(Filters.lte(EVALUATION_DATE, to)),
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
        for (Document document : result) System.out.println(document.toJson());
        return result;
    }

    public static List<Document> getRelations(LocalDate dateFrom, LocalDate dateTo, String projectId) {
        MongoDatabase database = Connection.getMongoDatabase();
        MongoCollection<Document> collection = database.getCollection( getRelationsIndex(projectId) );

        List<Bson> pipeline = Arrays.asList(
                Aggregates.match(Filters.gte(EVALUATION_DATE, dateFrom)),
                Aggregates.match(Filters.lte(EVALUATION_DATE, dateTo)),
                Aggregates.limit(1000),
                Aggregates.sort(Sorts.descending(EVALUATION_DATE))
        );

        List<Document> result = collection.aggregate(pipeline).into( new ArrayList<>() );
        for (Document document : result) System.out.println(document.toJson());
        return result;
    }

    public static List<Document> getLatestRelationsDate(String projectId) {
        MongoDatabase database = Connection.getMongoDatabase();
        MongoCollection<Document> collection = database.getCollection( getRelationsIndex(projectId) );

        List<Bson> pipeline = Arrays.asList(
                Aggregates.match(new Document()),
                Aggregates.limit(1),
                Aggregates.sort(Sorts.descending(EVALUATION_DATE))
        );

        List<Document> result = collection.aggregate(pipeline).into( new ArrayList<>() );
        for (Document document : result) System.out.println(document.toJson());
        return result;
    }

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
        MongoCollection<Document> collection = database.getCollection( getIndex(projectId, QMLevel) );
        List<Document> estimationArray;

        if (estimation == null || estimation.getEstimation() == null) estimationArray = new ArrayList<>();
        else {
            estimationArray = new ArrayList<>();
            for (QuadrupletDTO<Integer, String, Float, Float> e : estimation.getEstimation()) {
                Document estimationDoc = new Document()
                    .append(ESTIMATION_ID, e.getFirst())
                    .append(ESTIMATION_LABEL, e.getSecond())
                    .append(ESTIMATION_VALUE, e.getThird())
                    .append(ESTIMATION_UPPER_THRESHOLD, e.getFourth());
                estimationArray.add(estimationDoc);
            }
        }

        Document updateDoc = new Document("$set", new Document()
            .append("_id", hardID)
            .append(PROJECT, projectId)
            .append(STRATEGIC_INDICATOR_ID, strategicIndicatorID)
            .append(EVALUATION_DATE, evaluationDate)
            .append(DATA_SOURCE, "QRapids Dashboard")
            .append(NAME, strategicIndicatorName)
            .append(DESCRIPTION, strategicIndicatorDescription)
            .append(VALUE, value)
            .append(RATIONALE, info)
            .append(MISSING_FACTORS, missingFactors)
            .append(DATES_MISMATCH, datesMismatch)
            .append(ESTIMATION, estimationArray));

        Document filter = new Document("_id", hardID);
        UpdateOptions updateOptions = new UpdateOptions().upsert(true);
        return collection.updateOne(filter, updateDoc, updateOptions);
    }

    public static UpdateResult setFactorValue  (QMLevel QMLevel,
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
        MongoCollection<Document> collection = database.getCollection( getIndex(projectId, QMLevel) );
        List<Document> estimationArray;

        if (estimation == null || estimation.getEstimation() == null) estimationArray = new ArrayList<>();
        else {
            estimationArray = new ArrayList<>();
            for (QuadrupletDTO<Integer, String, Float, Float> e : estimation.getEstimation()) {
                Document estimationDoc = new Document()
                    .append(ESTIMATION_ID, e.getFirst())
                    .append(ESTIMATION_LABEL, e.getSecond())
                    .append(ESTIMATION_VALUE, e.getThird())
                    .append(ESTIMATION_UPPER_THRESHOLD, e.getFourth());
                estimationArray.add(estimationDoc);
            }
        }

        Document updateDoc = new Document("$set", new Document()
            .append("_id", hardID)
            .append(PROJECT, projectId)
            .append(FACTOR_ID, factorID)
            .append(EVALUATION_DATE, evaluationDate)
            .append(DATA_SOURCE, "QRapids Dashboard")
            .append(NAME, factorName)
            .append(DESCRIPTION, factorDescription)
            .append(VALUE, value)
            .append(RATIONALE, info)
            .append(MISSING_METRICS, missingMetrics)
            .append(DATES_MISMATCH, datesMismatch)
            .append(ARRAY_STRATEGIC_INDICATORS, indicators)
            .append(ESTIMATION, estimationArray));

        Document filter = new Document("_id", hardID);
        UpdateOptions updateOptions = new UpdateOptions().upsert(true);
        return collection.updateOne(filter, updateDoc, updateOptions);
    }

    // Function that updates the factors' index with the information of the strategic indicators using
    // a concrete factor evaluation. These entries already exist in the factors' index.
    public static UpdateResult setFactorStrategicIndicatorRelation(FactorEvaluationDTO factor) {
        MongoDatabase database = Connection.getMongoDatabase();
        String indexName = getIndex( factor.getProject(), QMLevel.factors );
        MongoCollection<Document> collection = database.getCollection(indexName);

        UpdateResult response = null;
        int index = 0;

        if (!factor.getEvaluations().isEmpty()) {
            for (EvaluationDTO eval : factor.getEvaluations()) {
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

    // Function that updates the metrics' index with the information of the quality factor using
    // a concrete metric evaluation. These entries already exist in the metrics' index.
    public static UpdateResult setMetricQualityFactorRelation(MetricEvaluationDTO metric) {
        String indexName = getIndex(metric.getProject(), QMLevel.metrics);
        MongoDatabase database = Connection.getMongoDatabase();
        MongoCollection<Document> collection = database.getCollection(indexName);

        UpdateResult response = null;
        int index = 0;

        if (!metric.getEvaluations().isEmpty()) {
            for (EvaluationDTO eval : metric.getEvaluations()) {
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

    public static boolean setFactorSIRelationIndex(String projectID, String[] factorID, double[] weight,
                                                   double[] sourceValue, String[] sourceCategories,
                                                   String strategicIndicatorID, LocalDate evaluationDate,
                                                   String targetValue) {

        BulkWriteOptions bulkWriteOptions = new BulkWriteOptions().ordered(false);
        List<WriteModel<Document>> writes = new ArrayList<>();
        String indexName = getRelationsIndex(projectID);
        MongoDatabase database = Connection.getMongoDatabase();
        MongoCollection<Document> collection = database.getCollection(indexName);

        for (int i = 0; i < factorID.length; i++) {
            String sourceID = String.join("-", projectID, factorID[i], evaluationDate.toString());
            String targetID = String.join("-", projectID, strategicIndicatorID, evaluationDate.toString());
            String relation = String.join("-", projectID, factorID[i]) + "->" +
                String.join("-", strategicIndicatorID, evaluationDate.toString());

            Document updateDoc = buildBulkWriteRequest(projectID, evaluationDate, relation, false, sourceID, targetID,
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
        return bulkWriteResult.wasAcknowledged() &&
               bulkWriteResult.getMatchedCount() == bulkWriteResult.getInsertedCount();
    }

    public static boolean setMetricQFRelationIndex(String projectID, String[] metricID, double[] weight,
                                                   double[] sourceValue, String[] sourceCategories,
                                                   String qualityFactorID, LocalDate evaluationDate,
                                                   String targetValue) {

        BulkWriteOptions bulkWriteOptions = new BulkWriteOptions().ordered(false);
        List<WriteModel<Document>> writes = new ArrayList<>();
        String indexName = getRelationsIndex(projectID);
        MongoDatabase database = Connection.getMongoDatabase();
        MongoCollection<Document> collection = database.getCollection(indexName);

        for (int i = 0; i < metricID.length; i++) {
            String sourceID = String.join("-", projectID, metricID[i], evaluationDate.toString());
            String targetID = String.join("-", projectID, qualityFactorID, evaluationDate.toString());
            String relation = String.join("-", projectID, metricID[i]) + "->" +
                String.join("-", qualityFactorID, evaluationDate.toString());

            Document updateDoc = buildBulkWriteRequest(projectID, evaluationDate, relation, true, sourceID, targetID,
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
        return bulkWriteResult.wasAcknowledged() &&
               bulkWriteResult.getMatchedCount() == bulkWriteResult.getInsertedCount();
    }

    public static Document buildBulkWriteRequest(String projectID,
                                                     LocalDate evaluationDate,
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

    public static List<String> getCollections() {
        MongoDatabase database = Connection.getMongoDatabase();
        List<String> collectionNames = new ArrayList<>();
        for (String name : database.listCollectionNames()) collectionNames.add(name);
        return collectionNames;
    }

	public static List<Document> getFactorMetricsRelations(String projectId, String evaluationDate) {
        MongoDatabase database = Connection.getMongoDatabase();
        String indexName = INDEX_RELATIONS + "." + projectId;
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

    public static boolean prepareSIIndex(String projectID) {
        MongoDatabase database = Connection.getMongoDatabase();
        String collectionName = getIndexName(STRATEGIC_INDICATOR_TYPE, projectID);

        if (database.listCollectionNames().into(new ArrayList<>()).contains(collectionName)) {
            System.out.println("INDEX ALREADY EXISTS: " + collectionName);
            return false;
        }

        ValidationOptions validationOptions = new ValidationOptions().validator(STRATEGIC_INDICATORS_MAPPING);
        CreateCollectionOptions options = new CreateCollectionOptions().validationOptions(validationOptions);
        database.createCollection(collectionName, options);

        for (String name : database.listCollectionNames())
            if (name.equals(collectionName)) {
                System.out.println("INDEX CREATED: " + collectionName);
                return true;
            }
        System.out.println("INDEX ALREADY EXISTS: " + collectionName);
        return false;
    }

    public static boolean prepareQFIndex(String projectID) {
        MongoDatabase database = Connection.getMongoDatabase();
        String collectionName = getIndexName(FACTOR_TYPE, projectID);

        if (database.listCollectionNames().into(new ArrayList<>()).contains(collectionName)) {
            System.out.println("INDEX ALREADY EXISTS: " + collectionName);
            return false;
        }

        ValidationOptions validationOptions = new ValidationOptions().validator(FACTORS_MAPPING);
        CreateCollectionOptions options = new CreateCollectionOptions().validationOptions(validationOptions);
        database.createCollection(collectionName, options);

        for (String name : database.listCollectionNames())
            if (name.equals(collectionName)) {
                System.out.println("INDEX CREATED: " + collectionName);
                return true;
            }
        System.out.println("INDEX ALREADY EXISTS: " + collectionName);
        return false;
    }

}




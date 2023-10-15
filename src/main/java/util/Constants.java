package util;


import org.bson.Document;

import java.util.Arrays;

public class Constants {

    // IDs for element aggregation
    static final String METRIC_ID = "metric";
    static final String FACTOR_ID = "factor";
    static final String STRATEGIC_INDICATOR_ID = "strategic_indicator";

    // Content of the field _type of the index
    static final String METRIC_TYPE = "metrics";
    public static final String FACTOR_TYPE = "factors";
    public static final String STRATEGIC_INDICATOR_TYPE = "strategic_indicators";
    static final String RELATIONS_TYPE = "relations";

    // FIELDS

    // Related to the element
    public static final String NAME = "name";
    static final String DESCRIPTION = "description";

    // Related to the evaluation
    public static final String EVALUATION_DATE = "evaluationDate";
    public static final String VALUE = "value";
    static final String DATA_SOURCE = "datasource";
    static final String PROJECT = "project";
    static final String RATIONALE = "info";
    static final String MISSING_FACTORS = "missing_factors";
    static final String MISSING_METRICS = "missing_metrics";
    static final String DATES_MISMATCH = "dates_mismatch_days";

    // Related to the relations
    public static final String RELATION = "relation";
    public static final String SOURCEID = "sourceId";
    public static final String SOURCETYPE = "sourceType";
    public static final String TARGETID = "targetId";
    public static final String TARGETTPYE = "targetType";
    public static final String WEIGHT = "weight";
    public static final String TARGETVALUE = "targetValue";
    public static final String SOURCELABEL = "sourceLabel";

    // INDEXES

    public static final String INDEX_STRATEGIC_INDICATORS = "strategic_indicators";
    public static final String INDEX_FACTORS = "factors";
    public static final String INDEX_METRICS = "metrics";
    public static final String INDEX_RELATIONS = "relations";

    // ARRAYS

    static final String ARRAY_FACTORS = "factors";
    static final String ARRAY_STRATEGIC_INDICATORS = "indicators";

    // ESTIMATIONS

    static final String ESTIMATION = "estimation";
    static final String ESTIMATION_ID = "id";
    static final String ESTIMATION_VALUE = "value";
    static final String ESTIMATION_LABEL = "label";
    static final String ESTIMATION_UPPER_THRESHOLD = "upperThreshold";

    // OTHERS

    public enum QMLevel { metrics, factors, strategic_indicators, relations }

    // MAPPINGS

    static final Document STRATEGIC_INDICATORS_MAPPING = new Document("$jsonSchema", new Document()
        .append("bsonType", "object")
        .append("required", Arrays.asList("_id", "datasource", "description", "evaluationDate", "strategic_indicator", "name", "project", "value", "info", "missing_factors", "dates_mismatch_days"))
        .append("properties", new Document()
            .append("_id", new Document("bsonType", Arrays.asList("objectId", "string")))
            .append("project", new Document("bsonType", Arrays.asList("string", "null")))
            .append("strategic_indicator", new Document("bsonType", Arrays.asList("string", "null")))
            .append("evaluationDate", new Document("bsonType", Arrays.asList("string", "null")))
            .append("datasource", new Document("bsonType", Arrays.asList("string", "null")))
            .append("name", new Document("bsonType", Arrays.asList("string", "null")))
            .append("description", new Document("bsonType", Arrays.asList("string", "null")))
            .append("value", new Document("bsonType", Arrays.asList("double", "int", "null")))
            .append("info", new Document("bsonType", Arrays.asList("string", "null")))
            .append("missing_factors", new Document()
                .append("bsonType", "array")
                .append("items", new Document("bsonType", "string"))
            )
            .append("dates_mismatch_days", new Document("bsonType", Arrays.asList("int", "null")))
        )
    );

    static final Document FACTORS_MAPPING = new Document("$jsonSchema", new Document()
        .append("bsonType", "object")
        .append("required", Arrays.asList("_id", "project", "factor", "evaluationDate", "indicators", "name", "description", "datasource", "value", "info", "missing_metrics", "dates_mismatch_days"))
        .append("properties", new Document()
            .append("_id", new Document("bsonType", Arrays.asList("objectId", "string")))
            .append("project", new Document("bsonType", Arrays.asList("string", "null")))
            .append("factor", new Document("bsonType", Arrays.asList("string", "null")))
            .append("evaluationDate", new Document("bsonType", Arrays.asList("string", "null")))
            .append("datasource", new Document("bsonType", Arrays.asList("string", "null")))
            .append("name", new Document("bsonType", Arrays.asList("string", "null")))
            .append("description", new Document("bsonType", Arrays.asList("string", "null")))
            .append("value", new Document("bsonType", Arrays.asList("double", "int", "null")))
            .append("info", new Document("bsonType", Arrays.asList("string", "null")))
            .append("missing_metrics", new Document()
                .append("bsonType", "array")
                .append("items", new Document("bsonType", "string"))
            )
            .append("dates_mismatch_days", new Document("bsonType", Arrays.asList("int", "null")))
            .append("indicators", new Document()
                .append("bsonType", "array")
                .append("items", new Document("bsonType", "string"))
            )
        )
    );
}

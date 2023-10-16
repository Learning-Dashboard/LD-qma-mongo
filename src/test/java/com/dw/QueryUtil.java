package com.dw;

import DTOs.*;
import DTOs.Relations.RelationDTO;
import evaluation.Relations;
import util.Connection;

import evaluation.Factor;
import evaluation.Metric;
import evaluation.StrategicIndicator;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("ALL")
class QueryUtil {

    public static void main(String[] args) throws IOException {
        LocalDate dateFrom = LocalDate.of(2023,10, 13);
        LocalDate dateTo = LocalDate.of(2023, 10, 15);
        String projectId="test";
        String factorCQ = "fB";
        String strategicIndicatorQ = "sA";
        String metricQ = "mA";

        // Set correct values before running tests
        String ip = "";
        int port = 0;
        String database = "";
        String username = null;
        String password = null;

        System.err.println("----- QUERY TEST UTIL -----");
        try {
            //OPEN CONNECTION
            System.err.println("-- OPEN CONNECTION");
            Connection.initConnection(ip, port, database, username, password);

            // RELATIONS
            String[] factorsID = {"fA", "fB"};
            double[] sourceValues = {0.8d, 0.2d};
            double[] weights = {0.7d, 0.3d};
            Relations.setStrategicIndicatorFactorRelation(projectId, factorsID, strategicIndicatorQ, dateFrom,
                weights, sourceValues, null, "0.80");

            String[] factorsID2 = {"runtimeErrors", "avrResponseTime"};
            double[] sourceValues2 = {0.5d, 0.1d};
            String[] sourceCategories = {"Medium", "Low"};
            Relations.setStrategicIndicatorFactorRelation(projectId, factorsID2,"HWReliability",
                dateFrom, new double[]{0d}, sourceValues2, sourceCategories, "High");

            // GET
            ArrayList<RelationDTO> relations = Relations.getRelations("test", dateFrom);

            // CLASS: FACTOR
            System.err.println("-- FACTORS 1 ");
            List<FactorEvaluationDTO> factorsEvaluationLatest = Factor.getEvaluations(projectId);
            System.err.println("-- FACTORS 2 ");
            FactorEvaluationDTO factorEvaluationDTO = Factor.getSingleEvaluation(projectId, factorCQ);
            System.err.println("-- FACTORS 3 ");
            List<FactorEvaluationDTO> factorsEvaluationRanged = Factor.getEvaluations(projectId, dateFrom, dateTo);

            System.err.println("-- FACTORS 4 ");
            List<FactorMetricEvaluationDTO> factorsMetricsLatest = Factor.getMetricsEvaluations(projectId);
            System.err.println("-- FACTORS 5 ");
            List<FactorMetricEvaluationDTO> factorsMetricsRanged = Factor.getMetricsEvaluations(projectId, dateFrom, dateTo);

            System.err.println("-- FACTORS 6 ");
            FactorMetricEvaluationDTO metricsEvaluationLatest = Factor.getMetricsEvaluations(projectId, factorCQ);
            System.err.println("-- FACTORS 7 ");
            FactorMetricEvaluationDTO metricsEvaluationRanged = Factor.getMetricsEvaluations(projectId, factorCQ, dateFrom, dateTo);

            // CLASS: METRIC
            System.err.println("--  METRIC 1 ");
            List<MetricEvaluationDTO> allMetricsEvaluationLatest = Metric.getEvaluations(projectId);
            System.err.println("-- METRIC 2 ");
            List<MetricEvaluationDTO> allMetricsEvaluationRanged = Metric.getEvaluations(projectId, dateFrom, dateTo);
            System.err.println("-- METRIC 3 ");
            MetricEvaluationDTO latestMetricEvaluation = Metric.getSingleEvaluation(projectId, metricQ);
            System.err.println("-- METRIC 4 ");
            MetricEvaluationDTO singleMetricEvaluationRanged = Metric.getSingleEvaluation(projectId, metricQ, dateFrom, dateTo);

            // CLASS: STRATEGIC INDICATOR
            System.err.println("-- STRATEGIC INDICATOR 1");
            List<StrategicIndicatorEvaluationDTO> strategicIndicatorsEvaluationLatest = StrategicIndicator.getEvaluations(projectId);
            System.err.println("-- STRATEGIC INDICATOR 2");
            StrategicIndicatorEvaluationDTO strategicIndicatorEvaluationDTO = StrategicIndicator.getSingleEvaluation(projectId, strategicIndicatorQ);
            System.err.println("-- STRATEGIC INDICATOR 3");
            List<StrategicIndicatorEvaluationDTO> strategicIndicatorsEvaluationRanged = StrategicIndicator.getEvaluations(projectId, dateFrom, dateTo);

            System.err.println("-- STRATEGIC INDICATOR 4");
            StrategicIndicatorFactorEvaluationDTO SIfactorsEvaluationLatest = StrategicIndicator.getFactorsEvaluations(projectId, strategicIndicatorQ);
            System.err.println("-- STRATEGIC INDICATOR 5 ");
            StrategicIndicatorFactorEvaluationDTO SIfactorsEvaluationRanged = StrategicIndicator.getFactorsEvaluations(projectId, strategicIndicatorQ, dateFrom, dateTo);

            System.err.println("-- STRATEGIC INDICATOR 6 ");
            List<StrategicIndicatorFactorEvaluationDTO> strategicIndicatorsFactorsLatest = StrategicIndicator.getFactorsEvaluations(projectId);
            System.err.println("-- STRATEGIC INDICATOR 7 ");
            List<StrategicIndicatorFactorEvaluationDTO> strategicIndicatorsFactorsMetricsRanged = StrategicIndicator.getFactorsEvaluations(projectId, dateFrom, dateTo);

            System.err.println("-- STRATEGIC INDICATOR 8 ");
            List<FactorMetricEvaluationDTO> strategicIndicatorFactorsMetricsLatest = StrategicIndicator.getMetricsEvaluations(projectId, strategicIndicatorQ);
            System.err.println("-- STRATEGIC INDICATOR 9");
            List<FactorMetricEvaluationDTO> strategicIndicatorFactorsMetricsRanged = StrategicIndicator.getMetricsEvaluations(projectId, strategicIndicatorQ, dateFrom, dateTo);

            // CLOSE CONNECTION
            System.err.println("-- CLOSE CONNECTION");
            Connection.closeConnection();
        }
        catch (Exception ex) {
            System.err.println("----- QUERY TEST UTIL - ERROR " + ex.toString());
            throw ex;
        }

    }
}


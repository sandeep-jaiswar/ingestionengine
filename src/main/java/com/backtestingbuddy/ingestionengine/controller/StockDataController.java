package com.backtestingbuddy.ingestionengine.controller;

import com.backtestingbuddy.ingestionengine.dto.SecurityRequest;
import com.backtestingbuddy.ingestionengine.exception.YahooFinanceException;
import com.backtestingbuddy.ingestionengine.service.ClickHouseService;
import com.backtestingbuddy.ingestionengine.service.YahooFinanceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import yahoofinance.Stock;
import yahoofinance.histquotes.HistoricalQuote;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/ingestion")
public class StockDataController {

    private static final Logger logger = LoggerFactory.getLogger(StockDataController.class);

    @Autowired
    private YahooFinanceService yahooFinanceService;

    @Autowired
    private ClickHouseService clickHouseService;

    @PostMapping("/stock")
    public ResponseEntity<?> ingestStockData(@RequestBody SecurityRequest securityRequest) {
        logger.debug("Entering ingestStockData method.");

        if (securityRequest == null || securityRequest.getSecurityName() == null || securityRequest.getSecurityName().trim().isEmpty()) {
            logger.debug("Validation failed: Security name is missing or empty.");
            return ResponseEntity.badRequest().body("Security name must be provided.");
        }

        String symbol = securityRequest.getSecurityName().trim().toUpperCase();
        logger.info("Received request to ingest data for symbol: {}", symbol);
        logger.debug("Processing symbol: {}", symbol);

        try {
            // Fetch current quote
            logger.debug("Calling YahooFinanceService.getStockQuote for {}", symbol);
            Stock stockQuote = yahooFinanceService.getStockQuote(symbol);
            logger.debug("Received stock quote response for {}. Stock: {}", symbol, stockQuote);

            if (stockQuote != null && stockQuote.getQuote() != null) {
                logger.debug("Stock quote found for {}. Saving...", symbol);
                clickHouseService.saveStockQuote(stockQuote);
                logger.info("Successfully fetched and saved stock quote for {}", symbol);
                logger.debug("Stock quote saved successfully for {}", symbol);
            } else {
                logger.warn("Could not retrieve stock quote for symbol: {}", symbol);
                logger.debug("Stock quote is null or quote is null for {}", symbol);
            }

            // Fetch historical data
            logger.debug("Calling YahooFinanceService.getHistoricalData for {}", symbol);
            List<HistoricalQuote> historicalData = yahooFinanceService.getHistoricalData(symbol);
            logger.debug("Received historical data response for {}. Records count: {}", symbol, historicalData != null ? historicalData.size() : 0);

            if (historicalData != null && !historicalData.isEmpty()) {
                logger.debug("Historical data found for {}. Saving {} records...", symbol, historicalData.size());
                clickHouseService.saveHistoricalData(symbol, historicalData);
                logger.info("Successfully fetched and saved {} historical records for {}", historicalData.size(), symbol);
                logger.debug("Historical data saved successfully for {}", symbol);
            } else {
                logger.warn("Could not retrieve historical data for symbol: {}", symbol);
                logger.debug("Historical data is null or empty for {}", symbol);
            }
            
            if (stockQuote == null && (historicalData == null || historicalData.isEmpty())) {
                 logger.warn("No data (neither quote nor historical) found for symbol: {}", symbol);
                 return ResponseEntity.status(HttpStatus.NOT_FOUND).body("No data found for symbol: " + symbol);
            }

            logger.debug("Ingestion process completed successfully for symbol: {}", symbol);
            return ResponseEntity.ok("Data ingestion process completed for symbol: " + symbol);

        } catch (YahooFinanceException e) {
            logger.error("YahooFinanceException for symbol {}: {}", symbol, e.getMessage(), e);
            logger.debug("YahooFinanceException details for {}: ", symbol, e);
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body("Error fetching data from Yahoo Finance: " + e.getMessage());
        } catch (Exception e) {
            logger.error("An unexpected error occurred while processing symbol {}: {}", symbol, e.getMessage(), e);
            logger.debug("Unexpected error details for {}: ", symbol, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("An unexpected error occurred: " + e.getMessage());
        }
    }
}

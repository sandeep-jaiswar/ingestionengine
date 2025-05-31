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
        if (securityRequest == null || securityRequest.getSecurityName() == null || securityRequest.getSecurityName().trim().isEmpty()) {
            return ResponseEntity.badRequest().body("Security name must be provided.");
        }

        String symbol = securityRequest.getSecurityName().trim().toUpperCase();
        logger.info("Received request to ingest data for symbol: {}", symbol);

        try {
            // Fetch current quote
            logger.info("Fetching stock quote for {}", symbol);
            Stock stockQuote = yahooFinanceService.getStockQuote(symbol);
            if (stockQuote != null && stockQuote.getQuote() != null) {
                clickHouseService.saveStockQuote(stockQuote);
                logger.info("Successfully fetched and saved stock quote for {}", symbol);
            } else {
                logger.warn("Could not retrieve stock quote for symbol: {}", symbol);
            }

            // Fetch historical data
            logger.info("Fetching historical data for {}", symbol);
            List<HistoricalQuote> historicalData = yahooFinanceService.getHistoricalData(symbol);
            if (historicalData != null && !historicalData.isEmpty()) {
                clickHouseService.saveHistoricalData(symbol, historicalData);
                logger.info("Successfully fetched and saved {} historical records for {}", historicalData.size(), symbol);
            } else {
                logger.warn("Could not retrieve historical data for symbol: {}", symbol);
            }
            
            if (stockQuote == null && (historicalData == null || historicalData.isEmpty())) {
                 return ResponseEntity.status(HttpStatus.NOT_FOUND).body("No data found for symbol: " + symbol);
            }

            return ResponseEntity.ok("Data ingestion process completed for symbol: " + symbol);

        } catch (YahooFinanceException e) {
            logger.error("YahooFinanceException for symbol {}: {}", symbol, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body("Error fetching data from Yahoo Finance: " + e.getMessage());
        } catch (Exception e) {
            logger.error("An unexpected error occurred while processing symbol {}: {}", symbol, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("An unexpected error occurred: " + e.getMessage());
        }
    }
}

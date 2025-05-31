package com.backtestingbuddy.ingestionengine.service;

import com.backtestingbuddy.ingestionengine.exception.YahooFinanceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import yahoofinance.Stock;
import yahoofinance.YahooFinance;
import yahoofinance.histquotes.HistoricalQuote;
import yahoofinance.histquotes.Interval;

import java.io.IOException;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

@Service
public class YahooFinanceService {

    private static final Logger logger = LoggerFactory.getLogger(YahooFinanceService.class);

    @Value("${yahoo.finance.retry.max-attempts:3}") // Default to 3 retries
    private int maxRetries;

    @Value("${yahoo.finance.retry.delay-ms:2000}") // Default to 2000ms (2 seconds)
    private long retryDelayMs;

    public Stock getStockQuote(String securityName) throws YahooFinanceException {
        logger.debug("Fetching stock quote for security: {}", securityName);
        for (int i = 0; i < maxRetries; i++) {
            try {
                Stock stock = YahooFinance.get(securityName);
                logger.debug("Received stock quote response for {}: {}", securityName, stock);
                return stock;
            } catch (IOException e) {
                if (e.getMessage() != null && e.getMessage().contains("Server returned HTTP response code: 429")) {
                    logger.warn("Rate limit exceeded for {} (attempt {}/{}), retrying after {}ms", securityName, i + 1, maxRetries, retryDelayMs);
                    try {
                        Thread.sleep(retryDelayMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new YahooFinanceException("Retry delay interrupted", ie);
                    }
                } else {
                    logger.error("IO Error fetching stock quote for {} after {} attempts", securityName, i + 1, e);
                    throw new YahooFinanceException("Error fetching stock quote for " + securityName, e);
                }
            }
        }
        // If all retries fail
        throw new YahooFinanceException("Failed to fetch stock quote for " + securityName + " after " + maxRetries + " retries due to rate limit.");
    }

    public List<HistoricalQuote> getHistoricalData(String securityName) throws YahooFinanceException {
        logger.debug("Fetching historical data for security: {}", securityName);
         for (int i = 0; i < maxRetries; i++) {
            try {
                Stock stock = YahooFinance.get(securityName);
                 if (stock == null) {
                    logger.warn("Stock not found for historical data fetch: {}", securityName);
                    throw new YahooFinanceException("Stock not found: " + securityName);
                }
                Calendar from = Calendar.getInstance();
                Calendar to = Calendar.getInstance();
                from.add(Calendar.YEAR, -5); // 5 years ago

                logger.debug("Fetching historical data from {} to {} for {}", from.getTime(), to.getTime(), securityName);
                List<HistoricalQuote> historicalQuotes = stock.getHistory(from, to, Interval.DAILY);
                logger.debug("Received {} historical data points for {}", historicalQuotes != null ? historicalQuotes.size() : 0, securityName);
                return historicalQuotes;
            } catch (IOException e) {
                 if (e.getMessage() != null && e.getMessage().contains("Server returned HTTP response code: 429")) {
                     logger.warn("Rate limit exceeded for historical data of {} (attempt {}/{}), retrying after {}ms", securityName, i + 1, maxRetries, retryDelayMs);
                    try {
                        Thread.sleep(retryDelayMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                         throw new YahooFinanceException("Retry delay interrupted during historical data fetch", ie);
                    }
                } else {
                    logger.error("IO Error fetching historical data for {} after {} attempts", securityName, i + 1, e);
                    throw new YahooFinanceException("Error fetching historical data for " + securityName, e);
                }
            }
        }
         // If all retries fail
        throw new YahooFinanceException("Failed to fetch historical data for " + securityName + " after " + maxRetries + " retries due to rate limit.");
    }
    
    public Map<String, Stock> getMultipleStockQuotes(String[] securityNames) throws YahooFinanceException {
        logger.debug("Fetching multiple stock quotes for securities: {}", (Object[]) securityNames);
         for (int i = 0; i < maxRetries; i++) {
            try {
                Map<String, Stock> stocks = YahooFinance.get(securityNames);
                logger.debug("Received multiple stock quotes response: {}", stocks);
                return stocks;
            } catch (IOException e) {
                 if (e.getMessage() != null && e.getMessage().contains("Server returned HTTP response code: 429")) {
                     logger.warn("Rate limit exceeded for multiple stock quotes (attempt {}/{}), retrying after {}ms", i + 1, maxRetries, retryDelayMs);
                     try {
                        Thread.sleep(retryDelayMs);
                    } catch (InterruptedException ie) {
                         Thread.currentThread().interrupt();
                         throw new YahooFinanceException("Retry delay interrupted during multiple quotes fetch", ie);
                    }
                } else {
                    logger.error("IO Error fetching multiple stock quotes after {} attempts", i + 1, e);
                    throw new YahooFinanceException("Error fetching multiple stock quotes", e);
                }
            }
        }
         // If all retries fail
        throw new YahooFinanceException("Failed to fetch multiple stock quotes after " + maxRetries + " retries due to rate limit.");
    }
}

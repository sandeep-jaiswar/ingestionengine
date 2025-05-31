package com.backtestingbuddy.ingestionengine.service;

import com.backtestingbuddy.ingestionengine.exception.YahooFinanceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    public Stock getStockQuote(String securityName) throws YahooFinanceException {
        logger.debug("Fetching stock quote for security: {}", securityName);
        try {
            Stock stock = YahooFinance.get(securityName);
            logger.debug("Received stock quote response for {}: {}", securityName, stock);
            return stock;
        } catch (IOException e) {
            logger.error("IO Error fetching stock quote for {}", securityName, e);
            throw new YahooFinanceException("Error fetching stock quote for " + securityName, e);
        }
    }

    public List<HistoricalQuote> getHistoricalData(String securityName) throws YahooFinanceException {
        logger.debug("Fetching historical data for security: {}", securityName);
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
            logger.error("IO Error fetching historical data for {}", securityName, e);
            throw new YahooFinanceException("Error fetching historical data for " + securityName, e);
        }
    }
    
    public Map<String, Stock> getMultipleStockQuotes(String[] securityNames) throws YahooFinanceException {
        logger.debug("Fetching multiple stock quotes for securities: {}", (Object[]) securityNames);
        try {
            Map<String, Stock> stocks = YahooFinance.get(securityNames);
            logger.debug("Received multiple stock quotes response: {}", stocks);
            return stocks;
        } catch (IOException e) {
            logger.error("IO Error fetching multiple stock quotes", e);
            throw new YahooFinanceException("Error fetching multiple stock quotes", e);
        }
    }
}

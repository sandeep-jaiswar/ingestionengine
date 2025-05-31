package com.backtestingbuddy.ingestionengine.service;

import com.backtestingbuddy.ingestionengine.exception.YahooFinanceException;
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

    public Stock getStockQuote(String securityName) throws YahooFinanceException {
        try {
            return YahooFinance.get(securityName);
        } catch (IOException e) {
            throw new YahooFinanceException("Error fetching stock quote for " + securityName, e);
        }
    }

    public List<HistoricalQuote> getHistoricalData(String securityName) throws YahooFinanceException {
        try {
            Stock stock = YahooFinance.get(securityName);
            if (stock == null) {
                throw new YahooFinanceException("Stock not found: " + securityName);
            }
            Calendar from = Calendar.getInstance();
            Calendar to = Calendar.getInstance();
            from.add(Calendar.YEAR, -5); // 5 years ago

            return stock.getHistory(from, to, Interval.DAILY);
        } catch (IOException e) {
            throw new YahooFinanceException("Error fetching historical data for " + securityName, e);
        }
    }
    
    public Map<String, Stock> getMultipleStockQuotes(String[] securityNames) throws YahooFinanceException {
        try {
            return YahooFinance.get(securityNames);
        } catch (IOException e) {
            throw new YahooFinanceException("Error fetching multiple stock quotes", e);
        }
    }
}

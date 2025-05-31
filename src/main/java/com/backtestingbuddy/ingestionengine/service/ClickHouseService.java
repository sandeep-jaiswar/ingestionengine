package com.backtestingbuddy.ingestionengine.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import yahoofinance.Stock;
import yahoofinance.histquotes.HistoricalQuote;

import jakarta.annotation.PostConstruct; // Changed import
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;
import java.util.ArrayList;

@Service
public class ClickHouseService {

    private static final Logger logger = LoggerFactory.getLogger(ClickHouseService.class);

    @Autowired
    private JdbcTemplate clickHouseJdbcTemplate;

    @PostConstruct
    public void initializeDatabase() {
        try {
            // Create stock_quotes table if it doesn't exist
            clickHouseJdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS stock_quotes (
                symbol String,
                name String,
                currency String,
                stock_exchange String,
                quote_price Decimal(20, 8),
                ask Decimal(20, 8),
                bid Decimal(20, 8),
                day_low Decimal(20, 8),
                day_high Decimal(20, 8),
                year_low Decimal(20, 8),
                year_high Decimal(20, 8),
                volume Int64,
                market_cap Int64,
                last_trade_time DateTime,
                fetch_time DateTime DEFAULT now()
            ) ENGINE = MergeTree() ORDER BY (symbol, last_trade_time)
            """);
            logger.info("Table 'stock_quotes' checked/created successfully.");

            // Create stock_historical_data table if it doesn't exist
            clickHouseJdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS stock_historical_data (
                symbol String,
                date DateTime,
                open Decimal(20, 8),
                high Decimal(20, 8),
                low Decimal(20, 8),
                close Decimal(20, 8),
                adj_close Decimal(20, 8),
                volume Int64,
                fetch_time DateTime DEFAULT now()
            ) ENGINE = MergeTree() ORDER BY (symbol, date)
            """);
            logger.info("Table 'stock_historical_data' checked/created successfully.");

        } catch (Exception e) {
            logger.error("Error initializing ClickHouse tables: {}", e.getMessage(), e);
            // Depending on your application's needs, you might want to re-throw or handle this more gracefully
        }
    }

    public void saveStockQuote(Stock stock) {
        if (stock == null || stock.getQuote() == null) {
            logger.warn("Stock or stock quote is null, skipping save for symbol: {}", stock != null ? stock.getSymbol() : "Unknown");
            return;
        }

        String sql = "INSERT INTO stock_quotes (symbol, name, currency, stock_exchange, quote_price, ask, bid, day_low, day_high, year_low, year_high, volume, market_cap, last_trade_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        try {
            clickHouseJdbcTemplate.update(sql,
                stock.getSymbol(),
                stock.getName(),
                stock.getCurrency(),
                stock.getStockExchange(),
                stock.getQuote().getPrice(),
                stock.getQuote().getAsk(),
                stock.getQuote().getBid(),
                stock.getQuote().getDayLow(),
                stock.getQuote().getDayHigh(),
                stock.getQuote().getYearLow(),
                stock.getQuote().getYearHigh(),
                stock.getQuote().getVolume(),
                stock.getStats().getMarketCap() != null ? stock.getStats().getMarketCap().longValue() : null,
                stock.getQuote().getLastTradeTime() != null ? new Timestamp(stock.getQuote().getLastTradeTime().getTimeInMillis()) : null
            );
            logger.info("Saved stock quote for: {}", stock.getSymbol());
        } catch (Exception e) {
            logger.error("Error saving stock quote for {}: {}", stock.getSymbol(), e.getMessage(), e);
        }
    }

    public void saveHistoricalData(String symbol, List<HistoricalQuote> historicalQuotes) {
        if (historicalQuotes == null || historicalQuotes.isEmpty()) {
            logger.warn("Historical data is null or empty for symbol: {}, skipping save.", symbol);
            return;
        }

        String sql = "INSERT INTO stock_historical_data (symbol, date, open, high, low, close, adj_close, volume) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

        List<Object[]> batchArgs = new ArrayList<>();
        for (HistoricalQuote hq : historicalQuotes) {
            if (hq.getDate() == null) continue; // Skip if date is null
            Object[] params = new Object[]{
                symbol,
                new Timestamp(hq.getDate().getTimeInMillis()),
                hq.getOpen(),
                hq.getHigh(),
                hq.getLow(),
                hq.getClose(),
                hq.getAdjClose(),
                hq.getVolume()
            };
            batchArgs.add(params);
        }
        
        try {
            if (!batchArgs.isEmpty()) {
                clickHouseJdbcTemplate.batchUpdate(sql, batchArgs);
                logger.info("Saved {} historical data points for: {}", batchArgs.size(), symbol);
            }
        } catch (Exception e) {
            logger.error("Error saving historical data for {}: {}", symbol, e.getMessage(), e);
        }
    }
}

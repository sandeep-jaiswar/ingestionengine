package com.backtestingbuddy.ingestionengine.config;

import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import com.clickhouse.jdbc.ClickHouseDataSource;

import java.sql.SQLException;
import java.util.Properties;

@Configuration
public class ClickHouseConfig {

    @Value("${clickhouse.datasource.url}")
    private String clickhouseUrl;

    @Value("${clickhouse.datasource.username:default}") // Default to 'default' if not set
    private String clickhouseUsername;

    @Value("${clickhouse.datasource.password:}") // Default to empty if not set
    private String clickhousePassword;

    @Bean
    public DataSource clickHouseDataSource() throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", clickhouseUsername);
        if (clickhousePassword != null && !clickhousePassword.isEmpty()) {
            props.setProperty("password", clickhousePassword);
        }
        // It is generally recommended to set ssl to true for production
        // props.setProperty("ssl", "true"); 
        // props.setProperty("sslmode", "strict"); // or "none" if not using SSL

        return new ClickHouseDataSource(clickhouseUrl, props);
    }

    @Bean
    public JdbcTemplate clickHouseJdbcTemplate(DataSource clickHouseDataSource) {
        return new JdbcTemplate(clickHouseDataSource);
    }
}

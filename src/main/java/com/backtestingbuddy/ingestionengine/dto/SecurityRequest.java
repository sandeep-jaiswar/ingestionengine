package com.backtestingbuddy.ingestionengine.dto;

public class SecurityRequest {
    private String securityName;

    // Default constructor
    public SecurityRequest() {
    }

    // Constructor with securityName
    public SecurityRequest(String securityName) {
        this.securityName = securityName;
    }

    // Getter
    public String getSecurityName() {
        return securityName;
    }

    // Setter
    public void setSecurityName(String securityName) {
        this.securityName = securityName;
    }
}

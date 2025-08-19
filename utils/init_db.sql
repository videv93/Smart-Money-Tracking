-- Initialize PostgreSQL database for Smart Money Tracking

-- Create schemas
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS production;

-- Create staging tables
CREATE TABLE IF NOT EXISTS staging.blockchain_transactions (
    hash VARCHAR(66) PRIMARY KEY,
    block_number BIGINT NOT NULL,
    block_timestamp TIMESTAMP NOT NULL,
    from_address VARCHAR(42) NOT NULL,
    to_address VARCHAR(42),
    value DECIMAL(36,18) NOT NULL,
    gas_price BIGINT NOT NULL,
    gas_used BIGINT NOT NULL,
    gas_limit BIGINT NOT NULL,
    status INTEGER NOT NULL,
    input_data TEXT,
    contract_address VARCHAR(42),
    token_transfers JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.wallet_features (
    from_address VARCHAR(42) PRIMARY KEY,
    total_volume DECIMAL(36,18),
    avg_transaction_value DECIMAL(36,18),
    transaction_count INTEGER,
    success_rate DECIMAL(5,4),
    avg_gas_price BIGINT,
    unique_recipients INTEGER,
    unique_contracts INTEGER,
    transactions_per_day DECIMAL(10,4),
    recipient_diversity DECIMAL(5,4),
    contract_interaction_rate DECIMAL(5,4),
    mev_score DECIMAL(5,4),
    high_gas_transactions INTEGER,
    potential_sandwich_attacks INTEGER,
    cluster INTEGER,
    smart_money_category VARCHAR(20),
    smart_money_score DECIMAL(5,4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.smart_money_wallets (
    from_address VARCHAR(42) PRIMARY KEY,
    smart_money_score DECIMAL(5,4),
    total_volume DECIMAL(36,18),
    transaction_count INTEGER,
    success_rate DECIMAL(5,4),
    avg_gas_price BIGINT,
    unique_recipients INTEGER,
    unique_contracts INTEGER,
    smart_money_category VARCHAR(20),
    rank INTEGER,
    first_detected TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create production tables
CREATE TABLE IF NOT EXISTS production.smart_money_transactions (
    id SERIAL PRIMARY KEY,
    transaction_hash VARCHAR(66) NOT NULL,
    block_number BIGINT NOT NULL,
    block_timestamp TIMESTAMP NOT NULL,
    from_address VARCHAR(42) NOT NULL,
    to_address VARCHAR(42),
    value DECIMAL(36,18) NOT NULL,
    gas_price BIGINT NOT NULL,
    smart_money_score DECIMAL(5,4) NOT NULL,
    alert_severity VARCHAR(10),
    characteristics JSONB,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS production.smart_money_wallets (
    wallet_address VARCHAR(42) PRIMARY KEY,
    smart_money_score DECIMAL(5,4) NOT NULL,
    wallet_category VARCHAR(20),
    total_volume DECIMAL(36,18),
    transaction_count INTEGER,
    success_rate DECIMAL(5,4),
    mev_activity_score DECIMAL(5,4),
    defi_interaction_score DECIMAL(5,4),
    risk_score DECIMAL(5,4),
    confidence_level VARCHAR(10),
    first_detected TIMESTAMP NOT NULL,
    last_activity TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS production.wallet_performance_metrics (
    id SERIAL PRIMARY KEY,
    wallet_address VARCHAR(42) NOT NULL,
    date_recorded DATE NOT NULL,
    daily_volume DECIMAL(36,18),
    daily_transactions INTEGER,
    daily_success_rate DECIMAL(5,4),
    daily_gas_spent DECIMAL(36,18),
    daily_mev_opportunities INTEGER,
    pnl_estimate DECIMAL(36,18),
    UNIQUE(wallet_address, date_recorded)
);

CREATE TABLE IF NOT EXISTS production.alert_history (
    id SERIAL PRIMARY KEY,
    alert_type VARCHAR(50) NOT NULL,
    severity VARCHAR(10) NOT NULL,
    wallet_address VARCHAR(42),
    transaction_hash VARCHAR(66),
    alert_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    acknowledged BOOLEAN DEFAULT FALSE,
    acknowledged_at TIMESTAMP,
    acknowledged_by VARCHAR(100)
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_transactions_from_address ON staging.blockchain_transactions(from_address);
CREATE INDEX IF NOT EXISTS idx_transactions_to_address ON staging.blockchain_transactions(to_address);
CREATE INDEX IF NOT EXISTS idx_transactions_block_timestamp ON staging.blockchain_transactions(block_timestamp);
CREATE INDEX IF NOT EXISTS idx_transactions_value ON staging.blockchain_transactions(value);

CREATE INDEX IF NOT EXISTS idx_wallet_features_score ON staging.wallet_features(smart_money_score);
CREATE INDEX IF NOT EXISTS idx_wallet_features_category ON staging.wallet_features(smart_money_category);
CREATE INDEX IF NOT EXISTS idx_wallet_features_volume ON staging.wallet_features(total_volume);

CREATE INDEX IF NOT EXISTS idx_smart_wallets_score ON production.smart_money_wallets(smart_money_score);
CREATE INDEX IF NOT EXISTS idx_smart_wallets_category ON production.smart_money_wallets(wallet_category);
CREATE INDEX IF NOT EXISTS idx_smart_wallets_last_activity ON production.smart_money_wallets(last_activity);

CREATE INDEX IF NOT EXISTS idx_smart_transactions_timestamp ON production.smart_money_transactions(block_timestamp);
CREATE INDEX IF NOT EXISTS idx_smart_transactions_from ON production.smart_money_transactions(from_address);
CREATE INDEX IF NOT EXISTS idx_smart_transactions_severity ON production.smart_money_transactions(alert_severity);

CREATE INDEX IF NOT EXISTS idx_performance_wallet_date ON production.wallet_performance_metrics(wallet_address, date_recorded);
CREATE INDEX IF NOT EXISTS idx_alerts_type_created ON production.alert_history(alert_type, created_at);
CREATE INDEX IF NOT EXISTS idx_alerts_wallet ON production.alert_history(wallet_address);

-- Create views for common queries
CREATE OR REPLACE VIEW production.v_top_smart_wallets AS
SELECT 
    wallet_address,
    smart_money_score,
    wallet_category,
    total_volume,
    transaction_count,
    success_rate,
    last_activity,
    CASE 
        WHEN smart_money_score >= 0.9 THEN 'Very High'
        WHEN smart_money_score >= 0.8 THEN 'High'
        WHEN smart_money_score >= 0.7 THEN 'Medium'
        ELSE 'Low'
    END as confidence_level
FROM production.smart_money_wallets
WHERE smart_money_score >= 0.7
ORDER BY smart_money_score DESC;

CREATE OR REPLACE VIEW production.v_daily_smart_money_stats AS
SELECT 
    DATE(block_timestamp) as date,
    COUNT(*) as transaction_count,
    COUNT(DISTINCT from_address) as unique_wallets,
    SUM(value) as total_volume,
    AVG(smart_money_score) as avg_smart_score,
    COUNT(CASE WHEN alert_severity = 'CRITICAL' THEN 1 END) as critical_alerts,
    COUNT(CASE WHEN alert_severity = 'HIGH' THEN 1 END) as high_alerts
FROM production.smart_money_transactions
GROUP BY DATE(block_timestamp)
ORDER BY date DESC;

-- Create trigger to update last_updated timestamp
CREATE OR REPLACE FUNCTION update_last_updated_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.last_updated = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_wallet_features_last_updated
    BEFORE UPDATE ON staging.wallet_features
    FOR EACH ROW EXECUTE FUNCTION update_last_updated_column();

CREATE TRIGGER update_smart_wallets_last_updated
    BEFORE UPDATE ON production.smart_money_wallets
    FOR EACH ROW EXECUTE FUNCTION update_last_updated_column();

-- Insert some sample configuration data
INSERT INTO production.alert_history (alert_type, severity, alert_data, acknowledged) 
VALUES ('system_startup', 'INFO', '{"message": "Smart Money Tracking database initialized"}', true)
ON CONFLICT DO NOTHING;

-- Grant permissions
GRANT USAGE ON SCHEMA staging TO PUBLIC;
GRANT USAGE ON SCHEMA production TO PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA staging TO PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA production TO PUBLIC;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA production TO PUBLIC;
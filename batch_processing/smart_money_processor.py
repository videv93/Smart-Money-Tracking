"""
Spark-based batch processing for smart money transaction analysis
Processes blockchain data from MinIO Delta Lake and identifies smart money patterns
"""

import logging
from typing import Dict, List
from datetime import datetime, timedelta
import yaml
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from delta import configure_spark_with_delta_pip
import psycopg2
from psycopg2.extras import execute_values

class SmartMoneyProcessor:
    def __init__(self, config_path: str = "configs/config.yaml"):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.logger = logging.getLogger(__name__)
        self.spark = self._create_spark_session()
        
    def _create_spark_session(self) -> SparkSession:
        """Create Spark session with Delta Lake support"""
        builder = SparkSession.builder \
            .appName(self.config['spark']['app_name']) \
            .master(self.config['spark']['master']) \
            .config("spark.driver.memory", self.config['spark']['driver_memory']) \
            .config("spark.executor.memory", self.config['spark']['executor_memory']) \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.hadoop.fs.s3a.endpoint", f"http://{self.config['minio']['endpoint']}") \
            .config("spark.hadoop.fs.s3a.access.key", self.config['minio']['access_key']) \
            .config("spark.hadoop.fs.s3a.secret.key", self.config['minio']['secret_key']) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        
        for package in self.config['spark']['packages']:
            builder = builder.config("spark.jars.packages", package)
        
        return configure_spark_with_delta_pip(builder).getOrCreate()
    
    def read_blockchain_data(self, start_date: str = None, end_date: str = None) -> DataFrame:
        """Read blockchain transaction data from Delta Lake"""
        table_path = f"s3a://{self.config['minio']['bucket_name']}/delta/blockchain_transactions"
        
        try:
            df = self.spark.read.format("delta").load(table_path)
            
            if start_date:
                df = df.filter(col("block_timestamp") >= start_date)
            if end_date:
                df = df.filter(col("block_timestamp") <= end_date)
            
            return df
        except Exception as e:
            self.logger.error(f"Failed to read blockchain data: {e}")
            return self.spark.createDataFrame([], schema=self._get_transaction_schema())
    
    def _get_transaction_schema(self) -> StructType:
        """Define transaction data schema"""
        return StructType([
            StructField("hash", StringType(), False),
            StructField("block_number", LongType(), False),
            StructField("block_timestamp", TimestampType(), False),
            StructField("from_address", StringType(), False),
            StructField("to_address", StringType(), True),
            StructField("value", DoubleType(), False),
            StructField("gas_price", LongType(), False),
            StructField("gas_used", LongType(), False),
            StructField("gas_limit", LongType(), False),
            StructField("status", IntegerType(), False),
            StructField("input_data", StringType(), True),
            StructField("contract_address", StringType(), True),
            StructField("token_transfers", ArrayType(MapType(StringType(), StringType())), True)
        ])
    
    def calculate_wallet_features(self, df: DataFrame) -> DataFrame:
        """Calculate wallet-level features for smart money detection"""
        
        # Aggregate wallet statistics
        wallet_stats = df.groupBy("from_address").agg(
            count("hash").alias("transaction_count"),
            sum("value").alias("total_volume"),
            avg("value").alias("avg_transaction_value"),
            avg("gas_price").alias("avg_gas_price"),
            sum(when(col("status") == 1, 1).otherwise(0)).alias("successful_transactions"),
            countDistinct("to_address").alias("unique_recipients"),
            countDistinct("contract_address").alias("unique_contracts"),
            min("block_timestamp").alias("first_transaction"),
            max("block_timestamp").alias("last_transaction"),
            stddev("value").alias("value_volatility"),
            percentile_approx("gas_price", 0.9).alias("gas_price_90th")
        )
        
        # Calculate derived features
        wallet_features = wallet_stats.withColumn(
            "success_rate", col("successful_transactions") / col("transaction_count")
        ).withColumn(
            "activity_days", datediff(col("last_transaction"), col("first_transaction"))
        ).withColumn(
            "transactions_per_day", col("transaction_count") / greatest(col("activity_days"), lit(1))
        ).withColumn(
            "gas_efficiency", col("avg_gas_price") / col("gas_price_90th")
        ).withColumn(
            "recipient_diversity", col("unique_recipients") / col("transaction_count")
        ).withColumn(
            "contract_interaction_rate", 
            coalesce(col("unique_contracts") / col("transaction_count"), lit(0))
        )
        
        # Add MEV and arbitrage indicators
        wallet_features = self._add_mev_features(wallet_features, df)
        
        return wallet_features
    
    def _add_mev_features(self, wallet_df: DataFrame, tx_df: DataFrame) -> DataFrame:
        """Add MEV (Maximal Extractable Value) detection features"""
        
        # High gas transactions (potential MEV)
        high_gas_txs = tx_df.filter(col("gas_price") > 100000000000).groupBy("from_address").count().withColumnRenamed("count", "high_gas_transactions")
        
        # Sandwich attack patterns (quick buy-sell sequences)
        sandwich_patterns = tx_df.withColumn("hour_bucket", date_trunc("hour", col("block_timestamp"))) \
            .groupBy("from_address", "hour_bucket", "to_address") \
            .count() \
            .filter(col("count") > 5) \
            .groupBy("from_address") \
            .count() \
            .withColumnRenamed("count", "potential_sandwich_attacks")
        
        # Join MEV features
        wallet_df = wallet_df.join(high_gas_txs, "from_address", "left") \
                           .join(sandwich_patterns, "from_address", "left")
        
        # Fill nulls and calculate MEV score
        wallet_df = wallet_df.fillna(0, ["high_gas_transactions", "potential_sandwich_attacks"]) \
                           .withColumn("mev_score", 
                                     (col("high_gas_transactions") * 0.6 + 
                                      col("potential_sandwich_attacks") * 0.4) / 
                                     greatest(col("transaction_count"), lit(1)))
        
        return wallet_df
    
    def cluster_wallets(self, df: DataFrame) -> DataFrame:
        """Cluster wallets based on behavior patterns using K-means"""
        
        # Select features for clustering
        feature_cols = [
            "total_volume", "avg_transaction_value", "transaction_count",
            "success_rate", "avg_gas_price", "unique_recipients", 
            "unique_contracts", "transactions_per_day", "recipient_diversity",
            "contract_interaction_rate", "mev_score"
        ]
        
        # Assemble features
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        df_features = assembler.transform(df.fillna(0))
        
        # Scale features
        scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")
        scaler_model = scaler.fit(df_features)
        df_scaled = scaler_model.transform(df_features)
        
        # Apply K-means clustering
        kmeans = KMeans(k=5, seed=42, featuresCol="scaledFeatures", predictionCol="cluster")
        model = kmeans.fit(df_scaled)
        df_clustered = model.transform(df_scaled)
        
        # Map clusters to smart money categories
        df_categorized = df_clustered.withColumn(
            "smart_money_category",
            when(col("cluster") == 0, "whale")
            .when(col("cluster") == 1, "mev_bot")
            .when(col("cluster") == 2, "active_trader") 
            .when(col("cluster") == 3, "institutional")
            .otherwise("retail")
        )
        
        return df_categorized.select(
            "from_address", *feature_cols, "cluster", "smart_money_category"
        )
    
    def calculate_smart_money_score(self, df: DataFrame) -> DataFrame:
        """Calculate composite smart money confidence score"""
        
        # Define scoring weights
        weights = {
            "volume_score": 0.25,
            "success_score": 0.20,
            "activity_score": 0.15,
            "gas_efficiency_score": 0.15,
            "diversification_score": 0.10,
            "mev_score": 0.15
        }
        
        # Calculate component scores (0-1 scale)
        df_scored = df.withColumn(
            "volume_score", 
            least(col("total_volume") / lit(self.config['smart_money']['min_transaction_volume']), lit(1.0))
        ).withColumn(
            "success_score", col("success_rate")
        ).withColumn(
            "activity_score", 
            least(col("transactions_per_day") / lit(10.0), lit(1.0))
        ).withColumn(
            "gas_efficiency_score", 
            greatest(lit(0.0), least(lit(2.0) - col("gas_efficiency"), lit(1.0)))
        ).withColumn(
            "diversification_score", 
            least((col("unique_recipients") + col("unique_contracts")) / lit(50.0), lit(1.0))
        ).withColumn(
            "mev_score_normalized", least(col("mev_score") * lit(5.0), lit(1.0))
        )
        
        # Calculate weighted smart money score
        smart_money_expr = (
            col("volume_score") * weights["volume_score"] +
            col("success_score") * weights["success_score"] +
            col("activity_score") * weights["activity_score"] +
            col("gas_efficiency_score") * weights["gas_efficiency_score"] +
            col("diversification_score") * weights["diversification_score"] +
            col("mev_score_normalized") * weights["mev_score"]
        )
        
        return df_scored.withColumn("smart_money_score", smart_money_expr)
    
    def identify_smart_money_wallets(self, df: DataFrame, threshold: float = None) -> DataFrame:
        """Identify wallets that meet smart money criteria"""
        
        if threshold is None:
            threshold = self.config['ml']['threshold']
        
        # Apply threshold and additional filters
        smart_wallets = df.filter(
            (col("smart_money_score") >= threshold) &
            (col("total_volume") >= self.config['smart_money']['min_transaction_volume']) &
            (col("success_rate") >= self.config['smart_money']['min_success_rate']) &
            (col("transaction_count") >= 10)  # Minimum activity
        )
        
        # Rank by smart money score
        smart_wallets = smart_wallets.withColumn(
            "rank", row_number().over(Window.orderBy(desc("smart_money_score")))
        )
        
        return smart_wallets
    
    def save_to_staging(self, df: DataFrame, table_name: str):
        """Save processed data to PostgreSQL staging schema"""
        
        connection_props = {
            "user": self.config['postgresql']['username'],
            "password": self.config['postgresql']['password'],
            "driver": "org.postgresql.Driver"
        }
        
        jdbc_url = f"jdbc:postgresql://{self.config['postgresql']['host']}:{self.config['postgresql']['port']}/{self.config['postgresql']['database']}"
        
        # Write to staging schema
        df.write \
          .format("jdbc") \
          .option("url", jdbc_url) \
          .option("dbtable", f"{self.config['postgresql']['staging_schema']}.{table_name}") \
          .options(**connection_props) \
          .mode("overwrite") \
          .save()
    
    def process_smart_money_batch(self, start_date: str = None, end_date: str = None):
        """Main batch processing pipeline"""
        
        self.logger.info("Starting smart money batch processing...")
        
        # Read blockchain data
        tx_df = self.read_blockchain_data(start_date, end_date)
        
        if tx_df.count() == 0:
            self.logger.warning("No transaction data found")
            return
        
        # Calculate wallet features
        wallet_features = self.calculate_wallet_features(tx_df)
        
        # Cluster wallets
        wallet_clusters = self.cluster_wallets(wallet_features)
        
        # Calculate smart money scores
        wallet_scored = self.calculate_smart_money_score(wallet_clusters)
        
        # Identify smart money wallets
        smart_wallets = self.identify_smart_money_wallets(wallet_scored)
        
        # Save results
        self.save_to_staging(wallet_scored, "wallet_features")
        self.save_to_staging(smart_wallets, "smart_money_wallets")
        
        # Log results
        total_wallets = wallet_scored.count()
        smart_count = smart_wallets.count()
        
        self.logger.info(f"Processed {total_wallets} wallets, identified {smart_count} smart money wallets")
        
        return {
            "total_wallets": total_wallets,
            "smart_money_wallets": smart_count,
            "smart_money_rate": smart_count / total_wallets if total_wallets > 0 else 0
        }

def main():
    """Run batch processing"""
    processor = SmartMoneyProcessor()
    
    # Process last 7 days
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)
    
    results = processor.process_smart_money_batch(
        start_date=start_date.strftime("%Y-%m-%d"),
        end_date=end_date.strftime("%Y-%m-%d")
    )
    
    print(f"Batch processing completed: {results}")

if __name__ == "__main__":
    main()

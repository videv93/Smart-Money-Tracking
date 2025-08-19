"""
Flink-based stream processing for real-time smart money detection
Processes blockchain transaction streams and generates alerts
"""

import json
import logging
from typing import Dict, List
from datetime import datetime
import yaml
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction, FilterFunction, ProcessFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.time import Time

class TransactionProcessor(MapFunction):
    """Process incoming blockchain transactions"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.smart_money_threshold = config['ml']['threshold']
        
    def map(self, value: str) -> Dict:
        """Parse and enrich transaction data"""
        try:
            tx = json.loads(value)
            
            # Add smart money indicators
            tx['is_large_transaction'] = tx.get('value', 0) > 10.0  # > 10 ETH
            tx['is_high_gas'] = tx.get('gas_price', 0) > 100000000000  # > 100 Gwei
            tx['has_contract_interaction'] = bool(tx.get('input_data', '0x'))
            tx['has_token_transfers'] = bool(tx.get('token_transfers', []))
            
            # Calculate preliminary smart money score
            score = 0.0
            if tx['is_large_transaction']:
                score += 0.3
            if tx['is_high_gas']:
                score += 0.2
            if tx['has_contract_interaction']:
                score += 0.3
            if tx['has_token_transfers']:
                score += 0.2
            
            tx['smart_money_score'] = min(score, 1.0)
            tx['timestamp'] = datetime.now().isoformat()
            
            return tx
            
        except Exception as e:
            logging.error(f"Failed to process transaction: {e}")
            return {}

class SmartMoneyFilter(FilterFunction):
    """Filter transactions that show smart money patterns"""
    
    def __init__(self, threshold: float = 0.5):
        self.threshold = threshold
        
    def filter(self, value: Dict) -> bool:
        """Return True if transaction shows smart money characteristics"""
        if not value:
            return False
            
        return value.get('smart_money_score', 0) >= self.threshold

class WalletStateProcessor(ProcessFunction):
    """Stateful processor to track wallet behavior over time"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.wallet_state = None
        
    def open(self, runtime_context):
        # Initialize wallet state
        wallet_descriptor = ValueStateDescriptor(
            "wallet_metrics",
            Types.STRING()
        )
        self.wallet_state = runtime_context.get_state(wallet_descriptor)
        
    def process_element(self, value: Dict, ctx, out):
        """Process transaction and update wallet state"""
        try:
            from_address = value.get('from_address', '')
            if not from_address:
                return
            
            # Get current wallet state
            current_state = self.wallet_state.value()
            if current_state:
                wallet_metrics = json.loads(current_state)
            else:
                wallet_metrics = {
                    'address': from_address,
                    'transaction_count': 0,
                    'total_volume': 0.0,
                    'successful_transactions': 0,
                    'total_gas_used': 0,
                    'unique_contracts': set(),
                    'first_seen': value.get('timestamp'),
                    'last_seen': value.get('timestamp'),
                    'high_gas_transactions': 0,
                    'large_transactions': 0
                }
            
            # Update metrics
            wallet_metrics['transaction_count'] += 1
            wallet_metrics['total_volume'] += value.get('value', 0)
            wallet_metrics['total_gas_used'] += value.get('gas_used', 0)
            wallet_metrics['last_seen'] = value.get('timestamp')
            
            if value.get('status') == 1:
                wallet_metrics['successful_transactions'] += 1
                
            if value.get('to_address'):
                wallet_metrics['unique_contracts'].add(value['to_address'])
                
            if value.get('is_high_gas'):
                wallet_metrics['high_gas_transactions'] += 1
                
            if value.get('is_large_transaction'):
                wallet_metrics['large_transactions'] += 1
            
            # Calculate updated smart money score
            success_rate = wallet_metrics['successful_transactions'] / wallet_metrics['transaction_count']
            avg_volume = wallet_metrics['total_volume'] / wallet_metrics['transaction_count']
            contract_diversity = len(wallet_metrics['unique_contracts'])
            
            smart_score = (
                min(wallet_metrics['total_volume'] / 1000, 1.0) * 0.3 +  # Volume score
                success_rate * 0.25 +  # Success rate score
                min(wallet_metrics['transaction_count'] / 100, 1.0) * 0.2 +  # Activity score
                min(contract_diversity / 50, 1.0) * 0.15 +  # Diversity score
                min(wallet_metrics['high_gas_transactions'] / wallet_metrics['transaction_count'], 1.0) * 0.1  # MEV score
            )
            
            wallet_metrics['smart_money_score'] = smart_score
            
            # Convert set to list for JSON serialization
            wallet_metrics['unique_contracts'] = list(wallet_metrics['unique_contracts'])
            
            # Save updated state
            self.wallet_state.update(json.dumps(wallet_metrics))
            
            # Emit wallet update if score is high enough
            if smart_score > self.config['ml']['threshold']:
                alert = {
                    'type': 'smart_money_wallet_update',
                    'wallet_address': from_address,
                    'smart_money_score': smart_score,
                    'transaction_count': wallet_metrics['transaction_count'],
                    'total_volume': wallet_metrics['total_volume'],
                    'success_rate': success_rate,
                    'timestamp': datetime.now().isoformat(),
                    'trigger_transaction': value.get('hash')
                }
                
                out.collect(json.dumps(alert))
                
        except Exception as e:
            logging.error(f"Failed to process wallet state: {e}")

class AlertGenerator(MapFunction):
    """Generate smart money alerts"""
    
    def __init__(self, config: Dict):
        self.config = config
        
    def map(self, value: Dict) -> str:
        """Generate alert for smart money transaction"""
        alert = {
            'alert_type': 'smart_money_transaction',
            'transaction_hash': value.get('hash'),
            'from_address': value.get('from_address'),
            'to_address': value.get('to_address'),
            'value': value.get('value'),
            'gas_price': value.get('gas_price'),
            'smart_money_score': value.get('smart_money_score'),
            'block_number': value.get('block_number'),
            'timestamp': value.get('timestamp'),
            'characteristics': {
                'is_large_transaction': value.get('is_large_transaction'),
                'is_high_gas': value.get('is_high_gas'),
                'has_contract_interaction': value.get('has_contract_interaction'),
                'has_token_transfers': value.get('has_token_transfers')
            }
        }
        
        return json.dumps(alert)

class SmartMoneyStreamProcessor:
    def __init__(self, config_path: str = "configs/config.yaml"):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.logger = logging.getLogger(__name__)
        
        # Create Flink environment
        self.env = StreamExecutionEnvironment.get_execution_environment()
        self.env.set_parallelism(4)
        self.env.enable_checkpointing(self.config['flink']['checkpoint_interval'])
        
    def create_kafka_consumer(self) -> FlinkKafkaConsumer:
        """Create Kafka consumer for blockchain transactions"""
        return FlinkKafkaConsumer(
            topics=self.config['kafka']['topics']['transactions'],
            deserialization_schema=SimpleStringSchema(),
            properties={
                'bootstrap.servers': self.config['kafka']['bootstrap_servers'],
                'group.id': 'smart-money-stream-processor'
            }
        )
    
    def create_kafka_producer(self, topic: str) -> FlinkKafkaProducer:
        """Create Kafka producer for alerts"""
        return FlinkKafkaProducer(
            topic=topic,
            serialization_schema=SimpleStringSchema(),
            producer_config={
                'bootstrap.servers': self.config['kafka']['bootstrap_servers']
            }
        )
    
    def run_stream_processing(self):
        """Main stream processing pipeline"""
        
        self.logger.info("Starting smart money stream processing...")
        
        # Create Kafka consumer
        kafka_consumer = self.create_kafka_consumer()
        
        # Create transaction stream
        transaction_stream = self.env.add_source(kafka_consumer)
        
        # Process transactions
        processed_stream = transaction_stream.map(
            TransactionProcessor(self.config),
            output_type=Types.PICKLED_BYTE_ARRAY()
        )
        
        # Filter smart money transactions
        smart_money_stream = processed_stream.filter(
            SmartMoneyFilter(self.config['ml']['threshold'])
        )
        
        # Generate immediate alerts for high-value transactions
        immediate_alerts = smart_money_stream.map(
            AlertGenerator(self.config),
            output_type=Types.STRING()
        )
        
        # Process wallet state updates
        wallet_updates = processed_stream.key_by(lambda x: x.get('from_address', 'unknown')) \
                                       .process(WalletStateProcessor(self.config))
        
        # Create Kafka producers for outputs
        alert_producer = self.create_kafka_producer(
            self.config['kafka']['topics']['smart_money_alerts']
        )
        wallet_producer = self.create_kafka_producer(
            self.config['kafka']['topics']['wallet_updates']
        )
        
        # Send alerts to Kafka
        immediate_alerts.add_sink(alert_producer)
        wallet_updates.add_sink(wallet_producer)
        
        # Print alerts to console for debugging
        immediate_alerts.print("Smart Money Alert")
        wallet_updates.print("Wallet Update")
        
        # Execute the pipeline
        self.env.execute("Smart Money Stream Processing")

def main():
    """Run stream processing"""
    processor = SmartMoneyStreamProcessor()
    processor.run_stream_processing()

if __name__ == "__main__":
    main()
"""
Smart Money Alert System
Monitors Kafka streams and sends notifications for smart money activities
"""

import json
import logging
import asyncio
from typing import Dict, List
from datetime import datetime, timedelta
import yaml
import requests
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from kafka import KafkaConsumer
from prometheus_client import Counter, Histogram, Gauge, start_http_server
import discord
from discord.ext import commands

# Prometheus metrics
ALERT_COUNTER = Counter('smart_money_alerts_total', 'Total smart money alerts', ['alert_type', 'severity'])
PROCESSING_TIME = Histogram('alert_processing_seconds', 'Time spent processing alerts')
WALLET_SCORE_GAUGE = Gauge('wallet_smart_money_score', 'Current smart money score', ['wallet_address'])
TRANSACTION_VALUE_GAUGE = Gauge('large_transaction_value_eth', 'Large transaction value in ETH')

class AlertManager:
    def __init__(self, config_path: str = "configs/config.yaml"):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.logger = logging.getLogger(__name__)
        self.alert_history = {}
        self.rate_limits = {}
        
    def should_send_alert(self, alert_type: str, wallet_address: str = None) -> bool:
        """Check if alert should be sent based on rate limiting"""
        now = datetime.now()
        key = f"{alert_type}_{wallet_address}" if wallet_address else alert_type
        
        # Rate limiting: max 1 alert per wallet per hour for same type
        if key in self.rate_limits:
            if now - self.rate_limits[key] < timedelta(hours=1):
                return False
        
        self.rate_limits[key] = now
        return True
    
    def classify_alert_severity(self, alert_data: Dict) -> str:
        """Classify alert severity based on transaction characteristics"""
        
        if alert_data.get('alert_type') == 'smart_money_transaction':
            value = alert_data.get('value', 0)
            score = alert_data.get('smart_money_score', 0)
            
            if value > 1000 or score > 0.95:  # Very large or very confident
                return 'CRITICAL'
            elif value > 100 or score > 0.8:
                return 'HIGH'
            elif value > 10 or score > 0.6:
                return 'MEDIUM'
            else:
                return 'LOW'
        
        elif alert_data.get('alert_type') == 'smart_money_wallet_update':
            score = alert_data.get('smart_money_score', 0)
            volume = alert_data.get('total_volume', 0)
            
            if score > 0.9 and volume > 500:
                return 'CRITICAL'
            elif score > 0.8 and volume > 100:
                return 'HIGH'
            else:
                return 'MEDIUM'
        
        return 'LOW'
    
    async def send_slack_alert(self, alert_data: Dict, severity: str):
        """Send alert to Slack webhook"""
        
        if not self.config['monitoring'].get('slack_webhook'):
            return
        
        # Format Slack message
        color_map = {
            'CRITICAL': '#FF0000',
            'HIGH': '#FF8000',
            'MEDIUM': '#FFFF00',
            'LOW': '#00FF00'
        }
        
        if alert_data.get('alert_type') == 'smart_money_transaction':
            message = {
                "attachments": [
                    {
                        "color": color_map[severity],
                        "title": f"🚨 Smart Money Transaction Alert ({severity})",
                        "fields": [
                            {
                                "title": "Transaction Hash",
                                "value": f"`{alert_data.get('transaction_hash', 'N/A')}`",
                                "short": True
                            },
                            {
                                "title": "Value",
                                "value": f"{alert_data.get('value', 0):.4f} ETH",
                                "short": True
                            },
                            {
                                "title": "From Address",
                                "value": f"`{alert_data.get('from_address', 'N/A')}`",
                                "short": False
                            },
                            {
                                "title": "Smart Money Score",
                                "value": f"{alert_data.get('smart_money_score', 0):.3f}",
                                "short": True
                            },
                            {
                                "title": "Gas Price",
                                "value": f"{alert_data.get('gas_price', 0) / 1e9:.1f} Gwei",
                                "short": True
                            }
                        ],
                        "footer": "Smart Money Tracker",
                        "ts": int(datetime.now().timestamp())
                    }
                ]
            }
        else:  # wallet update
            message = {
                "attachments": [
                    {
                        "color": color_map[severity],
                        "title": f"📈 Smart Money Wallet Update ({severity})",
                        "fields": [
                            {
                                "title": "Wallet Address",
                                "value": f"`{alert_data.get('wallet_address', 'N/A')}`",
                                "short": False
                            },
                            {
                                "title": "Smart Money Score",
                                "value": f"{alert_data.get('smart_money_score', 0):.3f}",
                                "short": True
                            },
                            {
                                "title": "Total Volume",
                                "value": f"{alert_data.get('total_volume', 0):.2f} ETH",
                                "short": True
                            },
                            {
                                "title": "Transaction Count",
                                "value": str(alert_data.get('transaction_count', 0)),
                                "short": True
                            },
                            {
                                "title": "Success Rate",
                                "value": f"{alert_data.get('success_rate', 0):.1%}",
                                "short": True
                            }
                        ],
                        "footer": "Smart Money Tracker",
                        "ts": int(datetime.now().timestamp())
                    }
                ]
            }
        
        try:
            response = requests.post(
                self.config['monitoring']['slack_webhook'],
                json=message,
                timeout=10
            )
            response.raise_for_status()
            self.logger.info("Slack alert sent successfully")
        
        except Exception as e:
            self.logger.error(f"Failed to send Slack alert: {e}")
    
    async def send_email_alert(self, alert_data: Dict, severity: str):
        """Send alert via email"""
        
        email_list = self.config['monitoring'].get('email_alerts', [])
        if not email_list:
            return
        
        # Email configuration (add to config.yaml)
        smtp_config = self.config.get('email', {
            'smtp_server': 'smtp.gmail.com',
            'smtp_port': 587,
            'username': 'your_email@gmail.com',
            'password': 'your_app_password'
        })
        
        subject = f"[{severity}] Smart Money Alert - {alert_data.get('alert_type')}"
        
        # Create email body
        if alert_data.get('alert_type') == 'smart_money_transaction':
            body = f"""
            Smart Money Transaction Detected!
            
            Severity: {severity}
            Transaction Hash: {alert_data.get('transaction_hash')}
            From Address: {alert_data.get('from_address')}
            To Address: {alert_data.get('to_address')}
            Value: {alert_data.get('value', 0):.4f} ETH
            Smart Money Score: {alert_data.get('smart_money_score', 0):.3f}
            Gas Price: {alert_data.get('gas_price', 0) / 1e9:.1f} Gwei
            Block Number: {alert_data.get('block_number')}
            Timestamp: {alert_data.get('timestamp')}
            
            Characteristics:
            - Large Transaction: {alert_data.get('characteristics', {}).get('is_large_transaction', False)}
            - High Gas: {alert_data.get('characteristics', {}).get('is_high_gas', False)}
            - Contract Interaction: {alert_data.get('characteristics', {}).get('has_contract_interaction', False)}
            - Token Transfers: {alert_data.get('characteristics', {}).get('has_token_transfers', False)}
            
            View on Etherscan: https://etherscan.io/tx/{alert_data.get('transaction_hash')}
            """
        else:
            body = f"""
            Smart Money Wallet Update!
            
            Severity: {severity}
            Wallet Address: {alert_data.get('wallet_address')}
            Smart Money Score: {alert_data.get('smart_money_score', 0):.3f}
            Total Volume: {alert_data.get('total_volume', 0):.2f} ETH
            Transaction Count: {alert_data.get('transaction_count', 0)}
            Success Rate: {alert_data.get('success_rate', 0):.1%}
            Trigger Transaction: {alert_data.get('trigger_transaction')}
            
            View on Etherscan: https://etherscan.io/address/{alert_data.get('wallet_address')}
            """
        
        try:
            msg = MIMEMultipart()
            msg['From'] = smtp_config['username']
            msg['Subject'] = subject
            msg.attach(MIMEText(body, 'plain'))
            
            server = smtplib.SMTP(smtp_config['smtp_server'], smtp_config['smtp_port'])
            server.starttls()
            server.login(smtp_config['username'], smtp_config['password'])
            
            for email in email_list:
                msg['To'] = email
                server.send_message(msg)
                del msg['To']
            
            server.quit()
            self.logger.info(f"Email alerts sent to {len(email_list)} recipients")
            
        except Exception as e:
            self.logger.error(f"Failed to send email alert: {e}")
    
    def update_prometheus_metrics(self, alert_data: Dict, severity: str):
        """Update Prometheus metrics"""
        
        alert_type = alert_data.get('alert_type', 'unknown')
        ALERT_COUNTER.labels(alert_type=alert_type, severity=severity).inc()
        
        if alert_type == 'smart_money_transaction':
            value = alert_data.get('value', 0)
            if value > 0:
                TRANSACTION_VALUE_GAUGE.set(value)
        
        elif alert_type == 'smart_money_wallet_update':
            wallet_address = alert_data.get('wallet_address', 'unknown')
            score = alert_data.get('smart_money_score', 0)
            WALLET_SCORE_GAUGE.labels(wallet_address=wallet_address).set(score)
    
    async def process_alert(self, alert_data: Dict):
        """Process and distribute alert"""
        
        with PROCESSING_TIME.time():
            try:
                # Classify severity
                severity = self.classify_alert_severity(alert_data)
                
                # Check rate limiting
                wallet_address = alert_data.get('wallet_address') or alert_data.get('from_address')
                if not self.should_send_alert(alert_data.get('alert_type'), wallet_address):
                    self.logger.debug("Alert rate limited")
                    return
                
                # Update metrics
                self.update_prometheus_metrics(alert_data, severity)
                
                # Send notifications based on severity
                if severity in ['CRITICAL', 'HIGH']:
                    await self.send_slack_alert(alert_data, severity)
                    await self.send_email_alert(alert_data, severity)
                elif severity == 'MEDIUM':
                    await self.send_slack_alert(alert_data, severity)
                
                # Store alert history
                alert_id = f"{alert_data.get('timestamp')}_{wallet_address}"
                self.alert_history[alert_id] = {
                    'data': alert_data,
                    'severity': severity,
                    'processed_at': datetime.now().isoformat()
                }
                
                self.logger.info(f"Processed {severity} alert for {wallet_address}")
                
            except Exception as e:
                self.logger.error(f"Failed to process alert: {e}")

class SmartMoneyMonitor:
    def __init__(self, config_path: str = "configs/config.yaml"):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.logger = logging.getLogger(__name__)
        self.alert_manager = AlertManager(config_path)
        
        # Start Prometheus metrics server
        start_http_server(8000)
        self.logger.info("Prometheus metrics server started on port 8000")
    
    def create_kafka_consumers(self) -> List[KafkaConsumer]:
        """Create Kafka consumers for alert topics"""
        
        consumers = []
        
        # Smart money alerts consumer
        alerts_consumer = KafkaConsumer(
            self.config['kafka']['topics']['smart_money_alerts'],
            bootstrap_servers=self.config['kafka']['bootstrap_servers'],
            group_id='smart-money-alert-processor',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='latest'
        )
        consumers.append(('alerts', alerts_consumer))
        
        # Wallet updates consumer
        wallet_consumer = KafkaConsumer(
            self.config['kafka']['topics']['wallet_updates'],
            bootstrap_servers=self.config['kafka']['bootstrap_servers'],
            group_id='smart-money-wallet-processor',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='latest'
        )
        consumers.append(('wallets', wallet_consumer))
        
        return consumers
    
    async def monitor_alerts(self):
        """Main monitoring loop"""
        
        self.logger.info("Starting smart money alert monitoring...")
        
        consumers = self.create_kafka_consumers()
        
        try:
            while True:
                for consumer_name, consumer in consumers:
                    try:
                        # Poll for messages with timeout
                        messages = consumer.poll(timeout_ms=1000)
                        
                        for topic_partition, message_list in messages.items():
                            for message in message_list:
                                alert_data = message.value
                                self.logger.debug(f"Received {consumer_name} alert: {alert_data}")
                                
                                # Process alert
                                await self.alert_manager.process_alert(alert_data)
                                
                    except Exception as e:
                        self.logger.error(f"Error processing {consumer_name} messages: {e}")
                
                # Small delay to prevent busy waiting
                await asyncio.sleep(0.1)
                
        except KeyboardInterrupt:
            self.logger.info("Shutting down monitoring...")
        finally:
            for _, consumer in consumers:
                consumer.close()

def main():
    """Run the monitoring system"""
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    monitor = SmartMoneyMonitor()
    
    try:
        asyncio.run(monitor.monitor_alerts())
    except KeyboardInterrupt:
        print("\nShutting down gracefully...")

if __name__ == "__main__":
    main()
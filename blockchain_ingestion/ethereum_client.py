"""
Ethereum blockchain data ingestion client
Fetches transaction data and identifies smart money patterns
"""

import asyncio
import json
import logging
from typing import Dict, List, Optional, AsyncGenerator
from datetime import datetime
import yaml
from web3 import Web3
from web3.middleware import geth_poa_middleware
import pandas as pd
from dataclasses import dataclass, asdict

@dataclass
class Transaction:
    hash: str
    block_number: int
    block_timestamp: datetime
    from_address: str
    to_address: str
    value: float  # in ETH
    gas_price: int  # in Wei
    gas_used: int
    gas_limit: int
    status: int
    input_data: str
    contract_address: Optional[str] = None
    token_transfers: List[Dict] = None

@dataclass
class WalletMetrics:
    address: str
    total_volume: float
    transaction_count: int
    success_rate: float
    avg_gas_price: float
    unique_contracts: int
    first_seen: datetime
    last_seen: datetime
    balance: float
    smart_money_score: float = 0.0

class EthereumClient:
    def __init__(self, config_path: str = "configs/config.yaml"):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.w3 = Web3(Web3.HTTPProvider(self.config['blockchain']['ethereum']['rpc_url']))
        self.w3.middleware_onion.inject(geth_poa_middleware, layer=0)
        
        self.logger = logging.getLogger(__name__)
        self.smart_money_wallets = set(self.config['smart_money']['tracking_wallets'])
        
    def is_connected(self) -> bool:
        return self.w3.is_connected()
    
    def get_latest_block_number(self) -> int:
        return self.w3.eth.block_number
    
    def get_block_transactions(self, block_number: int) -> List[Transaction]:
        """Extract all transactions from a specific block"""
        try:
            block = self.w3.eth.get_block(block_number, full_transactions=True)
            transactions = []
            
            for tx in block.transactions:
                transaction = Transaction(
                    hash=tx.hash.hex(),
                    block_number=block_number,
                    block_timestamp=datetime.fromtimestamp(block.timestamp),
                    from_address=tx['from'].lower(),
                    to_address=tx['to'].lower() if tx['to'] else "",
                    value=self.w3.from_wei(tx.value, 'ether'),
                    gas_price=tx.gasPrice,
                    gas_used=0,  # Will be filled by receipt
                    gas_limit=tx.gas,
                    status=1,  # Will be filled by receipt
                    input_data=tx.input.hex(),
                    token_transfers=[]
                )
                
                # Get transaction receipt for gas used and status
                try:
                    receipt = self.w3.eth.get_transaction_receipt(tx.hash)
                    transaction.gas_used = receipt.gasUsed
                    transaction.status = receipt.status
                    transaction.contract_address = receipt.contractAddress
                    
                    # Parse token transfers from logs if any
                    transaction.token_transfers = self._parse_token_transfers(receipt.logs)
                    
                except Exception as e:
                    self.logger.warning(f"Failed to get receipt for tx {tx.hash.hex()}: {e}")
                
                transactions.append(transaction)
                
            return transactions
            
        except Exception as e:
            self.logger.error(f"Failed to get block {block_number}: {e}")
            return []
    
    def _parse_token_transfers(self, logs: List) -> List[Dict]:
        """Parse ERC20 token transfer events from transaction logs"""
        transfers = []
        # ERC20 Transfer event signature
        transfer_signature = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
        
        for log in logs:
            if len(log.topics) >= 3 and log.topics[0].hex() == transfer_signature:
                try:
                    transfer = {
                        'contract_address': log.address.lower(),
                        'from_address': '0x' + log.topics[1].hex()[-40:],
                        'to_address': '0x' + log.topics[2].hex()[-40:],
                        'value': int(log.data, 16)
                    }
                    transfers.append(transfer)
                except Exception as e:
                    self.logger.warning(f"Failed to parse token transfer: {e}")
        
        return transfers
    
    def calculate_wallet_metrics(self, address: str, transactions: List[Transaction]) -> WalletMetrics:
        """Calculate smart money metrics for a wallet"""
        wallet_txs = [tx for tx in transactions if tx.from_address == address.lower()]
        
        if not wallet_txs:
            return None
        
        total_volume = sum(tx.value for tx in wallet_txs)
        success_rate = sum(1 for tx in wallet_txs if tx.status == 1) / len(wallet_txs)
        avg_gas_price = sum(tx.gas_price for tx in wallet_txs) / len(wallet_txs)
        unique_contracts = len(set(tx.to_address for tx in wallet_txs if tx.to_address))
        
        first_seen = min(tx.block_timestamp for tx in wallet_txs)
        last_seen = max(tx.block_timestamp for tx in wallet_txs)
        
        # Get current balance
        balance = self.w3.from_wei(self.w3.eth.get_balance(address), 'ether')
        
        # Calculate smart money score
        smart_money_score = self._calculate_smart_money_score({
            'total_volume': total_volume,
            'success_rate': success_rate,
            'balance': balance,
            'transaction_count': len(wallet_txs),
            'unique_contracts': unique_contracts
        })
        
        return WalletMetrics(
            address=address.lower(),
            total_volume=total_volume,
            transaction_count=len(wallet_txs),
            success_rate=success_rate,
            avg_gas_price=avg_gas_price,
            unique_contracts=unique_contracts,
            first_seen=first_seen,
            last_seen=last_seen,
            balance=balance,
            smart_money_score=smart_money_score
        )
    
    def _calculate_smart_money_score(self, metrics: Dict) -> float:
        """Calculate smart money confidence score"""
        score = 0.0
        
        # Volume score (30%)
        if metrics['total_volume'] > self.config['smart_money']['min_transaction_volume']:
            score += 0.3 * min(metrics['total_volume'] / 10000, 1.0)
        
        # Success rate score (25%)
        if metrics['success_rate'] > self.config['smart_money']['min_success_rate']:
            score += 0.25 * metrics['success_rate']
        
        # Balance score (20%)
        if metrics['balance'] > self.config['smart_money']['min_wallet_balance']:
            score += 0.2 * min(metrics['balance'] / 1000, 1.0)
        
        # Activity score (15%)
        activity_score = min(metrics['transaction_count'] / 100, 1.0)
        score += 0.15 * activity_score
        
        # Diversification score (10%)
        diversification_score = min(metrics['unique_contracts'] / 50, 1.0)
        score += 0.1 * diversification_score
        
        return min(score, 1.0)
    
    def identify_smart_money_transactions(self, transactions: List[Transaction]) -> List[Transaction]:
        """Filter transactions that show smart money patterns"""
        smart_transactions = []
        
        for tx in transactions:
            # Check if wallet is already known smart money
            if tx.from_address in self.smart_money_wallets:
                smart_transactions.append(tx)
                continue
            
            # Check transaction characteristics
            if self._is_smart_money_transaction(tx):
                smart_transactions.append(tx)
        
        return smart_transactions
    
    def _is_smart_money_transaction(self, tx: Transaction) -> bool:
        """Determine if a transaction shows smart money characteristics"""
        # Large transaction value
        if tx.value > 10:  # > 10 ETH
            return True
        
        # High gas price indicating urgency/sophistication
        if tx.gas_price > self.w3.to_wei(100, 'gwei'):
            return True
        
        # Contract interactions (potential DeFi/MEV)
        if tx.to_address and len(tx.input_data) > 10:
            return True
        
        # Token transfers indicating trading activity
        if tx.token_transfers and len(tx.token_transfers) > 0:
            return True
        
        return False
    
    async def stream_new_transactions(self) -> AsyncGenerator[Transaction, None]:
        """Stream new transactions in real-time"""
        latest_block = self.get_latest_block_number()
        
        while True:
            try:
                current_block = self.get_latest_block_number()
                
                if current_block > latest_block:
                    for block_num in range(latest_block + 1, current_block + 1):
                        transactions = self.get_block_transactions(block_num)
                        for tx in transactions:
                            yield tx
                    
                    latest_block = current_block
                
                await asyncio.sleep(2)  # Check for new blocks every 2 seconds
                
            except Exception as e:
                self.logger.error(f"Error in transaction stream: {e}")
                await asyncio.sleep(5)

def main():
    """Test the Ethereum client"""
    client = EthereumClient()
    
    if not client.is_connected():
        print("Failed to connect to Ethereum node")
        return
    
    print(f"Connected to Ethereum. Latest block: {client.get_latest_block_number()}")
    
    # Get recent block transactions
    latest_block = client.get_latest_block_number()
    transactions = client.get_block_transactions(latest_block)
    
    print(f"Found {len(transactions)} transactions in block {latest_block}")
    
    # Identify smart money transactions
    smart_txs = client.identify_smart_money_transactions(transactions)
    print(f"Identified {len(smart_txs)} smart money transactions")
    
    for tx in smart_txs[:3]:  # Show first 3
        print(f"Smart money tx: {tx.hash} - {tx.value} ETH")

if __name__ == "__main__":
    main()
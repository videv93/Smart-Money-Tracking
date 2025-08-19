"""
Machine Learning model for smart money pattern detection
Uses behavioral features to classify wallets as smart money
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import yaml
import pickle
import mlflow
import mlflow.sklearn
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score
from sklearn.feature_selection import SelectKBest, f_classif
import psycopg2
import logging
from typing import Dict, Tuple, List

class SmartMoneyModel:
    def __init__(self, config_path: str = "configs/config.yaml"):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.logger = logging.getLogger(__name__)
        self.model = None
        self.scaler = StandardScaler()
        self.feature_selector = None
        self.feature_names = []
        
        # Initialize MLflow
        mlflow.set_tracking_uri("http://localhost:5000")
        mlflow.set_experiment("smart-money-detection")
        
    def load_training_data(self) -> pd.DataFrame:
        """Load wallet features from PostgreSQL staging"""
        
        conn = psycopg2.connect(
            host=self.config['postgresql']['host'],
            port=self.config['postgresql']['port'],
            database=self.config['postgresql']['database'],
            user=self.config['postgresql']['username'],
            password=self.config['postgresql']['password']
        )
        
        query = f"""
        SELECT 
            from_address,
            total_volume,
            avg_transaction_value,
            transaction_count,
            success_rate,
            avg_gas_price,
            unique_recipients,
            unique_contracts,
            transactions_per_day,
            recipient_diversity,
            contract_interaction_rate,
            mev_score,
            high_gas_transactions,
            potential_sandwich_attacks,
            smart_money_category,
            smart_money_score
        FROM {self.config['postgresql']['staging_schema']}.wallet_features
        WHERE transaction_count >= 10  -- Minimum activity threshold
        """
        
        df = pd.read_sql(query, conn)
        conn.close()
        
        return df
    
    def create_labels(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create binary labels for smart money classification"""
        
        # Define smart money criteria
        smart_money_conditions = (
            (df['total_volume'] >= self.config['smart_money']['min_transaction_volume']) &
            (df['success_rate'] >= self.config['smart_money']['min_success_rate']) &
            (df['smart_money_score'] >= 0.7) &  # High confidence threshold
            (
                (df['smart_money_category'].isin(['whale', 'mev_bot', 'institutional'])) |
                (df['mev_score'] > 0.3) |  # High MEV activity
                (df['avg_gas_price'] > 50000000000) |  # Consistently high gas
                (df['contract_interaction_rate'] > 0.5)  # Heavy DeFi usage
            )
        )
        
        df['is_smart_money'] = smart_money_conditions.astype(int)
        
        return df
    
    def engineer_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create additional engineered features"""
        
        # Efficiency metrics
        df['gas_efficiency'] = df['avg_gas_price'] / (df['avg_gas_price'].quantile(0.9) + 1e-6)
        df['volume_per_transaction'] = df['total_volume'] / df['transaction_count']
        df['contracts_per_transaction'] = df['unique_contracts'] / df['transaction_count']
        
        # Activity patterns
        df['high_gas_ratio'] = df['high_gas_transactions'] / df['transaction_count']
        df['sandwich_ratio'] = df['potential_sandwich_attacks'] / df['transaction_count']
        
        # Composite scores
        df['activity_intensity'] = np.log1p(df['transactions_per_day']) * df['success_rate']
        df['sophistication_score'] = (
            df['contract_interaction_rate'] * 0.4 +
            df['mev_score'] * 0.3 +
            df['high_gas_ratio'] * 0.3
        )
        
        # Risk-adjusted metrics
        df['risk_adjusted_volume'] = df['total_volume'] * df['success_rate']
        df['diversification_index'] = (df['unique_recipients'] + df['unique_contracts']) / (df['transaction_count'] + 1)
        
        return df
    
    def select_features(self, X: pd.DataFrame, y: pd.Series, k: int = 15) -> Tuple[pd.DataFrame, List[str]]:
        """Select best features using statistical tests"""
        
        self.feature_selector = SelectKBest(score_func=f_classif, k=k)
        X_selected = self.feature_selector.fit_transform(X, y)
        
        # Get selected feature names
        selected_features = X.columns[self.feature_selector.get_support()].tolist()
        
        self.logger.info(f"Selected features: {selected_features}")
        
        return pd.DataFrame(X_selected, columns=selected_features, index=X.index), selected_features
    
    def train_models(self, X: pd.DataFrame, y: pd.Series) -> Dict:
        """Train multiple models and select best performer"""
        
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
        
        # Scale features
        X_train_scaled = pd.DataFrame(
            self.scaler.fit_transform(X_train),
            columns=X_train.columns,
            index=X_train.index
        )
        X_test_scaled = pd.DataFrame(
            self.scaler.transform(X_test),
            columns=X_test.columns,
            index=X_test.index
        )
        
        models = {
            'random_forest': RandomForestClassifier(
                n_estimators=200,
                max_depth=10,
                min_samples_split=5,
                min_samples_leaf=2,
                random_state=42,
                class_weight='balanced'
            ),
            'gradient_boosting': GradientBoostingClassifier(
                n_estimators=150,
                max_depth=8,
                learning_rate=0.1,
                min_samples_split=5,
                random_state=42
            ),
            'logistic_regression': LogisticRegression(
                C=1.0,
                class_weight='balanced',
                random_state=42,
                max_iter=1000
            )
        }
        
        results = {}
        
        with mlflow.start_run():
            for name, model in models.items():
                self.logger.info(f"Training {name}...")
                
                # Train model
                if name == 'logistic_regression':
                    model.fit(X_train_scaled, y_train)
                    y_pred = model.predict(X_test_scaled)
                    y_pred_proba = model.predict_proba(X_test_scaled)[:, 1]
                else:
                    model.fit(X_train, y_train)
                    y_pred = model.predict(X_test)
                    y_pred_proba = model.predict_proba(X_test)[:, 1]
                
                # Calculate metrics
                auc = roc_auc_score(y_test, y_pred_proba)
                cv_scores = cross_val_score(model, X_train if name != 'logistic_regression' else X_train_scaled, y_train, cv=5, scoring='roc_auc')
                
                results[name] = {
                    'model': model,
                    'auc': auc,
                    'cv_mean': cv_scores.mean(),
                    'cv_std': cv_scores.std(),
                    'predictions': y_pred,
                    'probabilities': y_pred_proba
                }
                
                # Log to MLflow
                mlflow.log_param(f"{name}_model_type", name)
                mlflow.log_metric(f"{name}_auc", auc)
                mlflow.log_metric(f"{name}_cv_mean", cv_scores.mean())
                
                self.logger.info(f"{name} - AUC: {auc:.4f}, CV: {cv_scores.mean():.4f} (+/- {cv_scores.std() * 2:.4f})")
                print(f"\n{name} Classification Report:")
                print(classification_report(y_test, y_pred))
        
        # Select best model based on cross-validation AUC
        best_model_name = max(results.keys(), key=lambda x: results[x]['cv_mean'])
        self.model = results[best_model_name]['model']
        
        self.logger.info(f"Selected best model: {best_model_name}")
        
        return results, X_test, y_test
    
    def analyze_feature_importance(self, X: pd.DataFrame):
        """Analyze and log feature importance"""
        
        if hasattr(self.model, 'feature_importances_'):
            importance_df = pd.DataFrame({
                'feature': X.columns,
                'importance': self.model.feature_importances_
            }).sort_values('importance', ascending=False)
            
            self.logger.info("Feature Importance:")
            for _, row in importance_df.head(10).iterrows():
                self.logger.info(f"{row['feature']}: {row['importance']:.4f}")
            
            # Log to MLflow
            mlflow.log_table(importance_df.head(15), "feature_importance.json")
            
            return importance_df
        
        return None
    
    def save_model(self, model_path: str = "models/smart_money_model.pkl"):
        """Save trained model and preprocessing components"""
        
        model_artifacts = {
            'model': self.model,
            'scaler': self.scaler,
            'feature_selector': self.feature_selector,
            'feature_names': self.feature_names,
            'config': self.config,
            'timestamp': datetime.now().isoformat()
        }
        
        with open(model_path, 'wb') as f:
            pickle.dump(model_artifacts, f)
        
        # Log model to MLflow
        mlflow.sklearn.log_model(self.model, "smart_money_model")
        mlflow.log_artifact(model_path)
        
        self.logger.info(f"Model saved to {model_path}")
    
    def load_model(self, model_path: str = "models/smart_money_model.pkl"):
        """Load trained model and preprocessing components"""
        
        with open(model_path, 'rb') as f:
            artifacts = pickle.load(f)
        
        self.model = artifacts['model']
        self.scaler = artifacts['scaler']
        self.feature_selector = artifacts['feature_selector']
        self.feature_names = artifacts['feature_names']
        
        self.logger.info(f"Model loaded from {model_path}")
    
    def predict(self, wallet_features: pd.DataFrame) -> Dict:
        """Predict smart money probability for new wallets"""
        
        if self.model is None:
            raise ValueError("Model not trained or loaded")
        
        # Engineer features
        wallet_features = self.engineer_features(wallet_features)
        
        # Select features
        X = wallet_features[self.feature_names]
        
        # Scale if using logistic regression
        if isinstance(self.model, LogisticRegression):
            X = pd.DataFrame(
                self.scaler.transform(X),
                columns=X.columns,
                index=X.index
            )
        
        # Predict
        probabilities = self.model.predict_proba(X)[:, 1]
        predictions = (probabilities >= self.config['ml']['threshold']).astype(int)
        
        return {
            'probabilities': probabilities,
            'predictions': predictions,
            'smart_money_addresses': wallet_features.loc[predictions == 1, 'from_address'].tolist()
        }
    
    def train_pipeline(self):
        """Full training pipeline"""
        
        self.logger.info("Starting smart money model training...")
        
        # Load data
        df = self.load_training_data()
        self.logger.info(f"Loaded {len(df)} wallet records")
        
        # Create labels
        df = self.create_labels(df)
        smart_money_rate = df['is_smart_money'].mean()
        self.logger.info(f"Smart money rate: {smart_money_rate:.3f}")
        
        if smart_money_rate < 0.01:
            self.logger.warning("Very low smart money rate, consider adjusting criteria")
        
        # Engineer features
        df = self.engineer_features(df)
        
        # Prepare features
        feature_columns = [col for col in df.columns if col not in ['from_address', 'is_smart_money', 'smart_money_category']]
        X = df[feature_columns].fillna(0)
        y = df['is_smart_money']
        
        # Select best features
        X_selected, selected_features = self.select_features(X, y)
        self.feature_names = selected_features
        
        # Train models
        results, X_test, y_test = self.train_models(X_selected, y)
        
        # Analyze feature importance
        self.analyze_feature_importance(X_selected)
        
        # Save model
        self.save_model()
        
        return results

def main():
    """Train smart money detection model"""
    model = SmartMoneyModel()
    results = model.train_pipeline()
    
    print("Training completed!")
    print(f"Best model AUC: {max(r['auc'] for r in results.values()):.4f}")

if __name__ == "__main__":
    main()
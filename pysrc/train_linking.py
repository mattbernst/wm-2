#!/usr/bin/env python3
"""
CatBoost Link Validity Predictor - Object Oriented Version

This script trains a CatBoost model to predict link validity (isValidLink)
from numerical features in CSV data using cross-validation.
"""

import argparse
import logging
import os
import sys
import time
import warnings

import numpy as np
import pandas as pd
from catboost import CatBoostClassifier
from sklearn.metrics import f1_score, roc_auc_score, classification_report, confusion_matrix
from sklearn.model_selection import cross_val_score, StratifiedKFold

warnings.filterwarnings('ignore')


class LinkValidityPredictor:
    """
    A class for training and evaluating CatBoost models for link validity prediction.
    """

    def __init__(self, cv_folds=5, random_seed=42):
        """
        Initialize the predictor.

        Args:
            cv_folds (int): Number of cross-validation folds
            random_seed (int): Random seed for reproducibility
        """
        self.cv_folds = cv_folds
        self.random_seed = random_seed
        self.model = None
        self.feature_names = None
        self.logger = None

        # CatBoost model parameters
        self.model_params = {
            'iterations': 1000,
            'learning_rate': 0.1,
            'depth': 6,
            'loss_function': 'Logloss',
            'eval_metric': 'F1',
            'random_seed': 1,
            'verbose': False,
            'early_stopping_rounds': 100
        }

    def setup_logging(self, log_file=None):
        """
        Set up logging to file with timestamp.

        Args:
            log_file (str, optional): Custom log file name
        """
        if log_file is None:
            timestamp = int(time.time() * 1000)  # milliseconds since epoch
            log_file = f"model_linking_run_{timestamp}.log"

        # Create logger
        self.logger = logging.getLogger('LinkValidityPredictor')
        self.logger.setLevel(logging.INFO)

        # Remove existing handlers to avoid duplicates
        self.logger.handlers.clear()

        # Create file handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)

        # Create console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)

        # Create formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)

        # Add handler to logger
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        self.logger.propagate = False

        self.logger.info(f"Logging initialized. Log file: {log_file}")
        return log_file

    def _get_prefix(self, train_file):
        """
        Extract prefix from training file name.

        Args:
            train_file (str): Path to training file

        Returns:
            str: File prefix
        """
        train_basename = os.path.basename(train_file)
        return train_basename.split("_linking")[0]

    def load_and_prepare_data(self, file_path):
        """
        Load CSV data and prepare features and target.

        Args:
            file_path (str): Path to the CSV file

        Returns:
            tuple: (features_df, target_series)
        """
        self.logger.info(f"Loading data from: {file_path}")

        # Load the data
        df = pd.read_csv(file_path)

        # Check if the target column exists
        if 'isValidLink' not in df.columns:
            error_msg = f"Target column 'isValidLink' not found in {file_path}"
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        # Separate features and target
        feature_columns = [col for col in df.columns if col != 'isValidLink']
        X = df[feature_columns]
        y = df['isValidLink']

        # Store feature names for later use
        if self.feature_names is None:
            self.feature_names = feature_columns

        self.logger.info(f"Data shape: {df.shape}")
        self.logger.info(f"Features: {feature_columns}")
        self.logger.info(f"Target distribution:")
        self.logger.info(f"\n{y.value_counts()}")
        self.logger.info(f"Target distribution (%):")
        self.logger.info(f"\n{y.value_counts(normalize=True) * 100}")

        return X, y

    def perform_cross_validation(self, X, y):
        """
        Perform cross-validation with CatBoost model.

        Args:
            X (pd.DataFrame): Features
            y (pd.Series): Target

        Returns:
            tuple: (f1_scores, auc_scores, trained_model)
        """
        self.logger.info(f"Performing {self.cv_folds}-fold cross-validation...")

        # Initialize CatBoost classifier
        self.model = CatBoostClassifier(**self.model_params)

        # Set up cross-validation
        cv = StratifiedKFold(n_splits=self.cv_folds, shuffle=True, random_state=self.random_seed)

        # Perform cross-validation for F1 score
        f1_scores = cross_val_score(self.model, X, y, cv=cv, scoring='f1')

        # Perform cross-validation for AUC score
        auc_scores = cross_val_score(self.model, X, y, cv=cv, scoring='roc_auc')

        # Train final model on full dataset
        self.model.fit(X, y)

        return f1_scores, auc_scores, self.model

    def evaluate_model(self, X_val, y_val):
        """
        Evaluate the trained model on validation data.

        Args:
            X_val (pd.DataFrame): Validation features
            y_val (pd.Series): Validation target

        Returns:
            dict: Evaluation metrics
        """
        if self.model is None:
            error_msg = "Model not trained yet. Call perform_cross_validation first."
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        self.logger.info("Evaluating on validation data...")

        # Make predictions
        y_pred = self.model.predict(X_val)
        y_pred_proba = self.model.predict_proba(X_val)[:, 1]

        # Calculate metrics
        f1 = f1_score(y_val, y_pred)
        auc = roc_auc_score(y_val, y_pred_proba)

        # Log detailed results
        self.logger.info(f"Validation F1 Score: {f1:.4f}")
        self.logger.info(f"Validation AUC Score: {auc:.4f}")
        self.logger.info("Classification Report:")
        self.logger.info(f"\n{classification_report(y_val, y_pred)}")

        # Conservative predictions (higher precision, lower recall)
        self.logger.info("Conservative predictions")
        conservative_preds = (self.model.predict_proba(X_val)[:, 1] >= 0.8).astype(int)
        self.logger.info(f"\n{classification_report(y_val, conservative_preds)}")

        self.logger.info("Confusion Matrix:")
        self.logger.info(f"\n{confusion_matrix(y_val, y_pred)}")

        # Log example predictions
        self._log_example_predictions(y_val, y_pred, y_pred_proba)

        return {
            'f1': f1,
            'auc': auc,
            'predictions': y_pred,
            'probabilities': y_pred_proba
        }

    def _log_example_predictions(self, y_val, y_pred, y_pred_proba):
        """
        Log example predictions for analysis.

        Args:
            y_val (pd.Series): Actual values
            y_pred (np.array): Predicted values
            y_pred_proba (np.array): Prediction probabilities
        """
        self.logger.info("Example Predictions:")
        self.logger.info("=" * 80)
        self.logger.info(f"{'Index':<6} {'Actual':<7} {'Predicted':<10} {'Probability':<12} {'Correct':<8} {'Status'}")
        self.logger.info("-" * 80)

        # Get indices for different prediction scenarios
        correct_indices = np.where(y_pred == y_val)[0]
        incorrect_indices = np.where(y_pred != y_val)[0]

        # Show a mix of correct and incorrect predictions
        example_indices = []

        # Add some correct predictions
        if len(correct_indices) > 0:
            example_indices.extend(correct_indices[:5])

        # Add some incorrect predictions
        if len(incorrect_indices) > 0:
            example_indices.extend(incorrect_indices[:5])

        # If we don't have enough examples, add more randomly
        if len(example_indices) < 10:
            remaining_indices = np.setdiff1d(np.arange(len(y_val)), example_indices)
            additional_needed = min(10 - len(example_indices), len(remaining_indices))
            if additional_needed > 0:
                additional_indices = np.random.choice(remaining_indices, additional_needed, replace=False)
                example_indices.extend(additional_indices)

        # Sort indices for better readability
        example_indices = sorted(example_indices[:10])

        for idx in example_indices:
            actual = y_val.iloc[idx]
            predicted = y_pred[idx]
            probability = y_pred_proba[idx]
            is_correct = actual == predicted

            status = "✓ Correct" if is_correct else "✗ Wrong"
            self.logger.info(f"{idx:<6} {actual:<7} {predicted:<10} {probability:<12.4f} {str(is_correct):<8} {status}")

        self.logger.info("-" * 80)

        # Summary statistics for the examples
        correct_count = sum(1 for idx in example_indices if y_val.iloc[idx] == y_pred[idx])
        total_examples = len(example_indices)
        example_accuracy = correct_count / total_examples if total_examples > 0 else 0

        self.logger.info(f"Example set accuracy: {correct_count}/{total_examples} ({example_accuracy:.2%})")

        # Show prediction confidence distribution
        self.logger.info("Prediction Confidence Distribution:")
        self.logger.info(f"High confidence (>0.8): {np.sum(y_pred_proba > 0.8)} predictions")
        self.logger.info(
            f"Medium confidence (0.2-0.8): {np.sum((y_pred_proba >= 0.2) & (y_pred_proba <= 0.8))} predictions")
        self.logger.info(f"Low confidence (<0.2): {np.sum(y_pred_proba < 0.2)} predictions")

    def log_feature_importance(self):
        """
        Log feature importance from the trained model.
        """
        if self.model is None:
            error_msg = "Model not trained yet. Call perform_cross_validation first."
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        self.logger.info("Feature Importance:")
        self.logger.info("-" * 40)

        # Get feature importances
        importances = self.model.get_feature_importance()

        # Create feature importance dataframe
        feature_importance_df = pd.DataFrame({
            'feature': self.feature_names,
            'importance': importances
        }).sort_values('importance', ascending=False)

        for _, row in feature_importance_df.iterrows():
            self.logger.info(f"{row['feature']:<30}: {row['importance']:.4f}")

    def save_model(self, output_path):
        """
        Save the trained model.

        Args:
            output_path (str): Path to save the model
        """
        if self.model is None:
            error_msg = "Model not trained yet. Call perform_cross_validation first."
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        self.model.save_model(output_path)
        self.logger.info(f"Model saved to: {output_path}")

    def run_full_pipeline(self, train_file, val_file, model_output=None):
        """
        Run the complete training and validation pipeline.

        Args:
            train_file (str): Path to training CSV file
            val_file (str): Path to validation CSV file
            model_output (str, optional): Output path for trained model

        Returns:
            dict: Results including cross-validation and validation metrics
        """
        # Generate default model output name if not provided
        if model_output is None:
            prefix = self._get_prefix(train_file)
            model_output = f"{prefix}_link_validity.cbm"

        self.logger.info("=" * 60)
        self.logger.info("CatBoost Link Validity Model")
        self.logger.info("=" * 60)
        self.logger.info(f"Training file: {train_file}")
        self.logger.info(f"Validation file: {val_file}")
        self.logger.info(f"Model output: {model_output}")
        self.logger.info(f"CV folds: {self.cv_folds}")
        self.logger.info("=" * 60)

        try:
            # Load training data
            X_train, y_train = self.load_and_prepare_data(train_file)

            # Load validation data
            X_val, y_val = self.load_and_prepare_data(val_file)

            # Ensure feature consistency between train and validation
            if list(X_train.columns) != list(X_val.columns):
                error_msg = "Feature columns don't match between training and validation data"
                self.logger.error(error_msg)
                raise ValueError(error_msg)

            # Perform cross-validation
            f1_scores, auc_scores, trained_model = self.perform_cross_validation(X_train, y_train)

            # Log cross-validation results
            self.logger.info("=" * 60)
            self.logger.info("CROSS-VALIDATION RESULTS")
            self.logger.info("=" * 60)
            self.logger.info(f"F1 Scores: {f1_scores}")
            self.logger.info(f"Mean F1: {np.mean(f1_scores):.4f} (+/- {np.std(f1_scores) * 2:.4f})")
            self.logger.info(f"AUC Scores: {auc_scores}")
            self.logger.info(f"Mean AUC: {np.mean(auc_scores):.4f} (+/- {np.std(auc_scores) * 2:.4f})")

            # Evaluate on validation data
            self.logger.info("=" * 60)
            self.logger.info("VALIDATION RESULTS")
            self.logger.info("=" * 60)
            val_metrics = self.evaluate_model(X_val, y_val)

            # Log feature importance
            self.log_feature_importance()

            # Save the trained model
            self.save_model(model_output)

            # Summary
            self.logger.info("=" * 60)
            self.logger.info("SUMMARY")
            self.logger.info("=" * 60)
            self.logger.info(f"Cross-validation F1: {np.mean(f1_scores):.4f} (+/- {np.std(f1_scores) * 2:.4f})")
            self.logger.info(f"Cross-validation AUC: {np.mean(auc_scores):.4f} (+/- {np.std(auc_scores) * 2:.4f})")
            self.logger.info(f"Validation F1: {val_metrics['f1']:.4f}")
            self.logger.info(f"Validation AUC: {val_metrics['auc']:.4f}")

            return {
                'cv_f1_scores': f1_scores,
                'cv_auc_scores': auc_scores,
                'cv_f1_mean': np.mean(f1_scores),
                'cv_auc_mean': np.mean(auc_scores),
                'val_f1': val_metrics['f1'],
                'val_auc': val_metrics['auc'],
                'model_output': model_output
            }

        except Exception as e:
            self.logger.error(f"Error in pipeline: {e}")
            raise


def main():
    """
    Main function with command line argument parsing.
    """
    parser = argparse.ArgumentParser(
        description='Train and validate a CatBoost model for link validity with cross-validation',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        '--train-file',
        required=True,
        help='Path to the training CSV file'
    )

    parser.add_argument(
        '--val-file',
        required=True,
        help='Path to the validation CSV file'
    )

    parser.add_argument(
        '--model-output',
        default=None,
        help='Output path for the trained model (if not specified, auto-generated from input filenames)'
    )

    parser.add_argument(
        '--cv-folds',
        type=int,
        default=5,
        help='Number of cross-validation folds'
    )

    parser.add_argument(
        '--log-file',
        default=None,
        help='Custom log file name (if not specified, auto-generated with timestamp)'
    )

    args = parser.parse_args()

    # Initialize predictor
    predictor = LinkValidityPredictor(cv_folds=args.cv_folds)

    # Set up logging
    log_file = predictor.setup_logging(args.log_file)
    print(f"Logging to: {log_file}")

    try:
        # Run the full pipeline
        results = predictor.run_full_pipeline(
            train_file=args.train_file,
            val_file=args.val_file,
            model_output=args.model_output
        )

        print(f"Pipeline completed successfully. Results logged to: {log_file}")
        print(f"Model saved to: {results['model_output']}")
        return 0

    except Exception as e:
        print(f"Error: {e}")
        print(f"Check log file for details: {log_file}")
        return 1


if __name__ == "__main__":
    exit(main())

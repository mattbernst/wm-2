import argparse
import logging
import sys
import time

import numpy as np
import pandas as pd
from catboost import CatBoostClassifier
from sklearn.metrics import *


class CatBoostTrainer:
    """
    A class to handle CatBoost model training and validation for word sense disambiguation.
    """

    def __init__(self, train_file, val_file, log_file=None):
        self.train_file = train_file
        self.val_file = val_file
        self.feature_cols = ['commonness', 'relatedness', 'contextQuality']
        self.target_col = 'isCorrectSense'
        self.weight_col = 'weight'
        self.model = None
        self.train_df = None
        self.val_df = None

        # Set up logging
        if log_file is None:
            timestamp = int(time.time())
            log_file = f"model_training_run_{timestamp}.log"

        self.log_file = log_file
        self.setup_logging()

    def setup_logging(self):
        """
        Set up logging to capture all console output to a file.
        """
        # Create logger
        self.logger = logging.getLogger('CatBoostTrainer')
        self.logger.setLevel(logging.INFO)

        # Create file handler
        file_handler = logging.FileHandler(self.log_file)
        file_handler.setLevel(logging.INFO)

        # Create console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)

        # Create formatter
        formatter = logging.Formatter('%(asctime)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(logging.Formatter('%(message)s'))

        # Add handlers to logger
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

        # Prevent propagation to root logger
        self.logger.propagate = False

    def log(self, message):
        """
        Log a message to both console and file.
        """
        self.logger.info(message)

    def validate_data_files(self):
        """
        Validate that the CSV files exist, contain required columns, and have sufficient data.
        """
        self.log("Validating data files...")

        # Check if files exist and load them
        try:
            self.train_df = pd.read_csv(self.train_file)
            self.log(f"✓ Training file loaded: {self.train_file}")
        except FileNotFoundError:
            raise FileNotFoundError(f"Training file not found: {self.train_file}")
        except Exception as e:
            raise Exception(f"Error loading training file: {e}")

        try:
            self.val_df = pd.read_csv(self.val_file)
            self.log(f"✓ Validation file loaded: {self.val_file}")
        except FileNotFoundError:
            raise FileNotFoundError(f"Validation file not found: {self.val_file}")
        except Exception as e:
            raise Exception(f"Error loading validation file: {e}")

        # Check required columns
        required_cols = self.feature_cols + [self.target_col, self.weight_col]

        # Validate training data columns
        missing_train_cols = [col for col in required_cols if col not in self.train_df.columns]
        if missing_train_cols:
            raise ValueError(f"Training file missing required columns: {missing_train_cols}")

        # Validate validation data columns
        missing_val_cols = [col for col in required_cols if col not in self.val_df.columns]
        if missing_val_cols:
            raise ValueError(f"Validation file missing required columns: {missing_val_cols}")

        self.log(f"✓ All required columns present: {required_cols}")

        # Check minimum row requirements
        min_rows = 10  # Minimum rows for meaningful training/validation
        if len(self.train_df) < min_rows:
            raise ValueError(f"Training data has insufficient rows: {len(self.train_df)} (minimum: {min_rows})")

        if len(self.val_df) < min_rows:
            raise ValueError(f"Validation data has insufficient rows: {len(self.val_df)} (minimum: {min_rows})")

        self.log(f"✓ Sufficient data rows - Training: {len(self.train_df)}, Validation: {len(self.val_df)}")

        # Check for missing values in critical columns
        train_missing = self.train_df[required_cols].isnull().sum()
        val_missing = self.val_df[required_cols].isnull().sum()

        if train_missing.any():
            self.log(f"⚠ Warning: Missing values in training data:\n{train_missing[train_missing > 0]}")

        if val_missing.any():
            self.log(f"⚠ Warning: Missing values in validation data:\n{val_missing[val_missing > 0]}")

        # Check target variable distribution
        train_target_dist = self.train_df[self.target_col].value_counts()
        val_target_dist = self.val_df[self.target_col].value_counts()

        if len(train_target_dist) < 2:
            raise ValueError("Training data must contain both positive and negative examples")

        if len(val_target_dist) < 2:
            raise ValueError("Validation data must contain both positive and negative examples")

        self.log(f"✓ Target distribution - Training: {train_target_dist.to_dict()}")
        self.log(f"✓ Target distribution - Validation: {val_target_dist.to_dict()}")

        self.log("Data validation completed successfully!\n")

    def prepare_data(self):
        """
        Prepare training and validation data for modeling.
        """
        self.log("Preparing data for training...")

        # Prepare training data
        self.X_train = self.train_df[self.feature_cols]
        self.y_train = self.train_df[self.target_col]
        self.sample_weights = self.train_df[self.weight_col]

        # Prepare validation data
        self.X_val = self.val_df[self.feature_cols]
        self.y_val = self.val_df[self.target_col]

        self.log(f"Training data shape: {self.train_df.shape}")
        self.log(f"Validation data shape: {self.val_df.shape}")
        self.log(f"Features: {self.feature_cols}")
        self.log(f"Sample weights range: {self.sample_weights.min():.4f} - {self.sample_weights.max():.4f}")

    def train_model(self):
        """
        Initialize and train the CatBoost model.
        """
        self.log("\nInitializing CatBoost model...")

        # Initialize CatBoost classifier
        self.model = CatBoostClassifier(
            iterations=1000,
            learning_rate=0.1,
            depth=6,
            loss_function='Logloss',
            eval_metric='AUC',
            random_seed=42,
            verbose=100,  # Print progress every 100 iterations
            early_stopping_rounds=50
        )

        self.log("Training CatBoost model...")
        # Train the model with sample weights
        self.model.fit(
            self.X_train,
            self.y_train,
            sample_weight=self.sample_weights,
            eval_set=(self.X_val, self.y_val),
            plot=False  # Set to True if you want to see training plots
        )

        self.log("Model training completed!")

    def evaluate_model(self):
        """
        Evaluate the trained model on validation data and display results.
        """
        if self.model is None:
            raise ValueError("Model must be trained before evaluation")

        self.log("\nMaking predictions on validation set...")
        y_pred = self.model.predict(self.X_val)
        y_pred_proba = self.model.predict_proba(self.X_val)[:, 1]  # Probability of positive class

        # Calculate metrics
        accuracy = accuracy_score(self.y_val, y_pred)
        precision = precision_score(self.y_val, y_pred)
        recall = recall_score(self.y_val, y_pred)
        f1 = f1_score(self.y_val, y_pred)
        auc = roc_auc_score(self.y_val, y_pred_proba)

        self.log("\n" + "=" * 50)
        self.log("VALIDATION RESULTS")
        self.log("=" * 50)
        self.log(f"Accuracy:  {accuracy:.4f}")
        self.log(f"Precision: {precision:.4f}")
        self.log(f"Recall:    {recall:.4f}")
        self.log(f"F1 Score:  {f1:.4f}")
        self.log(f"AUC:       {auc:.4f}")

        self.log("\nDetailed Classification Report:")
        self.log(classification_report(self.y_val, y_pred))

        # Feature importance
        self.log("\nFeature Importance:")
        feature_importance = self.model.get_feature_importance()
        for i, feature in enumerate(self.feature_cols):
            self.log(f"{feature}: {feature_importance[i]:.4f}")


        # Validation set predictions distribution
        self.log(f"\nValidation predictions distribution:")
        self.log(f"Predicted False: {np.sum(y_pred == False)}")
        self.log(f"Predicted True:  {np.sum(y_pred == True)}")
        self.log(f"Actual False:    {np.sum(self.y_val == False)}")
        self.log(f"Actual True:     {np.sum(self.y_val == True)}")

        # Example predictions
        self.log("\nExample: Making predictions on first 5 validation samples:")
        sample_predictions = self.model.predict_proba(self.X_val.head())
        for i in range(5):
            prob_false = sample_predictions[i][0]
            prob_true = sample_predictions[i][1]
            predicted_class = y_pred[i]
            actual_class = self.y_val.iloc[i]
            self.log(f"Sample {i + 1}: P(False)={prob_false:.3f}, P(True)={prob_true:.3f}, "
                     f"Predicted={predicted_class}, Actual={actual_class}")

    def save_model(self, model_path='word_sense_catboost_model.cbm'):
        """
        Save the trained model to disk.
        """
        if self.model is None:
            raise ValueError("Model must be trained before saving")

        self.model.save_model(model_path)
        self.log(f"\nModel saved as '{model_path}'")

    def run_full_pipeline(self):
        """
        Execute the complete training and evaluation pipeline.
        """
        try:
            self.log(f"Starting training pipeline - Log file: {self.log_file}")
            self.validate_data_files()
            self.prepare_data()
            self.train_model()
            self.evaluate_model()
            self.save_model()
            self.log(f"Pipeline completed successfully! Check {self.log_file} for full details.")
        except Exception as e:
            self.log(f"Error in pipeline: {e}")
            sys.exit(1)


def main():
    """
    Main function with command line argument parsing.
    """
    parser = argparse.ArgumentParser(
        description='Train and validate a CatBoost classifier for word sense disambiguation',
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
        default='word_sense_catboost_model.cbm',
        help='Output path for the trained model'
    )

    args = parser.parse_args()

    print("=" * 60)
    print("CatBoost Sense Disambiguation Classifier")
    print("=" * 60)
    print(f"Training file: {args.train_file}")
    print(f"Validation file: {args.val_file}")
    print(f"Model output: {args.model_output}")
    print("=" * 60)

    # Initialize and run the trainer
    trainer = CatBoostTrainer(args.train_file, args.val_file)
    trainer.run_full_pipeline()

    # Save with custom path if specified
    if args.model_output != 'word_sense_catboost_model.cbm':
        trainer.save_model(args.model_output)


if __name__ == "__main__":
    main()

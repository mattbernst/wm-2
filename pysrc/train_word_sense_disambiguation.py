import argparse
import logging
import sys
import time
from collections import Counter

import numpy as np
import pandas as pd
from catboost import CatBoostClassifier
from sklearn.metrics import *
from sklearn.model_selection import StratifiedKFold
from sklearn.utils.class_weight import compute_class_weight


class CatBoostTrainer:
    """
    A class to handle CatBoost model training and validation for word sense disambiguation.
    """

    def __init__(self, train_file, val_file, log_file=None, cv_folds=10):
        self.train_file = train_file
        self.val_file = val_file
        self.cv_folds = cv_folds
        self.feature_cols = ['commonness', 'relatedness', 'contextQuality']
        self.target_col = 'isCorrectSense'
        self.model = None
        self.train_df = None
        self.val_df = None
        self.cv_results = []

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

    def calculate_class_weights(self, y):
        """
        Calculate class weights automatically to handle imbalanced data.
        Returns both class_weights dict and scale_pos_weight value.
        """
        # Method 1: sklearn's compute_class_weight
        classes = np.unique(y)
        class_weights_array = compute_class_weight('balanced', classes=classes, y=y)
        class_weights_dict = dict(zip(classes, class_weights_array))

        # Method 2: Calculate scale_pos_weight for binary classification
        counter = Counter(y)
        neg_count = counter[False] if False in counter else counter[0]
        pos_count = counter[True] if True in counter else counter[1]
        scale_pos_weight = neg_count / pos_count

        self.log(f"Class distribution: {dict(counter)}")
        self.log(f"Calculated class weights: {class_weights_dict}")
        self.log(f"Scale pos weight: {scale_pos_weight:.4f}")

        return class_weights_dict, scale_pos_weight

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

        # Check required columns for training data
        train_required_cols = self.feature_cols + [self.target_col]
        missing_train_cols = [col for col in train_required_cols if col not in self.train_df.columns]
        if missing_train_cols:
            raise ValueError(f"Training file missing required columns: {missing_train_cols}")

        # Check required columns for validation data
        val_required_cols = self.feature_cols + [self.target_col]
        missing_val_cols = [col for col in val_required_cols if col not in self.val_df.columns]
        if missing_val_cols:
            raise ValueError(f"Validation file missing required columns: {missing_val_cols}")

        self.log(f"✓ Training data has all required columns: {train_required_cols}")
        self.log(f"✓ Validation data has all required columns: {val_required_cols}")

        # Check minimum row requirements
        min_rows = 10  # Minimum rows for meaningful training/validation
        if len(self.train_df) < min_rows:
            raise ValueError(f"Training data has insufficient rows: {len(self.train_df)} (minimum: {min_rows})")

        if len(self.val_df) < min_rows:
            raise ValueError(f"Validation data has insufficient rows: {len(self.val_df)} (minimum: {min_rows})")

        self.log(f"✓ Sufficient data rows - Training: {len(self.train_df)}, Validation: {len(self.val_df)}")

        # Check for missing values in critical columns
        train_missing = self.train_df[train_required_cols].isnull().sum()
        val_missing = self.val_df[val_required_cols].isnull().sum()

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

        # Prepare validation data
        self.X_val = self.val_df[self.feature_cols]
        self.y_val = self.val_df[self.target_col]

        # Calculate automatic class weights
        self.class_weights_dict, self.scale_pos_weight = self.calculate_class_weights(self.y_train)

        self.log(f"Training data shape: {self.train_df.shape}")
        self.log(f"Validation data shape: {self.val_df.shape}")
        self.log(f"Features: {self.feature_cols}")

    def perform_cross_validation(self):
        """
        Perform stratified k-fold cross-validation on the training data.
        """
        self.log(f"\n{'=' * 60}")
        self.log(f"PERFORMING {self.cv_folds}-FOLD CROSS-VALIDATION")
        self.log(f"{'=' * 60}")

        # Initialize stratified k-fold
        skf = StratifiedKFold(n_splits=self.cv_folds, shuffle=True, random_state=42)

        cv_scores = {
            'accuracy': [],
            'precision': [],
            'recall': [],
            'f1': [],
            'auc': []
        }

        fold_num = 1
        for train_idx, val_idx in skf.split(self.X_train, self.y_train):
            self.log(f"\nFold {fold_num}/{self.cv_folds}:")

            # Split data for this fold
            X_fold_train, X_fold_val = self.X_train.iloc[train_idx], self.X_train.iloc[val_idx]
            y_fold_train, y_fold_val = self.y_train.iloc[train_idx], self.y_train.iloc[val_idx]

            # Calculate class weights for this fold
            fold_class_weights, fold_scale_pos_weight = self.calculate_class_weights(y_fold_train)

            # Initialize model for this fold
            fold_model = CatBoostClassifier(
                iterations=1000,
                learning_rate=0.1,
                depth=6,
                loss_function='Logloss',
                eval_metric='AUC',
                random_seed=42,
                verbose=False,  # Reduce verbosity for CV
                early_stopping_rounds=50,
                scale_pos_weight=fold_scale_pos_weight  # Automatic class imbalance handling
            )

            # Train model
            fold_model.fit(
                X_fold_train,
                y_fold_train,
                eval_set=(X_fold_val, y_fold_val),
                plot=False,
                verbose=False
            )

            # Make predictions
            y_pred = fold_model.predict(X_fold_val)
            y_pred_proba = fold_model.predict_proba(X_fold_val)[:, 1]

            # Calculate metrics
            fold_accuracy = accuracy_score(y_fold_val, y_pred)
            fold_precision = precision_score(y_fold_val, y_pred)
            fold_recall = recall_score(y_fold_val, y_pred)
            fold_f1 = f1_score(y_fold_val, y_pred)
            fold_auc = roc_auc_score(y_fold_val, y_pred_proba)

            # Store results
            cv_scores['accuracy'].append(fold_accuracy)
            cv_scores['precision'].append(fold_precision)
            cv_scores['recall'].append(fold_recall)
            cv_scores['f1'].append(fold_f1)
            cv_scores['auc'].append(fold_auc)

            self.log(f"  Accuracy: {fold_accuracy:.4f}, Precision: {fold_precision:.4f}, "
                     f"Recall: {fold_recall:.4f}, F1: {fold_f1:.4f}, AUC: {fold_auc:.4f}")

            fold_num += 1

        # Calculate and display CV statistics
        self.log(f"\n{'=' * 60}")
        self.log("CROSS-VALIDATION RESULTS SUMMARY")
        self.log(f"{'=' * 60}")

        for metric, scores in cv_scores.items():
            mean_score = np.mean(scores)
            std_score = np.std(scores)
            self.log(f"{metric.upper()}: {mean_score:.4f} ± {std_score:.4f}")

        self.cv_results = cv_scores
        return cv_scores

    def train_model(self):
        """
        Initialize and train the CatBoost model on full training data.
        """
        self.log(f"\n{'=' * 60}")
        self.log("TRAINING FINAL MODEL ON FULL TRAINING DATA")
        self.log(f"{'=' * 60}")

        # Initialize CatBoost classifier with automatic class imbalance handling
        self.model = CatBoostClassifier(
            iterations=1000,
            learning_rate=0.1,
            depth=6,
            loss_function='Logloss',
            eval_metric='AUC',
            random_seed=42,
            verbose=100,
            early_stopping_rounds=50,
            scale_pos_weight=self.scale_pos_weight  # Automatic class imbalance handling
        )

        self.log("Training CatBoost model with automatic class imbalance handling...")
        self.log(f"Using scale_pos_weight: {self.scale_pos_weight:.4f}")

        # Train the model
        self.model.fit(
            self.X_train,
            self.y_train,
            eval_set=(self.X_val, self.y_val),
            plot=False
        )

        self.log("Model training completed!")

    def evaluate_model(self):
        """
        Evaluate the trained model on validation data and display results.
        """
        if self.model is None:
            raise ValueError("Model must be trained before evaluation")

        self.log(f"\n{'=' * 60}")
        self.log("FINAL MODEL VALIDATION RESULTS")
        self.log(f"{'=' * 60}")

        y_pred = self.model.predict(self.X_val)
        y_pred_proba = self.model.predict_proba(self.X_val)[:, 1]

        # Calculate metrics
        accuracy = accuracy_score(self.y_val, y_pred)
        precision = precision_score(self.y_val, y_pred)
        recall = recall_score(self.y_val, y_pred)
        f1 = f1_score(self.y_val, y_pred)
        auc = roc_auc_score(self.y_val, y_pred_proba)

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

        # Compare CV results with final validation
        if self.cv_results:
            self.log(f"\n{'=' * 60}")
            self.log("CROSS-VALIDATION VS FINAL VALIDATION COMPARISON")
            self.log(f"{'=' * 60}")
            cv_means = {metric: np.mean(scores) for metric, scores in self.cv_results.items()}
            final_scores = {'accuracy': accuracy, 'precision': precision, 'recall': recall, 'f1': f1, 'auc': auc}

            for metric in cv_means.keys():
                cv_score = cv_means[metric]
                final_score = final_scores[metric]
                diff = final_score - cv_score
                self.log(f"{metric.upper()}: CV={cv_score:.4f}, Final={final_score:.4f}, Diff={diff:+.4f}")

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
        Execute the complete training and evaluation pipeline with cross-validation.
        """
        try:
            self.log(f"Starting training pipeline with {self.cv_folds}-fold CV - Log file: {self.log_file}")
            self.validate_data_files()
            self.prepare_data()
            self.perform_cross_validation()
            self.train_model()
            self.evaluate_model()
            self.save_model()
            self.log(f"\nPipeline completed successfully! Check {self.log_file} for full details.")
        except Exception as e:
            self.log(f"Error in pipeline: {e}")
            sys.exit(1)


def main():
    """
    Main function with command line argument parsing.
    """
    parser = argparse.ArgumentParser(
        description='Train and validate a CatBoost classifier for word sense disambiguation with cross-validation',
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

    parser.add_argument(
        '--cv-folds',
        type=int,
        default=10,
        help='Number of cross-validation folds'
    )

    args = parser.parse_args()

    print("=" * 60)
    print("CatBoost Word Sense Disambiguation Classifier")
    print("=" * 60)
    print(f"Training file: {args.train_file}")
    print(f"Validation file: {args.val_file}")
    print(f"Model output: {args.model_output}")
    print(f"CV folds: {args.cv_folds}")
    print("=" * 60)

    # Initialize and run the trainer
    trainer = CatBoostTrainer(args.train_file, args.val_file, cv_folds=args.cv_folds)
    trainer.run_full_pipeline()

    # Save with custom path if specified
    if args.model_output != 'word_sense_catboost_model.cbm':
        trainer.save_model(args.model_output)


if __name__ == "__main__":
    main()

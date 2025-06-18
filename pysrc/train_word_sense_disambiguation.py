import argparse
import logging
import sys
import time
import os

import numpy as np
import pandas as pd
from catboost import CatBoostRanker
from sklearn.metrics import *
from sklearn.model_selection import GroupKFold


def find_common_prefix(train_file, val_file):
    """
    Find the common prefix between training and validation file names.
    Returns the prefix that can be used for generating the model filename.
    """
    # Get just the filenames without the directory path
    train_basename = os.path.basename(train_file)
    val_basename = os.path.basename(val_file)

    # Remove file extensions
    train_name = os.path.splitext(train_basename)[0]
    val_name = os.path.splitext(val_basename)[0]

    # Find common prefix
    common_prefix = ""
    min_length = min(len(train_name), len(val_name))

    for i in range(min_length):
        if train_name[i] == val_name[i]:
            common_prefix += train_name[i]
        else:
            break

    # Clean up the prefix - remove trailing underscores or hyphens
    common_prefix = common_prefix.rstrip('_-')

    # If no meaningful prefix found, use a default
    if len(common_prefix) < 3:
        common_prefix = "model"

    return common_prefix


class CatBoostRankerTrainer:
    """
    A class to handle CatBoost ranking model training and validation for word sense disambiguation.
    """

    def __init__(self, train_file, val_file, log_file=None, cv_folds=5, model_output=None):
        self.train_file = train_file
        self.val_file = val_file
        self.cv_folds = cv_folds
        self.feature_cols = ['commonness', 'inLinkVectorMeasure', 'outLinkVectorMeasure',
                             'inLinkGoogleMeasure', 'outLinkGoogleMeasure', 'contextQuality']
        self.target_col = 'isCorrectSense'
        self.group_col = 'linkDestination'
        self.model = None
        self.train_df = None
        self.val_df = None
        self.cv_results = []

        # Generate model output filename if not provided
        if model_output is None:
            prefix = find_common_prefix(train_file, val_file)
            self.model_output = f"{prefix}_word_sense_ranker.cbm"
        else:
            self.model_output = model_output

        # Set up logging
        if log_file is None:
            timestamp = int(time.time())
            log_file = f"model_ranking_run_{timestamp}.log"

        self.log_file = log_file
        self.setup_logging()

    def setup_logging(self):
        """
        Set up logging to capture all console output to a file.
        """
        # Create logger
        self.logger = logging.getLogger('CatBoostRankerTrainer')
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

    def create_group_ids(self, df):
        """
        Create group IDs based on assumption that consecutive rows with
        same linkDestination belong to same ranking group.
        """
        group_ids = []
        current_group_id = 0
        prev_link_dest = None

        for _, row in df.iterrows():
            link_dest = row[self.group_col]

            if link_dest != prev_link_dest:
                if prev_link_dest is not None:
                    current_group_id += 1

            group_ids.append(current_group_id)
            prev_link_dest = link_dest

        return group_ids

    def analyze_group_structure(self, df, group_ids):
        """
        Analyze the group structure to understand the ranking problem.
        """
        df_with_groups = df.copy()
        df_with_groups['group_id'] = group_ids

        self.log("Group structure analysis:")

        # Count groups per linkDestination
        link_dest_groups = df_with_groups.groupby(self.group_col)['group_id'].nunique()
        self.log(f"Number of unique linkDestinations: {len(link_dest_groups)}")
        self.log(
            f"Groups per linkDestination: min={link_dest_groups.min()}, max={link_dest_groups.max()}, mean={link_dest_groups.mean():.2f}")

        # Analyze group sizes
        group_sizes = df_with_groups.groupby('group_id').size()
        self.log(f"Group sizes: min={group_sizes.min()}, max={group_sizes.max()}, mean={group_sizes.mean():.2f}")

        # Check for groups with correct answers
        groups_with_correct = df_with_groups.groupby('group_id')[self.target_col].sum()
        groups_without_correct = (groups_with_correct == 0).sum()
        self.log(f"Groups without correct answer: {groups_without_correct} out of {len(groups_with_correct)}")

        # Show some example groups
        self.log("\nExample groups:")
        for i, (group_id, group_data) in enumerate(df_with_groups.groupby('group_id')):
            if i >= 3:  # Show first 3 groups
                break
            self.log(f"Group {group_id} (linkDestination: {group_data[self.group_col].iloc[0]}):")
            self.log(f"  Size: {len(group_data)}")
            self.log(f"  Correct answers: {group_data[self.target_col].sum()}")
            self.log(f"  Target values: {group_data[self.target_col].tolist()}")

        return df_with_groups

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
        train_required_cols = self.feature_cols + [self.target_col, self.group_col]
        missing_train_cols = [col for col in train_required_cols if col not in self.train_df.columns]
        if missing_train_cols:
            raise ValueError(f"Training file missing required columns: {missing_train_cols}")

        # Check required columns for validation data
        val_required_cols = self.feature_cols + [self.target_col, self.group_col]
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

        self.log("Data validation completed successfully!\n")

    def prepare_data(self):
        """
        Prepare training and validation data for ranking.
        """
        self.log("Preparing data for ranking...")

        # Create group IDs
        self.train_group_ids = self.create_group_ids(self.train_df)
        self.val_group_ids = self.create_group_ids(self.val_df)

        # Analyze group structure
        self.log("\nTraining data group analysis:")
        self.train_df_with_groups = self.analyze_group_structure(self.train_df, self.train_group_ids)

        self.log("\nValidation data group analysis:")
        self.val_df_with_groups = self.analyze_group_structure(self.val_df, self.val_group_ids)

        # Prepare features and targets
        self.X_train = self.train_df[self.feature_cols]
        self.y_train = self.train_df[self.target_col].astype(int)  # Convert boolean to int for ranking

        self.X_val = self.val_df[self.feature_cols]
        self.y_val = self.val_df[self.target_col].astype(int)

        self.log(f"Training data shape: {self.train_df.shape}")
        self.log(f"Validation data shape: {self.val_df.shape}")
        self.log(f"Number of training groups: {len(set(self.train_group_ids))}")
        self.log(f"Number of validation groups: {len(set(self.val_group_ids))}")
        self.log(f"Features: {self.feature_cols}")

    def calculate_ranking_metrics(self, y_true, y_pred, group_ids):
        """
        Calculate ranking-specific metrics.
        """
        # Group predictions and true labels
        groups = {}
        for i, group_id in enumerate(group_ids):
            if group_id not in groups:
                groups[group_id] = {'y_true': [], 'y_pred': []}
            groups[group_id]['y_true'].append(y_true.iloc[i] if hasattr(y_true, 'iloc') else y_true[i])
            groups[group_id]['y_pred'].append(y_pred[i])

        # Calculate metrics
        correct_predictions = 0
        total_groups = 0
        ndcg_scores = []

        for group_id, group_data in groups.items():
            y_true_group = np.array(group_data['y_true'])
            y_pred_group = np.array(group_data['y_pred'])

            # Check if this group has any correct answers
            if np.sum(y_true_group) == 0:
                continue

            total_groups += 1

            # Accuracy: check if highest predicted is actually correct
            highest_pred_idx = np.argmax(y_pred_group)
            if y_true_group[highest_pred_idx] == 1:
                correct_predictions += 1

            # Calculate NDCG for this group
            if len(y_true_group) > 1:
                try:
                    ndcg = ndcg_score([y_true_group], [y_pred_group])
                    ndcg_scores.append(ndcg)
                except:
                    pass  # Skip if NDCG calculation fails

        accuracy = correct_predictions / total_groups if total_groups > 0 else 0
        mean_ndcg = np.mean(ndcg_scores) if ndcg_scores else 0

        return {
            'accuracy': accuracy,
            'ndcg': mean_ndcg,
            'total_groups': total_groups,
            'correct_predictions': correct_predictions
        }

    def perform_cross_validation(self):
        """
        Perform group k-fold cross-validation on the training data.
        """
        self.log(f"\n{'=' * 60}")
        self.log(f"PERFORMING {self.cv_folds}-FOLD GROUP CROSS-VALIDATION")
        self.log(f"{'=' * 60}")

        # Initialize group k-fold (ensures same linkDestination doesn't appear in both train and val)
        gkf = GroupKFold(n_splits=self.cv_folds)

        # Create groups for CV based on linkDestination
        cv_groups = self.train_df[self.group_col].values

        cv_scores = {
            'accuracy': [],
            'ndcg': []
        }

        fold_num = 1
        for train_idx, val_idx in gkf.split(self.X_train, self.y_train, groups=cv_groups):
            self.log(f"\nFold {fold_num}/{self.cv_folds}:")

            # Split data for this fold
            X_fold_train, X_fold_val = self.X_train.iloc[train_idx], self.X_train.iloc[val_idx]
            y_fold_train, y_fold_val = self.y_train.iloc[train_idx], self.y_train.iloc[val_idx]

            # Create group IDs for this fold
            fold_train_groups = [self.train_group_ids[i] for i in train_idx]
            fold_val_groups = [self.train_group_ids[i] for i in val_idx]

            # Remap group IDs to be contiguous
            unique_train_groups = sorted(set(fold_train_groups))
            group_mapping = {old_id: new_id for new_id, old_id in enumerate(unique_train_groups)}
            fold_train_groups = [group_mapping[gid] for gid in fold_train_groups]

            unique_val_groups = sorted(set(fold_val_groups))
            group_mapping_val = {old_id: new_id for new_id, old_id in enumerate(unique_val_groups)}
            fold_val_groups = [group_mapping_val[gid] for gid in fold_val_groups]

            # Initialize model for this fold
            fold_model = CatBoostRanker(
                iterations=500,
                learning_rate=0.1,
                depth=6,
                loss_function='YetiRank',
                l2_leaf_reg=3.0,  # Add L2 regularization
                random_seed=1,
                verbose=False,
                early_stopping_rounds=50
            )

            # Train model
            fold_model.fit(
                X_fold_train,
                y_fold_train,
                group_id=fold_train_groups,
                plot=False,
                verbose=False
            )

            # Make predictions
            y_pred = fold_model.predict(X_fold_val)

            # Calculate ranking metrics
            metrics = self.calculate_ranking_metrics(y_fold_val, y_pred, fold_val_groups)

            cv_scores['accuracy'].append(metrics['accuracy'])
            cv_scores['ndcg'].append(metrics['ndcg'])

            self.log(f"  Accuracy: {metrics['accuracy']:.4f}, NDCG: {metrics['ndcg']:.4f}")
            self.log(f"  Groups: {metrics['total_groups']}, Correct: {metrics['correct_predictions']}")

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
        Initialize and train the CatBoost ranking model on full training data.
        """
        self.log(f"\n{'=' * 60}")
        self.log("TRAINING FINAL RANKING MODEL ON FULL TRAINING DATA")
        self.log(f"{'=' * 60}")

        # Initialize CatBoost ranker
        self.model = CatBoostRanker(
            iterations=1000,
            learning_rate=0.1,
            depth=6,
            loss_function='YetiRank',  # Good for ranking tasks
            l2_leaf_reg=3.0,  # Increased L2 regularization to prevent overfitting
            random_seed=1,
            verbose=100,
            early_stopping_rounds=50
        )

        self.log("Training CatBoost ranking model...")

        # Train the model
        self.model.fit(
            self.X_train,
            self.y_train,
            group_id=self.train_group_ids,
            plot=False
        )

        self.log("Model training completed!")

    def evaluate_model(self):
        """
        Evaluate the trained ranking model on validation data and display results.
        """
        if self.model is None:
            raise ValueError("Model must be trained before evaluation")

        self.log(f"\n{'=' * 60}")
        self.log("FINAL RANKING MODEL VALIDATION RESULTS")
        self.log(f"{'=' * 60}")

        y_pred = self.model.predict(self.X_val)

        # Calculate ranking metrics
        metrics = self.calculate_ranking_metrics(self.y_val, y_pred, self.val_group_ids)

        self.log(f"Ranking Accuracy: {metrics['accuracy']:.4f}")
        self.log(f"NDCG Score:      {metrics['ndcg']:.4f}")
        self.log(f"Total Groups:    {metrics['total_groups']}")
        self.log(f"Correct Predictions: {metrics['correct_predictions']}")

        # Feature importance
        self.log("\nFeature Importance:")
        try:
            # For ranking models, we need to create a Pool object
            from catboost import Pool
            train_pool = Pool(
                data=self.X_train,
                label=self.y_train,
                group_id=self.train_group_ids
            )
            feature_importance = self.model.get_feature_importance(
                data=train_pool,
                type='LossFunctionChange'
            )
            for i, feature in enumerate(self.feature_cols):
                self.log(f"{feature}: {feature_importance[i]:.4f}")
        except Exception as e:
            self.log(f"Could not calculate LossFunctionChange importance: {e}")
            try:
                # Fallback to PredictionValuesChange which doesn't require training data
                feature_importance = self.model.get_feature_importance(type='PredictionValuesChange')
                for i, feature in enumerate(self.feature_cols):
                    self.log(f"{feature}: {feature_importance[i]:.4f}")
            except Exception as e2:
                self.log(f"Could not calculate feature importance: {e2}")

        # Show example predictions for first few groups
        self.log("\nExample predictions for first 3 validation groups:")
        val_df_with_pred = self.val_df.copy()
        val_df_with_pred['group_id'] = self.val_group_ids
        val_df_with_pred['prediction'] = y_pred

        for i, (group_id, group_data) in enumerate(val_df_with_pred.groupby('group_id')):
            if i >= 3:
                break

            self.log(f"\nGroup {group_id} (linkDestination: {group_data[self.group_col].iloc[0]}):")
            sorted_group = group_data.sort_values('prediction', ascending=False)

            for j, (_, row) in enumerate(sorted_group.iterrows()):
                correct_marker = "✓" if row[self.target_col] else "✗"
                self.log(f"  Rank {j + 1}: Score={row['prediction']:.4f} {correct_marker} "
                         f"(Actual: {row[self.target_col]})")

        # Compare CV results with final validation
        if self.cv_results:
            self.log(f"\n{'=' * 60}")
            self.log("CROSS-VALIDATION VS FINAL VALIDATION COMPARISON")
            self.log(f"{'=' * 60}")
            cv_means = {metric: np.mean(scores) for metric, scores in self.cv_results.items()}
            final_scores = {'accuracy': metrics['accuracy'], 'ndcg': metrics['ndcg']}

            for metric in cv_means.keys():
                cv_score = cv_means[metric]
                final_score = final_scores[metric]
                diff = final_score - cv_score
                self.log(f"{metric.upper()}: CV={cv_score:.4f}, Final={final_score:.4f}, Diff={diff:+.4f}")

    def save_model(self, model_path=None):
        """
        Save the trained model to disk.
        """
        if self.model is None:
            raise ValueError("Model must be trained before saving")

        # Use the auto-generated filename if no path is provided
        if model_path is None:
            model_path = self.model_output

        self.model.save_model(model_path)
        self.log(f"\nModel saved as '{model_path}'")

    def run_full_pipeline(self):
        """
        Execute the complete training and evaluation pipeline with cross-validation.
        """
        try:
            self.log(f"Starting ranking pipeline with {self.cv_folds}-fold CV - Log file: {self.log_file}")
            self.log(f"Model will be saved as: {self.model_output}")
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
        description='Train and validate a CatBoost ranking model for word sense disambiguation with cross-validation',
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
        help='Number of cross-validation folds (reduced from 10 due to group constraints)'
    )

    args = parser.parse_args()

    # Generate default model output name if not provided
    if args.model_output is None:
        prefix = find_common_prefix(args.train_file, args.val_file)
        args.model_output = f"{prefix}_word_sense_ranker.cbm"

    print("=" * 60)
    print("CatBoost Word Sense Disambiguation Ranking Model")
    print("=" * 60)
    print(f"Training file: {args.train_file}")
    print(f"Validation file: {args.val_file}")
    print(f"Model output: {args.model_output}")
    print(f"CV folds: {args.cv_folds}")
    print("=" * 60)

    # Initialize and run the trainer
    trainer = CatBoostRankerTrainer(args.train_file, args.val_file, cv_folds=args.cv_folds, model_output=args.model_output)
    trainer.run_full_pipeline()


if __name__ == "__main__":
    main()

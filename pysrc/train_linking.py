#!/usr/bin/env python3
"""
CatBoost Link Validity Predictor

This script trains a CatBoost model to predict link validity (isValidLink)
from numerical features in CSV data using cross-validation.
"""

import argparse
import os
import warnings

import numpy as np
import pandas as pd
from catboost import CatBoostClassifier
from sklearn.metrics import f1_score, roc_auc_score, classification_report, confusion_matrix
from sklearn.model_selection import cross_val_score, StratifiedKFold

warnings.filterwarnings('ignore')


def get_prefix(file_path):
    """
    Extract prefix from file path for naming output files.

    Args:
        file_path (str): Path to the input file

    Returns:
        str: Prefix for output files
    """
    base_name = os.path.basename(file_path)
    return os.path.splitext(base_name)[0]


def load_and_prepare_data(file_path):
    """
    Load CSV data and prepare features and target.

    Args:
        file_path (str): Path to the CSV file

    Returns:
        tuple: (features_df, target_series)
    """
    print(f"Loading data from: {file_path}")

    # Load the data
    df = pd.read_csv(file_path)

    # Check if the target column exists
    if 'isValidLink' not in df.columns:
        raise ValueError(f"Target column 'isValidLink' not found in {file_path}")

    # Separate features and target
    feature_columns = [col for col in df.columns if col != 'isValidLink']
    X = df[feature_columns]
    y = df['isValidLink']

    print(f"Data shape: {df.shape}")
    print(f"Features: {feature_columns}")
    print(f"Target distribution:")
    print(y.value_counts())
    print(f"Target distribution (%):")
    print(y.value_counts(normalize=True) * 100)

    return X, y


def perform_cross_validation(X, y, cv_folds=5):
    """
    Perform cross-validation with CatBoost model.

    Args:
        X (pd.DataFrame): Features
        y (pd.Series): Target
        cv_folds (int): Number of cross-validation folds

    Returns:
        tuple: (f1_scores, auc_scores, trained_model)
    """
    print(f"\nPerforming {cv_folds}-fold cross-validation...")

    # Initialize CatBoost classifier
    model = CatBoostClassifier(
        iterations=1000,
        learning_rate=0.1,
        depth=6,
        loss_function='Logloss',
        eval_metric='F1',
        random_seed=1,
        verbose=False,
        early_stopping_rounds=100
    )

    # Set up cross-validation
    cv = StratifiedKFold(n_splits=cv_folds, shuffle=True, random_state=42)

    # Perform cross-validation for F1 score
    f1_scores = cross_val_score(model, X, y, cv=cv, scoring='f1')

    # Perform cross-validation for AUC score
    auc_scores = cross_val_score(model, X, y, cv=cv, scoring='roc_auc')

    # Train final model on full dataset
    model.fit(X, y)

    return f1_scores, auc_scores, model


def evaluate_model(model, X_val, y_val):
    """
    Evaluate the trained model on validation data.

    Args:
        model: Trained CatBoost model
        X_val (pd.DataFrame): Validation features
        y_val (pd.Series): Validation target

    Returns:
        dict: Evaluation metrics
    """
    print("\nEvaluating on validation data...")

    # Make predictions
    y_pred = model.predict(X_val)
    y_pred_proba = model.predict_proba(X_val)[:, 1]

    # Calculate metrics
    f1 = f1_score(y_val, y_pred)
    auc = roc_auc_score(y_val, y_pred_proba)

    # Print detailed results
    print(f"Validation F1 Score: {f1:.4f}")
    print(f"Validation AUC Score: {auc:.4f}")
    print("\nClassification Report:")
    print(classification_report(y_val, y_pred))
    # Conservative predictions (higher precision, lower recall)
    print("\nConservative predictions")
    conservative_preds = (model.predict_proba(X_val)[:, 1] >= 0.8).astype(int)
    print(classification_report(y_val, conservative_preds))
    print("\nConfusion Matrix:")
    print(confusion_matrix(y_val, y_pred))

    return {
        'f1': f1,
        'auc': auc,
        'predictions': y_pred,
        'probabilities': y_pred_proba
    }


def print_feature_importance(model, feature_names):
    """
    Print feature importance from the trained model.

    Args:
        model: Trained CatBoost model
        feature_names (list): List of feature names
    """
    print("\nFeature Importance:")
    print("-" * 40)

    # Get feature importances
    importances = model.get_feature_importance()

    # Create feature importance dataframe
    feature_importance_df = pd.DataFrame({
        'feature': feature_names,
        'importance': importances
    }).sort_values('importance', ascending=False)

    for _, row in feature_importance_df.iterrows():
        print(f"{row['feature']:<30}: {row['importance']:.4f}")


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

    args = parser.parse_args()

    # Generate default model output name if not provided
    if args.model_output is None:
        prefix = get_prefix(args.train_file)
        args.model_output = f"{prefix}_link_validity.cbm"

    print("=" * 60)
    print("CatBoost Link Validity Model")
    print("=" * 60)
    print(f"Training file: {args.train_file}")
    print(f"Validation file: {args.val_file}")
    print(f"Model output: {args.model_output}")
    print(f"CV folds: {args.cv_folds}")
    print("=" * 60)

    try:
        # Load training data
        X_train, y_train = load_and_prepare_data(args.train_file)

        # Load validation data
        X_val, y_val = load_and_prepare_data(args.val_file)

        # Ensure feature consistency between train and validation
        if list(X_train.columns) != list(X_val.columns):
            raise ValueError("Feature columns don't match between training and validation data")

        # Perform cross-validation
        f1_scores, auc_scores, trained_model = perform_cross_validation(
            X_train, y_train, args.cv_folds
        )

        # Print cross-validation results
        print("\n" + "=" * 60)
        print("CROSS-VALIDATION RESULTS")
        print("=" * 60)
        print(f"F1 Scores: {f1_scores}")
        print(f"Mean F1: {np.mean(f1_scores):.4f} (+/- {np.std(f1_scores) * 2:.4f})")
        print(f"AUC Scores: {auc_scores}")
        print(f"Mean AUC: {np.mean(auc_scores):.4f} (+/- {np.std(auc_scores) * 2:.4f})")

        # Evaluate on validation data
        print("\n" + "=" * 60)
        print("VALIDATION RESULTS")
        print("=" * 60)
        val_metrics = evaluate_model(trained_model, X_val, y_val)

        # Print feature importance
        print_feature_importance(trained_model, X_train.columns.tolist())

        # Save the trained model
        trained_model.save_model(args.model_output)
        print(f"\nModel saved to: {args.model_output}")

        # Summary
        print("\n" + "=" * 60)
        print("SUMMARY")
        print("=" * 60)
        print(f"Cross-validation F1: {np.mean(f1_scores):.4f} (+/- {np.std(f1_scores) * 2:.4f})")
        print(f"Cross-validation AUC: {np.mean(auc_scores):.4f} (+/- {np.std(auc_scores) * 2:.4f})")
        print(f"Validation F1: {val_metrics['f1']:.4f}")
        print(f"Validation AUC: {val_metrics['auc']:.4f}")

    except Exception as e:
        print(f"Error: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())

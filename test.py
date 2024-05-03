import pyspark.sql.functions as F
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from sklearn.metrics import roc_auc_score, f1_score, recall_score, precision_score
from deepctr.models import DeepFM
from deepctr.feature_column import SparseFeat, DenseFeat, get_feature_names
from util import subsample_stratified, time_series_split, get_metrix, get_product_summary


class Experiment:
    def __init__(self, spark, train_tbl_name, validation_tbl_name, label_col, product_col, zero_ratio, at_least, k,
                 initial_seed):
        self.spark = spark
        self.train_tbl_name = train_tbl_name
        self.validation_tbl_name = validation_tbl_name
        self.label_col = label_col
        self.product_col = product_col
        self.zero_ratio = zero_ratio
        self.at_least = at_least
        self.k = k
        self.initial_seed = initial_seed
        self.models = []

class Experiment:
    def __init__(self, spark, train_tbl_name, validation_tbl_name, label_col, product_col, zero_ratio, at_least, k,
                 initial_seed):
        self.spark = spark
        self.train_tbl_name = train_tbl_name
        self.validation_tbl_name = validation_tbl_name
        self.label_col = label_col
        self.product_col = product_col
        self.zero_ratio = zero_ratio
        self.at_least = at_least
        self.k = k
        self.initial_seed = initial_seed
        self.models = []

    def train_models(self):
        for seed in range(self.initial_seed, self.initial_seed + self.k):
            subsampled_data = subsample_stratified(self.spark, self.train_tbl_name, self.label_col, self.product_col,
                                                    self.zero_ratio, self.at_least, seed)
            model = self.train_model(subsampled_data)
            self.models.append(model)

    def train_model(self, data):
        # Load data into a DataFrame
        df = data.toPandas()

        # Preprocessing: Drop columns not in use
        drop_columns = ['business_date', 'open_date', 'one_year_before_open_date']
        df = df.drop(drop_columns, axis=1)

        # Feature Engineering: Identify sparse and dense features
        sparse_features = []
        dense_features = []
        for col in df.columns:
            if col != self.label_col and df[col].dtype == 'object':
                sparse_features.append(col)
            elif col != self.label_col:
                dense_features.append(col)

        # Preprocessing: Convert data types and fill missing values
        df[sparse_features] = df[sparse_features].astype(str)
        df[dense_features] = df[dense_features].astype(float)
        df[sparse_features] = df[sparse_features].fillna('-1')
        df[dense_features] = df[dense_features].fillna(0)

        # Preprocessing: Encode categorical features
        for feat in sparse_features:
            df[feat] = df[feat].astype('category').cat.codes

        # Split features and target
        X = df.drop(self.label_col, axis=1)
        y = df[self.label_col]

        # Define feature columns for DeepFM
        feature_columns = [SparseFeat(feat, vocabulary_size=X[feat].max() + 1, embedding_dim=4)
                           for feat in sparse_features] + [DenseFeat(feat, 1) for feat in dense_features]

        # Build DeepFM model
        model = DeepFM(feature_columns, task='binary')

        # Compile model
        model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy'])

        # Train model
        model.fit(X, y, batch_size=256, epochs=10, validation_split=0.2)

        return model

    def evaluate_models(self):
        for i, model in enumerate(self.models):
            validation_data = self.spark.sql(f"SELECT * FROM {self.validation_tbl_name}")
            metrics = self.evaluate_model(model, validation_data)
            print(f"Metrics for Model {i + 1}: {metrics}")

    def evaluate_model(self, model, data):
        df = data.toPandas()

        # Preprocess data if necessary
        # ...

        # Predictions
        y_pred = model.predict(df.drop(self.label_col, axis=1))

        # Calculate evaluation metrics
        metrics = {
            "AUC": roc_auc_score(df[self.label_col], y_pred),
            "Precision": precision_score(df[self.label_col], y_pred),
            "Recall": recall_score(df[self.label_col], y_pred),
            "F1": f1_score(df[self.label_col], y_pred)
        }

        return metrics

    def compare_evaluations(self):
        # Compare evaluation metrics of all models
        # Example: Calculate mean, standard deviation, or any other relevant statistic
        pass

    def validate_models(self):
        for model in self.models:
            validate_data = self.spark.sql(f"SELECT * FROM {self.validation_tbl_name}")
            metrics = self.evaluate_model(model, validate_data)
            print(f"Metrics for Validation: {metrics}")

    def product_level_summary(self):
        test_data = self.spark.sql(f"SELECT * FROM {self.validation_tbl_name}")
        df = test_data.toPandas()

        for model_num, model in enumerate(self.models):
            product_list = df[self.product_col].unique()
            for product in product_list:
                product_df = df[df[self.product_col] == product]
                predictions = model.predict(product_df.drop(self.label_col, axis=1))
                product_df['prob'] = predictions
                get_product_summary(product_list, product_df, self.label_col,
                                    f'product_level_summary_model_{model_num}_{product}.csv')

    def time_series_validate_models(self):
        for model in self.models:
            validation_data = self.spark.sql(f"SELECT * FROM {self.validation_tbl_name}")
            df = validation_data.toPandas()
            train, test = time_series_split(df, df["business_date"], test_size=0.2)
            metrics = self.evaluate_model(model, test)
            print(f"Metrics for Time Series Validation: {metrics}")

# Usage example:
spark = SparkSession.builder.master("local").appName("Experiment").getOrCreate()
experiment = Experiment(spark, "sb_dsp.recommendation_train_data_v2", "sb_dsp.recommendation_validation_data_v1",
                        "Flag", "Segmt_Prod_Type", 100, 0, 3, 123)
experiment.train_models()
experiment.evaluate_models()
experiment.compare_evaluations()
experiment.validate_models()
experiment.product_level_summary()
experiment.time_series_validate_models()



def train_model(self, data, validation=None):
    # Load data into a DataFrame
    df = data.toPandas()

    # Preprocessing: Drop columns not in use
    drop_columns = ['business_date', 'open_date', 'one_year_before_open_date']
    df = df.drop(drop_columns, axis=1)

    # Feature Engineering: Identify sparse and dense features
    sparse_features = []
    dense_features = []
    for col in df.columns:
        if col != self.label_col and df[col].dtype == 'object':
            sparse_features.append(col)
        elif col != self.label_col:
            dense_features.append(col)

    # Preprocessing: Convert data types and fill missing values
    df[sparse_features] = df[sparse_features].astype(str)
    df[dense_features] = df[dense_features].astype(float)
    df[sparse_features] = df[sparse_features].fillna('-1')
    df[dense_features] = df[dense_features].fillna(0)

    # Preprocessing: Encode categorical features
    for feat in sparse_features:
        df[feat] = df[feat].astype('category').cat.codes

    # Split features and target
    X = df.drop(self.label_col, axis=1)
    y = df[self.label_col]

    # Define feature columns for DeepFM
    feature_columns = [SparseFeat(feat, vocabulary_size=X[feat].max() + 1, embedding_dim=4)
                       for feat in sparse_features] + [DenseFeat(feat, 1) for feat in dense_features]

    # Build DeepFM model
    model = DeepFM(feature_columns, task='binary')

    # Compile model
    model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy'])

    if validation is None:
        # Train model on whole data
        model.fit(X, y, batch_size=256, epochs=10, validation_split=0.2)
        # Evaluate model on whole data
        print("Metrics on whole data:")
        self.evaluate_model(model, X, y)
    elif 0 < validation < 1:
        # Split data into train and validation sets
        train_size = int((1 - validation) * len(df))
        X_train, X_val = X[:train_size], X[train_size:]
        y_train, y_val = y[:train_size], y[train_size:]
        # Train model on train data
        model.fit(X_train, y_train, batch_size=256, epochs=10, validation_data=(X_val, y_val))
        # Evaluate model on validation data
        print("Metrics on validation data:")
        self.evaluate_model(model, X_val, y_val)
    else:
        raise ValueError("Validation parameter must be None or a value between 0 and 1")

    return model

def evaluate_model(self, model, X, y):
    # Perform predictions
    pred_prob = model.predict(X)
    pred_labels = (pred_prob >= 0.5).astype(int)

    # Compute evaluation metrics
    auc = roc_auc_score(y, pred_prob)
    precision = precision_score(y, pred_labels)
    recall = recall_score(y, pred_labels)
    f1 = f1_score(y, pred_labels)

    # Print metrics
    print("AUC:", auc)
    print("Precision:", precision)
    print("Recall:", recall)
    print("F1 Score:", f1)



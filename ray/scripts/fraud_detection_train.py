import ray
import os
import boto3
import botocore
import json
from time import strftime, gmtime

import tensorflow as tf

import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.utils import class_weight
import pickle
from pathlib import Path


@ray.remote
def tf_train():
    aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    endpoint_url = os.environ.get('AWS_S3_ENDPOINT')
    region_name = os.environ.get('AWS_DEFAULT_REGION')
    bucket_name = os.environ.get('AWS_S3_BUCKET')

    session = boto3.session.Session(aws_access_key_id=aws_access_key_id,
                                    aws_secret_access_key=aws_secret_access_key)


    s3_resource = session.resource(
        's3',
        config=botocore.client.Config(signature_version='s3v4'),
        endpoint_url=endpoint_url,
        region_name=region_name)

    bucket = s3_resource.Bucket(bucket_name)

    def download_file(file_key, local_path):
        bucket.download_file(file_key, local_path)
        return local_path


    def list_objects(prefix):
        filter = bucket.objects.filter(Prefix=prefix)
        keys = [obj.key for obj in filter.all()]

        return keys

    data_file_key = "data/card_transdata.csv"
    local_data_file = "card_transdata.csv"

    print(list_objects(""))

    download_file(data_file_key, local_data_file)

    assert os.path.exists(local_data_file)

    print("TensorFlow version:", tf.__version__)

    data = pd.read_csv(local_data_file)
    head = data.head()

    # Set the input (X) and output (Y) data.
    # The only output data is whether it's fraudulent. All other fields are inputs to the model.

    X = data.drop(columns = ['repeat_retailer','distance_from_home', 'fraud'])
    y = data['fraud']

    # Split the data into training and testing sets so you have something to test the trained model with.

    # X_train, X_test, y_train, y_test = train_test_split(X,y, test_size = 0.2, stratify = y)
    X_train, X_test, y_train, y_test = train_test_split(X,y, test_size = 0.2, shuffle = False)

    X_train, X_val, y_train, y_val = train_test_split(X_train,y_train, test_size = 0.2, stratify = y_train)

    # Scale the data to remove mean and have unit variance. The data will be between -1 and 1, which makes it a lot easier for the model to learn than random (and potentially large) values.
    # It is important to only fit the scaler to the training data, otherwise you are leaking information about the global distribution of variables (which is influenced by the test set) into the training set.

    scaler = StandardScaler()

    X_train = scaler.fit_transform(X_train.values)

    # Since the dataset is unbalanced (it has many more non-fraud transactions than fraudulent ones), set a class weight to weight the few fraudulent transactions higher than the many non-fraud transactions.
    class_weights = class_weight.compute_class_weight('balanced',classes = np.unique(y_train),y = y_train)
    class_weights = {i : class_weights[i] for i in range(len(class_weights))}


    from keras.models import Sequential
    from keras.layers import Dense, Dropout, BatchNormalization, Activation
    from ray import train
    from ray.train import Checkpoint
    from ray.train.tensorflow import TensorflowTrainer
    from ray.train.tensorflow.keras import ReportCheckpointCallback
    from ray.air.config import ScalingConfig
    import tensorflow as tf
    import tempfile
    import os

    def build_model():
        model = Sequential()
        model.add(Dense(32, activation = 'relu', input_dim = len(X.columns)))
        model.add(Dropout(0.2))
        model.add(Dense(32))
        model.add(BatchNormalization())
        model.add(Activation('relu'))
        model.add(Dropout(0.2))
        model.add(Dense(32))
        model.add(BatchNormalization())
        model.add(Activation('relu'))
        model.add(Dropout(0.2))
        model.add(Dense(1, activation = 'sigmoid'))
        return model

    def train_model(config: dict):
        batch_size = config.get("batch_size", 32)
        batch_size = batch_size * train.get_context().get_world_size()
        epochs = config.get("epochs", 2)

        # Build the model
        strategy = tf.distribute.MultiWorkerMirroredStrategy()
        with strategy.scope():
            model = build_model()
            model.compile(optimizer='adam',loss='binary_crossentropy',metrics=['accuracy'])
            model.summary()

        # Get the dataset
        dataset = train.get_dataset_shard("train")
        tf_dataset = dataset.to_tf(
            feature_columns="x", label_columns="y", batch_size=batch_size
        )

        # Train the model
        results = []
        history = model.fit(
            tf_dataset,
            epochs=epochs,
            callbacks=[ReportCheckpointCallback()]
        )
        results.append(history.history)

        # Push results to S3
        s3 = boto3.client(
            's3', endpoint_url=s3_endpoint_url,
            aws_access_key_id=s3_access_key, aws_secret_access_key=s3_secret_key,
        )
        with tempfile.TemporaryDirectory() as temp_checkpoint_dir:
            model_location = os.path.join(temp_checkpoint_dir, "model.keras")
            model.save(model_location)
            s3.upload_file(model_location, s3_bucket_name, 'model.keras')

        return results

    # Run everything distributed
    config = {"batch_size": 32, "epochs": 2}
    reshaped_dataset = [{"x": X_train[i], "y":y_train.values[i]} for i in range(len(X_train))]
    train_dataset = ray.data.from_items(reshaped_dataset)
    scaling_config = ScalingConfig(num_workers=2, use_gpu=False)

    trainer = TensorflowTrainer(
        train_loop_per_worker=train_model,
        train_loop_config=config,
        scaling_config=scaling_config,
        datasets={"train": train_dataset},
    )

    result = trainer.fit()

    return result


# Automatically connect to the running Ray cluster.
ray.init()
result = ray.get(tf_train.remote())
# print(result)

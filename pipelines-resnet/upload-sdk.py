# %%
import kfp
from kfp.components import create_component_from_func, InputPath, OutputPath
from kubernetes.client import *
from kubernetes.client.models import *
import os


def upload_model():
    import os
    import boto3
    import botocore
    import json
    from datetime import datetime

    aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    endpoint_url = os.environ.get('AWS_S3_ENDPOINT')
    region_name = os.environ.get('AWS_DEFAULT_REGION')
    bucket_name = os.environ.get('AWS_S3_BUCKET')

    s3_prefix = 'data/upload-test'

    session = boto3.session.Session(aws_access_key_id=aws_access_key_id,
                                    aws_secret_access_key=aws_secret_access_key)

    s3_resource = session.resource(
        's3',
        config=botocore.client.Config(signature_version='s3v4'),
        endpoint_url=endpoint_url,
        region_name=region_name)

    bucket = s3_resource.Bucket(bucket_name)

    date_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    data = {
        "date": date_str
    }
    filename = f"{date_str}.json"
    path = f"/tmp/{filename}"
    with open(path, "w") as write_file:
        json.dump(data, write_file)

    bucket.upload_file(path, f"{s3_prefix}/{filename}")


upload_model_component = create_component_from_func(
    upload_model,
    base_image="quay.io/modh/runtime-images:runtime-datascience-ubi9-python-3.9-2023a-20230725",
    packages_to_install=["boto3", "botocore"]
)


@kfp.dsl.pipeline(name="upload_sdk_pipeline")
def sdk_pipeline():
    upload_model_task = upload_model_component()

    upload_model_task.container.add_env_from(
        V1EnvFromSource(
            secret_ref=V1SecretReference(
                name="aws-connection-my-storage"
            )
        )
    )

    upload_model_task_2 = upload_model_component()

    for name in ["AWS_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_S3_ENDPOINT",
                 "AWS_DEFAULT_REGION", "AWS_S3_BUCKET"]:
        upload_model_task_2.container.add_env_variable(
            V1EnvVar(
                name=name,
                value_from=V1EnvVarSource(
                    secret_key_ref=V1SecretKeySelector(
                        name="aws-connection-my-storage", key=name
                    )
                ),
            )
        )


from kfp_tekton.compiler import TektonCompiler

os.environ["DEFAULT_STORAGE_CLASS"] = os.environ.get(
    "DEFAULT_STORAGE_CLASS", "gp3"
)
os.environ["DEFAULT_ACCESSMODES"] = os.environ.get(
    "DEFAULT_ACCESSMODES", "ReadWriteOnce"
)
TektonCompiler().compile(sdk_pipeline, __file__.replace(".py", ".yaml"))

# %%
import kfp
from kfp.components import create_component_from_func, InputPath, OutputPath
from kubernetes.client import *
from kubernetes.client.models import *
import os


def train_model(output_path: OutputPath()):
    import urllib.request
    url = "https://rhods-public.s3.amazonaws.com/demo-models/resnet50-onnx/1/resnet50-caffe2-v1-9.onnx"
    # output = "resnet50-caffe2-v1-9.onnx"
    urllib.request.urlretrieve(url, output_path)



train_model_component = create_component_from_func(
    train_model,
    base_image="quay.io/modh/runtime-images:runtime-datascience-ubi9-python-3.9-2023a-20230725"
)


def upload_model(input_path: InputPath()):
    import os
    import boto3
    import botocore

    aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    endpoint_url = os.environ.get('AWS_S3_ENDPOINT')
    region_name = os.environ.get('AWS_DEFAULT_REGION')
    bucket_name = os.environ.get('AWS_S3_BUCKET')

    model_s3_prefix = 'models'

    session = boto3.session.Session(aws_access_key_id=aws_access_key_id,
                                    aws_secret_access_key=aws_secret_access_key)

    s3_resource = session.resource(
        's3',
        config=botocore.client.Config(signature_version='s3v4'),
        endpoint_url=endpoint_url,
        region_name=region_name)

    bucket = s3_resource.Bucket(bucket_name)
    bucket.upload_file(input_path, f"{model_s3_prefix}/resnet50-caffe2-v1-9.onnx")


upload_model_component = create_component_from_func(
    upload_model,
    base_image="quay.io/modh/runtime-images:runtime-datascience-ubi9-python-3.9-2023a-20230725",
    packages_to_install=["boto3", "botocore"]
)


@kfp.dsl.pipeline(name="train_upload_pipeline")
def sdk_pipeline():
    train_model_task = train_model_component()
    onnx_file = train_model_task.output
    upload_model_task = upload_model_component(onnx_file)

    upload_model_task.container.add_env_from(
        V1EnvFromSource(
            secret_ref=V1SecretReference(
                name="aws-connection-my-storage"
            )
        )
    )

    # Add envVars one by one
    # for name in ["AWS_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_S3_ENDPOINT",
    #              "AWS_DEFAULT_REGION", "AWS_S3_BUCKET"]:
    #     upload_model_task.container.add_env_variable(
    #         V1EnvVar(
    #             name=name,
    #             value_from=V1EnvVarSource(
    #                 secret_key_ref=V1SecretKeySelector(
    #                     name="aws-connection-my-storage", key=name
    #                 )
    #             ),
    #         )
    #     )



from kfp_tekton.compiler import TektonCompiler

os.environ["DEFAULT_STORAGE_CLASS"] = os.environ.get(
    "DEFAULT_STORAGE_CLASS", "gp3"
)
os.environ["DEFAULT_ACCESSMODES"] = os.environ.get(
    "DEFAULT_ACCESSMODES", "ReadWriteOnce"
)
TektonCompiler().compile(sdk_pipeline, __file__.replace(".py", ".yaml"))

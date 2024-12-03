""" top level run script """
import s3fs
from urllib.parse import urlparse

import json
import logging
import os
import re
import sys
import time
from glob import glob
from pathlib import Path
from typing import List, Tuple, Union
import shutil

from aind_codeocean_api.codeocean import CodeOceanClient
from aind_codeocean_api.models.data_assets_requests import (
    CreateDataAssetRequest, Source, Sources)
from utils import utils

PathLike = Union[str, Path]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s : %(message)s",
    datefmt="%Y-%m-%d %H:%M",
    handlers=[
        logging.StreamHandler(),
        # logging.FileHandler("test.log", "a"),
    ],
)
logger = logging.getLogger(__name__)

PIPELINE_VERSION = "2.0.2" # TODO
def get_data_config(
    data_folder: PathLike,
    data_description_path: str = "data_description.json",
) -> Tuple:
    """
    Returns the first behavior dataset found
    in the data folder

    Parameters
    -----------
    data_folder: str
        Path to the folder that contains the data

    data_description_path: str
        Path for the data description

    Returns
    -----------
    Tuple[str, list]
        Str: Empty string if the processing manifest
        was not found

        List: Empty list if no investigators in data description
    """
    # Returning first behavior dataset found
    # Doing this because of Code Ocean, ideally we would have
    # a single dataset in the pipeline

    data_description_path = Path(f"{data_folder}/{data_description_path}")

    if not data_description_path.exists():
        raise ValueError(
            f"Please, check data description path: {data_description_path}"
        )

    data_description_dict = utils.read_json_as_dict(str(data_description_path))

    behavior_dataset = data_description_dict["name"]
    investigators = data_description_dict["investigators"]

    return behavior_dataset, investigators


def create_derived_videoprocessed_metadata(
    data_folder: PathLike, results_folder: PathLike, logger: logging.Logger
) -> Tuple[PathLike, str]:
    """
    Creates the derived metadata following
    AIND conventions.

    Parameters
    ----------
    data_folder: PathLike
        Path to the code ocean data folder

    results_folder: PathLike
        Path to the code ocean results folder

    logger: logging.Logger
        Logging object

    Returns
    -------
    Tuple[PathLike, str]
        The first position of the tuple
        corresponds to the path where the
        metadata was created while the
        second position has the new name
        of the dataset
    """
    logger.info("Generating derived data description")
    raw_metadata_path = data_folder.joinpath("input_aind_metadata")
    output_dispatch_metadata = f"{results_folder}/output_aind_metadata"
    utils.create_folder(output_dispatch_metadata)

    new_dataset_name = utils.generate_data_description(
        raw_data_description_path=raw_metadata_path.joinpath("data_description.json"),
        dest_data_description=output_dispatch_metadata,
        process_name="videoprocessed",
    )

    logger.info("Copying all available raw behavior metadata")

    # This is the AIND metadata
    found_metadata = utils.copy_available_metadata(
        input_path=raw_metadata_path,
        output_path=output_dispatch_metadata,
        files_to_copy=[
            "acquisition.json",
            "instrument.json",
            "subject.json",
            "procedures.json",
            "session.json",
        ],
    )

    logger.info(f"Copied metadata from {raw_metadata_path}: {found_metadata}")
    logger.info(
        f"Metadata in raw folder {raw_metadata_path}: {os.listdir(raw_metadata_path)}"
    )
    logger.info(
        f"Metadata in folder {output_dispatch_metadata}: {os.listdir(output_dispatch_metadata)}"
    )

    return output_dispatch_metadata, new_dataset_name


def copy_intermediate_data(
    output_dispatch_metadata: PathLike,
    pred_folders: List[PathLike],
    eval_folders: List[PathLike],
    new_dataset_name: str,
    bucket_path: str,
    results_folder: PathLike,
    logger: logging.Logger,
) -> str:
    """
    Copies the prediction and evaluation metadata
    to the destination bucket to make it available
    to scientists as soon as possible.

    Parameters
    ----------
    output_dispatch_metadata: PathLike
        Path where the new metadata (derived)
        for the processed dataset is located

    pred_folders: List[PathLike]
        Video prediction folders generated in the
        prediction step.

    eval_folders: List[PathLike]
        Evaluation folders generated in the
        evaluation step.

    new_dataset_name: str
        New dataset name where the data will
        be copied following the aind conventions
        e.g., s3://{bucket_path}/{new_dataset_name}

    bucket_path: str
        S3 path where the data will be moved.
        Do not include 's3://' since this is
        automatically added.

    results_folder: PathLike
        Results folder path in Code Ocean

    logger: logging.Logger
        Logging object

    Returns
    -------
    Tuple[str, str]
        The first position is the path where the dataset
        was moved. e.g., s3://{bucket_path}/{new_dataset_name}
        It includes the "s3://" prefix. 
        e.g., s3://{bucket_path}/{new_dataset_name}/{output_prediction}
    """

    pred_processings = []
    eval_processings = []

    for pred_folder in pred_folders:
        processing_jsons = [
            p
            for p in glob(f"{pred_folder}/*processing*.json")
            if "manifest" not in str(p)
        ]
        pred_processings.append(processing_jsons)

    for eval_folder in eval_folders:
        processing_jsons = [
            p
            for p in glob(f"{eval_folder}/*processing*.json")
            if "manifest" not in str(p)
        ]
        eval_processings.append(processing_jsons)

    # Flattening list
    processing_paths = list()
    combined_processing_list = pred_processings + eval_processings
    for sub_list in combined_processing_list:
        processing_paths += sub_list
    logger.info(f"Processing paths: {processing_paths}")

    try:
        output_filename = utils.compile_processing_jsons(
            processing_paths=processing_paths,
            output_general_processing=output_dispatch_metadata,
            processor_full_name="Di Wang",
            pipeline_version=PIPELINE_VERSION,
        )

    except Exception as e:
        print(f"Error while compiling processing manifests: {e}")
        output_filename = None

    logger.info(f"Compiled processing.json in path {output_filename}")

    s3_path = f"s3://{bucket_path}/{new_dataset_name}"
    logger.info(f"Copy files to path {s3_path}")

    # Copying derived metadata
    output_dispatch_metadata = Path(output_dispatch_metadata)
    print(f"output_dispatch_metadata: {output_dispatch_metadata}")

    qc_file = "../data/quality_control.json" # TODO
    shutil.copy(qc_file, f"{output_dispatch_metadata}/quality_control.json")


    #======================#
    fs = s3fs.S3FileSystem()
    url = urlparse(s3_path)
    logger.info("url:", url)

    if url.scheme != "s3":
        raise NotImplementedError("Only s3 output_uri is supported, not {url.scheme}")
        
    file_names = [
            "processing.json",
            "quality_control.json",
            "data_description.json",
            "procedures.json",
            "subject.json",
            ]

    for file in file_names:
        print(f"uploading ../results/output_aind_metadata/{file}")
        fs.put(
            f"../results/output_aind_metadata/{file}", url.netloc + url.path.rstrip("/") + "/", recursive=True, maxdepth=10
        ) 

    file_names = [
        "eval_outputs/eval_outputs",
        "pred_outputs",
        ]

    for file in file_names:
        print(f"uploading data/{file}")
        fs.put(
            f"../data/{file}", url.netloc + url.path.rstrip("/") + "/", recursive=True, maxdepth=10
        ) 

    utils.save_string_to_txt(
        f"Video prediction and evaluation dataset saved in: {s3_path}",
        f"{results_folder}/output_video_pred_evaluation.txt",
    )

    return s3_path
def wait_for_data_availability(
    co_client,
    data_asset_id: str,
    timeout_seconds: int = 300,
    pause_interval=10,
):
    """
    There is a lag between when a register data request is made and when the
    data is available to be used in a capsule.
    Parameters
    ----------
    data_asset_id : str
    timeout_seconds : int
        Roughly how long the method should check if the data is available.
    pause_interval : int
        How many seconds between when the backend is queried.

    Returns
    -------
    requests.Response

    """
    num_of_checks = 0
    break_flag = False
    time.sleep(pause_interval)
    response = co_client.get_data_asset(data_asset_id)

    if ((pause_interval * num_of_checks) > timeout_seconds) or (
        response.status_code == 200
    ):
        break_flag = True
    while not break_flag:
        time.sleep(pause_interval)
        response = co_client.get_data_asset(data_asset_id)
        num_of_checks += 1
        if ((pause_interval * num_of_checks) > timeout_seconds) or (
            response.status_code == 200
        ):
            break_flag = True
    return response

def make_data_viewable(co_client: CodeOceanClient, response_contents: dict):
    """
    Makes a registered dataset viewable

    Parameters
    ----------
    co_client: CodeOceanClient
        Code ocean client

    response_contents: dict
        Dictionary with the response
        of the created data asset

    """
    data_asset_id = response_contents["id"]
    response_data_available = wait_for_data_availability(co_client, data_asset_id)

    if response_data_available.status_code != 200:
        logger.info(f"Unable to find: {data_asset_id}")
        return

    # Make data asset viewable to everyone
    update_data_perm_response = co_client.update_permissions(
        data_asset_id=data_asset_id, everyone="viewer"
    )
    logger.info(f"Data asset viewable to everyone: {update_data_perm_response}")

def dispatch(s3_path: str, results_folder: PathLike, bucket: str):
    """
    Register s3 bucket

    Parameters
    ----------
    s3_path: str
        Path to the s3 path

    results_folder: str
        Path pointing to the results folder

    bucket: str
        Bucket name where the data is stored
    """

    logger.info(f"Provided s3_path: {s3_path}")

    # codeocean_domain = os.environ["CODEOCEAN_DOMAIN"]
    codeocean_domain="https://codeocean.allenneuraldynamics.org/"
    co_token = os.environ["CUSTOM_KEY"]
    co_client = CodeOceanClient(domain=codeocean_domain, token=co_token)

    # Getting path in S3
    dataset_to_register = s3_path

    for modality in ["ecephys", "behavior"]:
        pattern = (
            modality + r"_\d+_\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}"
            r"_videoprocessed_\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}"
        )
        logger.info(f"pattern: {pattern}")
        found_pattern = re.findall(pattern=pattern, string=dataset_to_register)
        logger.info(f"found_pattern: {found_pattern}")

        # Extract the data asset info
        if len(found_pattern)>0:
            dataset_to_register = found_pattern[0]
            logger.info(f"dataset_to_register: {dataset_to_register}")

            video_pred_eval_tags = [modality, "behavior-videos", "processed", "derived"] # TODO: add subject_id?
            logger.info(f"video_pred_eval_tags: {video_pred_eval_tags}")


            # Registering AWS data asset
            aws_source = Sources.AWS(
                bucket=bucket,
                prefix=dataset_to_register,
                keep_on_external_storage=True,
                public=True,
            )
            source = Source(aws=aws_source)

            create_data_asset_request = CreateDataAssetRequest(
                name=dataset_to_register,
                tags=video_pred_eval_tags,
                mount=dataset_to_register,
                source=source,
                custom_metadata=None,
            )

            input_json_data = json.loads(create_data_asset_request.json_string)

            # Register the behavior video processed dataset
            try:
                data_asset_reg_response = co_client.create_data_asset(
                    request=input_json_data
                )

                response_contents = data_asset_reg_response.json()
                logger.info(f"Created data asset in Code Ocean: {response_contents}")

                # Making the created data asset available for everyone
                make_data_viewable(co_client, response_contents)

            except Exception as e:
                logger.warning(f"Error registering data asset in the API call. Error: {e}")

        else:
            logger.warning("Error registering data asset")



def run():
    """ basic run function """
    # Absolute paths of common Code Ocean folders
    data_folder = Path(os.path.abspath("../data"))
    results_folder = Path(os.path.abspath("../results"))

    # It is assumed that these files
    # will be in the data folder
    required_input_elements = [
        f"{data_folder}/input_aind_metadata/data_description.json",
    ]

    missing_files = utils.validate_capsule_inputs(required_input_elements)

    if len(missing_files):
        raise ValueError(
            f"We miss the following files in the capsule input: {missing_files}"
        )

    dataset_name, investigators = get_data_config(
            data_folder=data_folder,
            data_description_path="input_aind_metadata/data_description.json",
        )
    logger.info(f"dataset_name: {dataset_name}")

    # Creating new metadata for videoprocessed dataset
    output_dispatch_metadata, new_dataset_name = create_derived_videoprocessed_metadata(
        data_folder=data_folder, results_folder=results_folder, logger=logger
    )

    # get prediction and evaluation outputs
    pred_folders = glob(f"{data_folder}/pred_outputs")
    eval_folders = glob(f"{data_folder}/eval_outputs/eval_outputs") # TODO

    bucket_path = "aind-open-data"

    logger.info(f"bucket_path: {bucket_path}")
    logger.info(f"new_dataset_name: {new_dataset_name}")
    logger.info(f"output_dispatch_metadata: {output_dispatch_metadata}")
    logger.info(f"pred_folders: {pred_folders}")
    logger.info(f"eval_folders: {eval_folders}")
    logger.info(f"results_folder: {results_folder}")

    s3_path = copy_intermediate_data(
        output_dispatch_metadata=output_dispatch_metadata,
        pred_folders=pred_folders,
        eval_folders=eval_folders,
        new_dataset_name=new_dataset_name,
        bucket_path=bucket_path,
        results_folder=results_folder,
        logger=logger,
    )

    logger.info(f"s3_path: {s3_path}")

    dispatch(
        s3_path=s3_path,
        results_folder=results_folder,
        bucket=bucket_path,
    )

if __name__ == "__main__": run()
"""
This is the actual file handler that manages the updating of the metadata files

"""
from typing import List
from datetime import datetime
import time

import rasterio

from .utils.settings import settings
from .utils.supabase_client import use_client, login
from .utils.metadata_models import FileUploadMetadata, StatusEnum
from .resample import resample

__ACCESS_TOKEN = None
__REFRESH_TOKEN = None
__ISSUED_AT = None


def authenticate_processor():
    # use the global TOKENS
    global __ACCESS_TOKEN
    global __REFRESH_TOKEN
    global __ISSUED_AT
    
    # if there is no access token
    if __ACCESS_TOKEN is None or __REFRESH_TOKEN is None:    
        # login to the supabase backend to retrieve an access token
        auth = login(settings.processor_username, settings.processor_password)

        # upadate
        __ACCESS_TOKEN = auth.session.access_token
        __REFRESH_TOKEN = auth.session.refresh_token
        __ISSUED_AT = datetime.now()

    # check if we need to refresh the token
    elif (datetime.now() - __ISSUED_AT).total_seconds() > 60 * 45:  # 45 minutes
        with use_client(__ACCESS_TOKEN) as client:
            auth = client.auth.refresh_session(__REFRESH_TOKEN)

            # update the tokens
            __ACCESS_TOKEN = auth.session.access_token
            __REFRESH_TOKEN = auth.session.refresh_token
            __ISSUED_AT = datetime.now()
    
    else:
        # there is nothing to do
        pass


def list_pending_uuids() -> List[str]:
    # we can load these without authentication
    with use_client(None) as client:
        response = client.table(settings.metadata_table).select("uuid").eq("status", StatusEnum.pending.value).execute()
    
    # extract the uudis from the retunred data
    uuids = [row['uuid'] for row in response.data]
    return uuids


def get_metadata(uuid: str) -> FileUploadMetadata:
    # we need authenticated access
    authenticate_processor()

    # use the client to load the data
    with use_client(__ACCESS_TOKEN) as client:
        # load the metadata row
        response = client.table(settings.metadata_table).select("*").eq("uuid", uuid).single().execute()
        metadata = FileUploadMetadata(**response.data)
    
    return metadata


def preprocess_file(uuid: str) -> FileUploadMetadata:
    # get the current metadata for the given uuid
    metadata = get_metadata(uuid=uuid)

    # update the status to processing
    with use_client(__ACCESS_TOKEN) as client:
        client.table(settings.metadata_table).update({"status": StatusEnum.processing.value}).eq("uuid", uuid).execute()
        metadata.status = StatusEnum.processing.value
    
    # start the resample method
    t1 = time.time()
    try:
        # first build the output path
        output_path = settings.processed_path / metadata.file_id
        resample(metadata.target_path, str(output_path))
    except Exception as e:
        # update the status to errored
        metadata.status = StatusEnum.errored
        
    finally:
        t2 = time.time()
        metadata.compress_time = t2 - t1

    # try to open the processed file
    if not metadata.status == StatusEnum.errored:
        try:
            with rasterio.open(str(output_path)) as src:
                metadata.bbox = src.bounds
        except Exception as e:
            metadata.status = StatusEnum.errored

    # update the metadata
    with use_client(__ACCESS_TOKEN) as client:
        client.table(settings.metadata_table).update(metadata.model_dump()).eq("uuid", uuid).execute()
    
    return metadata

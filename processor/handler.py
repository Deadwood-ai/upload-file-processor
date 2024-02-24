"""
This is the actual file handler that manages the updating of the metadata files

"""
import time
from tempfile import NamedTemporaryFile

from .utils.settings import settings
from .metadata import get_metadata
from .auth import supabase_client
from .utils.metadata_models import FileUploadMetadata, StatusEnum
from .resample import resample
from .files import put_file, fetch_file, archive_file
from .logger import logger



def preprocess_file(uuid: str) -> FileUploadMetadata:
    # get the current metadata for the given uuid
    metadata = get_metadata(uuid=uuid)

    # update the status to processing
    with supabase_client() as client:
        client.table(settings.metadata_table).update({"status": StatusEnum.processing.value}).eq("uuid", uuid).execute()
        metadata.status = StatusEnum.processing.value
    
    # START - resampling
    t1 = time.time()
    try:
        # get the file
        with NamedTemporaryFile() as target_path:
            with fetch_file(metadata) as src_file:
                # resample the file
                bbox = resample(src_file, target_path.name)

                # update the metadata
                metadata.bbox = bbox
            
            # put the file to the right location
            put_file(metadata, target_path.name)
        
    except Exception as e:
        logger.error(str(e))
        # update the status to errored
        metadata.status = StatusEnum.errored
        
    finally:
        t2 = time.time()
        metadata.compress_time = t2 - t1
    # FINISH - resampling
        
    # START - copy the file
    try:
        archive_file(metadata)
    except Exception as e:
        logger.error(str(e))
    

    # update the metadata
    with supabase_client() as client:
        client.table(settings.metadata_table).update(metadata.model_dump()).eq("uuid", uuid).execute()
    logger.info(f"Finished processing {uuid} in {metadata.compress_time} seconds.")
    
    return metadata

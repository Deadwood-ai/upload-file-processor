"""
This is the actual file handler that manages the updating of the metadata files

"""
import time
from tempfile import NamedTemporaryFile
from concurrent.futures import ThreadPoolExecutor

from .utils.settings import settings
from .metadata import get_metadata, list_pending_uuids
from .auth import supabase_client
from .utils.metadata_models import FileUploadMetadata, StatusEnum
from .utils.settings import settings
from .resample import resample
from .mapserver import create_wms_source
from .files import put_processed_raster, fetch_raw_raster, archive_raster
from .logger import logger


def dispatch_pending_files(wait: bool = True):
    """
    Load a list of all pending files on the server

    The create a new Thread for each file and start the preprocessing
    """
    # get the list of pending files
    uuids = list_pending_uuids()
    
    # start a new thread for each file
    with ThreadPoolExecutor() as executor:
        # map the uuids to the preprocessing function
        executor.map(preprocess_file, uuids)

        # wait for all threads to finish
        executor.shutdown(wait=wait)


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
            with fetch_raw_raster(metadata) as src_file:
                # resample the file
                bbox = resample(
                    src_file,
                    target_path.name,
                    scale_factor=settings.scale_factor,
                    compress=settings.processor_compression,
                    jpeg_quality=settings.compression_quality
                )

                # update the metadata
                metadata.bbox = bbox
            
            # put the file to the right location
            processed_path = put_processed_raster(metadata, target_path.name)
            metadata.processed_path = processed_path
        
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
        archive_raster(metadata)
    except Exception as e:
        logger.error(str(e))
        metadata.status = StatusEnum.errored
    # END - copy the file
        
    # START - create the WMS source
    try:
        wms_url = create_wms_source(metadata=metadata)
        metadata.wms_source = wms_url
    except Exception as e:
        logger.error(str(e))
        metadata.status = StatusEnum.errored
    # END - create the WMS source
        
    # finally set the flag to processed
    metadata.status = StatusEnum.processed
    logger.debug(f"Final metadata state: {metadata}")

    # update the metadata
    updates = {
        "status": metadata.status.value,
        "compress_time": metadata.compress_time,
        "processed_path": metadata.processed_path,
        "wms_source": metadata.wms_source,
        "bbox": f"BOX({metadata.bbox.bottom} {metadata.bbox.left}, {metadata.bbox.top} {metadata.bbox.right})"
    }
    logger.debug(f"Updates sent to backend: {updates}")
    
    with supabase_client() as client:
        client.table(settings.metadata_table).update(updates).eq("uuid", uuid).execute()
    logger.info(f"Finished processing {uuid} in {metadata.compress_time} seconds.")
    
    return metadata

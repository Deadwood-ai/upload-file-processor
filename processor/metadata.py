from typing import List

from .utils.settings import settings
from .utils.metadata_models import FileUploadMetadata, StatusEnum
from .auth import supabase_client

def list_pending_uuids() -> List[str]:
    # we can load these without authentication
    with supabase_client() as client:
        response = client.table(settings.metadata_table).select("uuid").eq("status", StatusEnum.pending.value).execute()
    
    # extract the uudis from the retunred data
    uuids = [row['uuid'] for row in response.data]
    return uuids


def get_metadata(uuid: str) -> FileUploadMetadata:
    # use the client to load the data
    with supabase_client() as client:
        # load the metadata row
        response = client.table(settings.metadata_table).select("*").eq("uuid", uuid).single().execute()
        metadata = FileUploadMetadata(**response.data)
    
    return metadata

"""
This submodule makes sure to fetch and put the correct files.

The processor can run on with the files locally available, but also fetch the files
via SFTP from the storage server. This is abstracted into a submodule to add more 
connections, like S3, later.

"""
from pathlib import Path
from tempfile import NamedTemporaryFile
from contextlib import contextmanager
import shutil

from fabric import Connection

from .utils.settings import settings
from .utils.metadata_models import FileUploadMetadata

@contextmanager
def fetch_file(metadata: FileUploadMetadata) -> str:
    """
    Check if the file is locally available, then return the path to the file.
    If not, fetch the file into a temporary path and return that
    """
    # check if the file is locally available
    if Path(metadata.target_path).exists():
        # we are local
        return metadata.target_path
    
    # otherwise we need to ssh SFTP the file form the server
    with Connection(f"{settings.ssh_user}@{settings.ssh_host}") as c:
        with NamedTemporaryFile(delete=False) as tmp:
            c.get(metadata.target_path, tmp.name)

    try:
        yield tmp.name
    finally:
        shutil.rmtree(tmp.name)


def put_file(metadata: FileUploadMetadata, local_path: Path) -> str:
    """
    Put the file into the correct location. Check if the referenced 
    path from the metadata is local to the processor, then copy locally.
    Otherwise put the file via SFTP.
    """
    # create the needed target path
    target_path = Path(settings.processed_path) / metadata.file_id

    # derive the assusmed path, if we are local
    if Path(settings.target_path).exists():
        # we are local
        shutil.copy(local_path, str(target_path))

    # otherwise we need to ssh SFTP the file from the server
    else:
        with Connection(f"{settings.ssh_user}@{settings.ssh_host}") as c:
            c.put(local_path, str(target_path))

    return str(target_path)


def archive_file(metadata: FileUploadMetadata) -> str:
    """
    Copy the file in the specified archive location and remove from the
    raw upload location.

    """
    # build the archive path
    archive_path = settings.archive_path / metadata.file_id

    # check if the target path exists locally
    if Path(metadata.target_path).exists():
        # we are local
        shutil.copy(metadata.target_path, str(archive_path))
    
    else:
        with Connection(f"{settings.ssh_user}@{settings.ssh_host}") as c:
            c.run(f"mv {metadata.target_path} {archive_path}")
    
    return str(archive_path)
            
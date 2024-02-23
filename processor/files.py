"""
This submodule makes sure to fetch and put the correct files.

The processor can run on with the files locally available, but also fetch the files
via SFTP from the storage server. This is abstracted into a submodule to add more 
connections, like S3, later.

"""
from pathlib import Path
from tempfile import NamedTemporaryFile

from paramiko import SSHClient, SFTPClient

from .utils.settings import settings
from .utils.metadata_models import FileUploadMetadata


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
    with SSHClient().connect(settings.ssh_host, username=settings.ssh_user, password=settings.ssh_password):
        with SFTPClient() as sftp:
            with NamedTemporaryFile(delete=False) as tmp:
                sftp.get(metadata.target_path, tmp.name)

    return tmp.name


def put_file(metadata: FileUploadMetadata, local_path: Path) -> str:
    """
    Put the file into the correct location. Check if the referenced 
    path from the metadata is local to the processor, then copy locally.
    Otherwise put the file via SFTP.
    """
    # check if the file is locally available
    pass

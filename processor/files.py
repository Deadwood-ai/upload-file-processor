"""
This submodule makes sure to fetch and put the correct files.

The processor can run on with the files locally available, but also fetch the files
via SFTP from the storage server. This is abstracted into a submodule to add more 
connections, like S3, later.

"""
from typing import Literal
from typing import Generator
from pathlib import Path
from tempfile import NamedTemporaryFile
from contextlib import contextmanager
import shutil

from fabric import Connection
import mappyfile

from .utils.settings import settings
from .utils.metadata_models import FileUploadMetadata


@contextmanager
def ssh_connect(server: Literal['storage'] | Literal['mapserver'] = 'storage') -> Generator[Connection, None, None]:
    # check which server to connect to
    if server == 'storage':
        serv = f"{settings.storage_ssh_user}@{settings.storage_ssh_host}"
        args = dict(password=settings.storage_ssh_password)
    elif server == 'mapserver':
        serv = f"{settings.mapserver_ssh_user}@{settings.mapserver_ssh_host}"
        args = dict(password=settings.mapserver_ssh_password)
    try:
        connection = Connection(serv, connect_kwargs=args)
        yield connection
    finally:
        connection.close()


def file_exists(c: Connection, path: str) -> bool:
    """
    Check if a file exists on the server
    """
    try:
        c.run(f"ls -ls {path}", hide=True)
        return True
    except Exception:
        return False
    

@contextmanager
def fetch_raw_raster(metadata: FileUploadMetadata) -> Generator[str, None, None]:
    """
    Check if the file is locally available, then return the path to the file.
    If not, fetch the file into a temporary path and return that
    """
    # check if the file is locally available
    if settings.storage_local:
        # we are local
        try:
            yield metadata.raw_path
        finally:
            pass
    else:
        # otherwise we need to ssh SFTP the file form the server
        with ssh_connect() as c:
            with NamedTemporaryFile(delete=False) as tmp:
                c.get(metadata.raw_path, tmp.name)

        try:
            yield tmp.name
        finally:
            shutil.rmtree(tmp.name)


@contextmanager
def fetch_processed_raster(metadata: FileUploadMetadata) -> Generator[str, None, None]:
    """
    Check if the file is locally available, then return the path to the file.
    If not, fetch the file into a temporary path and return that
    """
    # check if the file is locally available
    if settings.mapserver_local:
        # we are local
        try:
            yield metadata.processed_path
        finally:
            pass
    else:
        # otherwise we need to ssh SFTP the file form the server
        with ssh_connect(server='mapserver') as c:
            with NamedTemporaryFile(delete=False) as tmp:
                c.get(metadata.processed_path, tmp.name)

        try:
            yield tmp.name
        finally:
            shutil.rmtree(tmp.name)

def put_processed_raster(metadata: FileUploadMetadata, local_path: Path) -> str:
    """
    Put the file into the correct location. Check if the referenced 
    path from the metadata is local to the processor, then copy locally.
    Otherwise put the file via SFTP.
    """
    # create the needed target path
    target_path = Path(settings.processed_path) / metadata.file_id

    # derive the assusmed path, if we are local
    if settings.mapserver_local:
        # we are local
        shutil.copy(local_path, str(target_path))

    # otherwise we need to ssh SFTP the file from the server
    else:
        with ssh_connect(server='mapserver') as c:
            c.put(local_path, str(target_path))

    return str(target_path)


def archive_raster(metadata: FileUploadMetadata) -> str:
    """
    Copy the file in the specified archive location and remove from the
    raw upload location.

    """
    # build the archive path
    archive_path = settings.archive_path / metadata.file_id

    # check if the target path exists locally
    if settings.storage_local:
        # we are local
        shutil.copy(metadata.raw_path, str(archive_path))
    
    else:
        with ssh_connect() as c:
            c.run(f"mv {metadata.raw_path} {archive_path}")
    
    return str(archive_path)


def get_mapfile() -> dict:
    # check if the MAINFILE exists locally
    if settings.mapserver_local:
        if (settings.mapfile_path / "MAINFILE.map").exists():
            with open(settings.mapfile_path / "MAINFILE.map") as f:
                mappy = mappyfile.load(f)
        else:
            with open(Path(__file__).parent / "MAINFILE.map", "r") as f:
                mappy = mappyfile.load(f)

                # inject the correct wms online_resource path
                mappy["web"]["metadata"]["wms_onlineresource"] = settings.ows_base_url
    
    # use the mapserver 
    else:
        with ssh_connect(server='mapserver') as c:
            # check if it exists
            if not file_exists(c, str(settings.mapfile_path / "MAINFILE.map")):
                with open(Path(__file__).parent / "MAINFILE.map", "r") as f:
                    mappy = mappyfile.load(f)

                    # inject the correct wms online_resource path
                    mappy["web"]["metadata"]["wms_onlineresource"] = settings.ows_base_url
            else:
                with NamedTemporaryFile() as tmp:
                    c.get(str(settings.mapfile_path / "MAINFILE.map"), tmp.name)
                    with open(tmp.name, "r") as f:
                        mappy = mappyfile.load(f)
                    
    # return the object
    return mappy


def put_mapfile(mappy: dict) -> str:
    # check if the MAINFILE exists locally
    local_path = settings.mapfile_path / "MAINFILE.map"
    if settings.mapserver_local:
        mappyfile.save(mappy, str(local_path))
        return str(local_path)
    else:
        with ssh_connect(server='mapserver') as c:
            with NamedTemporaryFile() as tmp:
                mappyfile.save(mappy, tmp.name)
                c.put(tmp.name, str(settings.mapfile_path / "MAINFILE.map"))
                
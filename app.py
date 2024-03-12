from typing import Literal, Optional
from concurrent.futures import ThreadPoolExecutor

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn

from processor.metadata import list_pending_uuids
from processor.handler import dispatch_pending_files, preprocess_file
from processor.utils.settings import settings
from processor.logger import logger


app = FastAPI(
    title="Deadwood AI upload preprocessor",
    description="This is a simplistic API around two preprocessing entrypoints: `/dispatch/all` and `/dispatch/{uuid}`",
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# define a number of data models
class SupabaseWebhookPayload(BaseModel):
    type: Literal['INSERT']
    table: str
    schema: str
    record: dict
    old_record: Optional[dict] = None


@app.get("/files/pending")
def get_pending() -> list[str]:
    """
    Get a list of all pending files
    """
    return list_pending_uuids()

@app.get("/dispatch")
@app.get("/dispatch/{uuid}")
async def dispatch(uuid: str = 'all', record: Optional[dict] = None, **body: dict):
    """
    Dispatch a file for preprocessing.
    The API will not wait for the process to be finished.
    """
    # handle supabase webhook payloads
    if record:
        if 'uuid' not in record:
            logger.error(f"Received a Supabase webhhok payload without a uuid: {record}")
        else:
            uuid = record['uuid']
            logger.info("Dispatching preprocessor over /dispatch by Supabase webhook using payload: {record}")

    # dispatch all 
    if uuid == 'all':
        dispatch_pending_files(wait=False)
        logger.info("Dispatching preprocessor over /dispatch by API using uuid: 'all'")
    else:
        ThreadPoolExecutor().submit(preprocess_file, uuid)
        logger.info(f"Dispatching preprocessor by invoking /dispatch/{uuid}")


def run(
    host: str = settings.uvicorn_host,
    port: int = settings.uvicorn_port,
    root_path: str = settings.uvicorn_root_path, 
    proxy_headers: bool = settings.uvicorn_proxy_headers,
    reload=False
):
    """
    Run the storage app using the uvicron wsgi server. the settings are loaded from the 
    settings submodule, but can be overwritten with directly.
    :param host: host for the server
    :param port: port for the server
    :param root_path: root path for the server
    :param proxy_headers: use proxy headers
    :param reload: reload the server on file changes
    """
    uvicorn.run("app:app", host=host, port=port, root_path=root_path, proxy_headers=proxy_headers, reload=reload)


if __name__ == "__main__":
    import fire
    fire.Fire(run)

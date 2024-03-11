from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from concurrent.futures import ThreadPoolExecutor

from processor.metadata import list_pending_uuids
from processor.handler import dispatch_pending_files, preprocess_file
from processor.utils.settings import settings


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

@app.get("/files/pending")
def get_pending() -> list[str]:
    """
    Get a list of all pending files
    """
    return list_pending_uuids()

@app.get("/dispatch")
@app.get("/dispatch/{uuid}")
async def dispatch(uuid: str = 'all'):
    """
    Dispatch a file for preprocessing.
    The API will not wait for the process to be finished.
    """
    # dispatch all 
    if uuid == 'all':
        dispatch_pending_files(wait=False)
        return {"message": ""}
    else:
        ThreadPoolExecutor().submit(preprocess_file, uuid)
        return {"message": f"dispatching {uuid}"}


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
    uvicorn.run("storage.app:app", host=host, port=port, root_path=root_path, proxy_headers=proxy_headers, reload=reload)


if __name__ == "__main__":
    import fire
    fire.Fire(run)

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from concurrent.futures import ThreadPoolExecutor

from processor.metadata import list_pending_uuids
from processor.handler import dispatch_pending_files, preprocess_file


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
    
def run_server(host='127.0.0.1', port=8000, reload=True):
    uvicorn.run('app:app', host=host, port=port, reload=reload)

if __name__ == "__main__":
    import fire
    fire.Fire(run_server)

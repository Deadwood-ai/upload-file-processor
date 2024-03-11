"""
Create a MAPFILE for MapServer for each preprocssed dataset.

The MAPFILE is a text file that contains the instructions for MapServer to render the data.
"""
from .metadata import FileUploadMetadata
from .files import get_mapfile, put_mapfile
from .utils.settings import settings


def add_wms_layer(metadata: FileUploadMetadata):
    # load the mapfile, we want to use
    mappy = get_mapfile()

    # create the layer dictionary
    layer = {
        "__type__": "layer",
        "type": "raster",
        "name": metadata.file_id,
        "status": "on",
        "data": metadata.processed_path,
        "processing": ["BANDS=1,2,3"],
        "metadata": {
            "wms_title": metadata.file_id,
        },
        "template": "empty"
    }

    # check if the mapfile already has layers
    if "layers" not in mappy:
        mappy["layers"] = [layer]
    else:
        mappy["layers"].append(layer)

    # put the layerfile back to the mapserver
    put_mapfile(mappy)


def create_wms_source(metadata: FileUploadMetadata) -> str:
    """
    Takes a UUID for a metadata entry and creates a LAYER for that file in the 
    MAPFILE for MapServer. The MapServer is configured to serve a WMS for the 
    preproccessed image at the configured processed file location, which is always 
    local to the MapServer instance. Finally, the WMS source URL serving exactly this layer
    is saved back to the metadata entry.

    """
    # add the WMS layer to the mapfile
    add_wms_layer(metadata)

    # create the WMS source URL
    wms_url = f"{settings.ows_base_url}LAYERS={metadata.file_id}"

    return wms_url



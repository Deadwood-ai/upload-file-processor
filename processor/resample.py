"""
Resample a given GeoTiff to a specific spatial resolution.
"""
from typing import Union, Literal, Optional
import rasterio
import pyproj
from rasterio.coords import BoundingBox
from rasterio.enums import Resampling, Compression


def auto_scale_factor(raster: rasterio.DatasetReader, target_resolution: float = 0.04, referece_epsg: int = 3857) -> float:
    # TODO: hardcode the target crs for now
    if raster.crs.to_epsg() != referece_epsg:
        transform = pyproj.Transformer.from_crs(raster.crs, pyproj.CRS.from_epsg(referece_epsg), always_xy=True)

        # transform the bounds
        tbounds = transform.transform_bounds(*raster.bounds)
    else:
        tbounds = raster.bounds
    
    # calculate the resolutions in meters
    xres = (tbounds[2] - tbounds[0]) / raster.width
    yres = (tbounds[3] - tbounds[1]) / raster.height
    
    # divide the smaller resolution by the target resolution to get the scale factor
    return min(xres, yres) / target_resolution


def resample(
    input_file: str,
    output_file: str,
    scale_factor: Union[float, Literal['auto']] = 1 / 10,
    method: Resampling = Resampling.bilinear,
    driver: str = "GTiff",
    compress: Optional[Compression] = None,
    jpeg_quality: Optional[int] = None
) -> BoundingBox:
    """
    Resample the input_file to the given scale_factor and save the output to output_file.

    Returns the bounding box of the resampled and reprojected image

    """
    with rasterio.open(input_file) as src:
        # check the scale factor
        if isinstance(scale_factor, str):
            if scale_factor.lower() == 'auto':
                scale_factor = auto_scale_factor(src)

                # TODO: not sure how we want to handle this case:
                if scale_factor > 1:
                    scale_factor = 1.0
            else:
                raise ValueError("Invalid value for scale_factor. Must be a float or 'auto'")

        # resample data to target shape while reading
        data = src.read(
            out_shape=(
                src.count,
                int(src.height * scale_factor),
                int(src.width * scale_factor),
            ),
            resampling=method,
        )

        # check if the source is already EPSG:4326
        if src.crs.to_epsg() != 4326:
            # reproject the data to EPSG:4326
            data, transform = rasterio.warp.reproject(
                data,
                src_crs=src.crs,
                src_transform=src.transform,
                dst_crs="EPSG:4326",
                dst_transform=src.transform
                * src.transform.scale(
                    (src.width / data.shape[-1]), (src.height / data.shape[-2])
                ),
                resampling=method,
            )
        else:
            # scale the affine transform
            transform = src.transform * src.transform.scale(
                (src.width / data.shape[-1]), (src.height / data.shape[-2])
            )

        # save to output file
        write_options = dict(
            driver=driver,
            height=data.shape[1],
            width=data.shape[2],
            count=src.count,
            dtype=data.dtype,
            crs=src.crs,
            transform=transform,
        )

        # check if a compression is specified
        if compress is not None:
            write_options["compress"] = compress.upper()
            if compress.lower() == 'jpeg':
                write_options[jpeg_quality] = jpeg_quality if jpeg_quality is not None else 90
        
        # write the file
        with rasterio.open(output_file, "w", **write_options) as dst:
            dst.write(data)
        
        # return a read-only reference to the file
        with rasterio.open(output_file, 'r') as dst:
            return dst.bounds

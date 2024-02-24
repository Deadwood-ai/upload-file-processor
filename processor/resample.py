"""
Resample a given GeoTiff to a specific spatial resolution.
"""
import rasterio
from rasterio.coords import BoundingBox
from rasterio.enums import Resampling


def resample(
    input_file: str,
    output_file: str,
    scale_factor: float = 1 / 10,
    method: Resampling = Resampling.bilinear,
    driver: str = "GTiff",
) -> BoundingBox:
    """
    Resample the input_file to the given scale_factor and save the output to output_file.

    Returns the bounding box of the resampled and reprojected image

    """
    with rasterio.open(input_file) as src:
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
        with rasterio.open(output_file, "w", **write_options) as dst:
            dst.write(data)
        
        with rasterio.open(output_file, 'r') as dst:
            return dst.bounds

"""
Resample a given GeoTiff to a specific spatial resolution.
"""
import rasterio
from rasterio.enums import Resampling


def resample(input_file: str, output_file: str, scale_factor: float = 1 / 10, method: Resampling = Resampling.bilinear):
    """
    Resample the input_file to the given scale_factor and save the output to output_file.

    """
    with rasterio.open(input_file) as src:
        # resample data to target shape while reading
        data = src.read(
            out_shape=(
                src.count,
                int(src.height * scale_factor),
                int(src.width * scale_factor)
            ),
            resampling=method
        )

        # scale the affine transform
        transform = src.transform * src.transform.scale(
            (src.width / data.shape[-1]),
            (src.height / data.shape[-2])
        )

        # save to output file
        with rasterio.open(
            output_file,
            'w',
            driver='GTiff',
            height=data.shape[1],
            width=data.shape[2],
            count=src.count,
            dtype=data.dtype,
            crs=src.crs,
            transform=transform
        ) as dst:
            dst.write(data)


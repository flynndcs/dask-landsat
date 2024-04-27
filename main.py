import dask
import dask.delayed
from dask.distributed import Client
import requests
import geopandas as gpd
import rasterio


class DaskLandsat:
    STAC_ROOT = "https://landsatlook.usgs.gov/stac-server"
    COLLECTIONS = "collections"
    ITEMS = "items"
    SEPARATOR = "/"
    COLLECTION_IDS = ["landsat-c2l2-sr", "landsat-c2l2-st", "landsat-c2l1"]
    BANDS = ["red", "green", "blue"]

    def _build_url(self, path_segments):
        return self.SEPARATOR.join(path_segments)

    def _get(self, path_segments):
        return requests.get(self._build_url(path_segments)).json()

    def _get_collection_items(self, id: str) -> dict:
        """
        Returns the items response for the collection id
        """
        return self._get(
            [
                self.STAC_ROOT,
                self.COLLECTIONS,
                id,
                self.ITEMS,
            ]
        )

    def _get_feature_area_geometry(self, feature):
        gdf = gpd.GeoDataFrame.from_features([feature])
        gdf.set_crs(4326, inplace=True)

        epsg = gdf["proj:epsg"].item()
        if epsg:
            gdf.to_crs(epsg=epsg, inplace=True)
        return {"area": gdf.area.item(), "geometry": gdf.geometry.to_wkt().item()}

    def _read_asset_hrefs(self, feature):
        results = []
        # placeholder working COG, reading Landsat COGs results in:
        # rasterio.errors.RasterioIOError: Line 51: Didn't find expected '=' for value of attribute 'async'.
        with rasterio.open(
            "https://download.osgeo.org/geotiff/samples/usgs/c41078a1.tif"
        ) as src:
            results.append(src.profile)
        return results

    def _process_items(self, items_response):
        items = items_response["features"]
        results = []
        for feature in items:
            results.append(
                (
                    self._get_feature_area_geometry(feature),
                    self._read_asset_hrefs(feature),
                )
            )
        return results

    def __call__(self) -> dict:
        results = [
            dask.delayed(self._process_items)(
                dask.delayed(self._get_collection_items)(id)
            )
            for id in self.COLLECTION_IDS
        ]

        return dask.compute(*results)


def run():
    with Client():
        runner = DaskLandsat()
        print(runner())


if __name__ == "__main__":
    run()

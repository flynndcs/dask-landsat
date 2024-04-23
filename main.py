import dask
from dask.distributed import Client
import requests
import geopandas as gpd
from typing import List, Tuple

# Landsat Collection 2 STAC API (stac-server)
# https://landsatlook.usgs.gov/stac-server/
#
#   Collections
#       Level-2 UTM Surface Reflectance (SR)
#           Metadata
#           Items
#               LC_0{45789}...
#               Metadata
#               Assets (data)
#                   Thumbnail image/jpeg
#                   Browse image/jpeg
#                   B1-7 vnd.stac.geotiff; cloud-optimized=true
#                   ...
#               ...
#       ...


class DaskLandsat:
    STAC_ROOT = "https://landsatlook.usgs.gov/stac-server"
    COLLECTIONS = "collections"
    ITEMS = "items"
    SEPARATOR = "/"

    def _get(self, url):
        return requests.get(url).json()

    def _get_collection_ids(self) -> List[str]:
        """
        Returns an array of collection ids.
        """
        response = self._get(self.SEPARATOR.join([self.STAC_ROOT, self.COLLECTIONS]))

        return [collection["id"] for collection in response["collections"]]

    @dask.delayed
    def _get_collection_items(self, id: str) -> gpd.GeoDataFrame:
        """
        Returns a Delayed GeoDataFrame of the first 10 items' scene id and area from the collection
        """
        response = self._get(
            self.SEPARATOR.join(
                [
                    self.STAC_ROOT,
                    self.COLLECTIONS,
                    id,
                    self.ITEMS,
                ]
            )
        )

        return (id, gpd.GeoDataFrame.from_features(response))

    @dask.delayed
    def _get_area(self, id_df: Tuple[str, gpd.GeoDataFrame]) -> dict:
        """
        Returns a Delayed dict of scene ids -> area for this dataframe
        """
        id, gdf = id_df

        gdf.set_crs(4326, inplace=True)

        # transform UTM geometries to meters
        epsg = gdf.head(1)["proj:epsg"].item()

        if not epsg:
            return {}
        else:
            gdf.to_crs(epsg=epsg, inplace=True)

        return {
            f"{key}_{id}": {"area": area, "geometry": geometry.wkt}
            for key, area, geometry in zip(
                gdf["landsat:scene_id"], gdf.area, gdf["geometry"]
            )
        }

    def __call__(self) -> dict:
        collection_ids = self._get_collection_ids()
        id_gdfs: List[gpd.GeoDataFrame] = dask.compute(
            *[self._get_collection_items(id) for id in collection_ids]
        )

        collection_items_dicts = dask.compute(
            *[
                self._get_area(id_gdf)
                for id_gdf in id_gdfs
                if "landsat:scene_id" in id_gdf[1].columns
            ]
        )

        return {
            key: value for dict in collection_items_dicts for key, value in dict.items()
        }


def run():
    with Client():
        runner = DaskLandsat()
        print(runner())


if __name__ == "__main__":
    run()

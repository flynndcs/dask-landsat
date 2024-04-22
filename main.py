import dask
from dask.distributed import Client
import requests
import geopandas as gpd
from typing import List

# Landsat Collection 2 STAC API (stac-server)
# https://landsatlook.usgs.gov/stac-server/
#
#   Collections
#       Level-2 UTM Surface Reflectance (SR)
#       Metadata
#       Items
#           LC_0{45789}...
#           Metadata
#           Assets (data)
#               Thumbnail image/jpeg
#               Browse image/jpeg
#               ...
#               B1-7 vnd.stac.geotiff; cloud-optimized=true
#   ...


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
    def _get_collection_items(self, id: str) -> dict:
        """
        Returns a Delayed dictionary of the first 10 items' scene id and area from the collection 
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

        gdf = gpd.GeoDataFrame.from_features(response)

        if "landsat:scene_id" not in gdf:
            return {}
        else:
            return {
                f"{key}_{id}": value
                for key, value in zip(gdf["landsat:scene_id"], gdf.area)
            }

    def __call__(self) -> dict:
        collection_ids = self._get_collection_ids()
        collection_items_dicts = dask.compute(*[self._get_collection_items(id) for id in collection_ids])

        return {
            key: value
            for dict in collection_items_dicts 
            for key, value in dict.items()
        }


def run():
    with Client():
        runner = DaskLandsat()
        print(runner())


if __name__ == "__main__":
    run()

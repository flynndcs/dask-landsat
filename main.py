import dask
import dask.delayed
from dask.distributed import Client
import requests

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
    LANDSAT_STAC_API_ROOT = "https://landsatlook.usgs.gov/stac-server"
    COLLECTIONS_ROUTE = "collections"
    ITEMS_ROUTE = "items"
    SEPARATOR = "/"

    def _get(self, url):
        return requests.get(url).json()

    def _get_collection_ids(self):
        """
        Returns an array of collection ids.
        """
        response = self._get(
            self.SEPARATOR.join([self.LANDSAT_STAC_API_ROOT, self.COLLECTIONS_ROUTE])
        )

        return [collection["id"] for collection in response["collections"]]

    @dask.delayed
    def _get_collection_metadata(self, id):
        """
        Returns a Delayed of a collection's metadata (title for now).
        """
        response = self._get(
            self.SEPARATOR.join(
                [self.LANDSAT_STAC_API_ROOT, self.COLLECTIONS_ROUTE, id]
            )
        )
        return response["title"]

    @dask.delayed
    def _get_collection_items(self, id):
        """
        Get a Delayed of the first 10 items from the {id} collection
        """
        response = self._get(
            self.SEPARATOR.join(
                [
                    self.LANDSAT_STAC_API_ROOT,
                    self.COLLECTIONS_ROUTE,
                    id,
                    self.ITEMS_ROUTE,
                ]
            )
        )
        return [item["id"] for item in response["features"]]

    def __call__(self):
        # 1. Get all collection ids, cannot parallelize
        collection_ids = self._get_collection_ids()
        
        # 2. Get metadata and items for each collection, can parallelize
        collections = []
        for id in collection_ids:
            collections.append(
                {self._get_collection_metadata(id): self._get_collection_items(id)}
            )

        return dask.compute(*collections)


def run():
    with Client():
        runner = DaskLandsat()
        print(runner())


if __name__ == "__main__":
    run()

import dask
from dask.distributed import Client
import requests
import geopandas as gpd
from typing import List, Tuple


class DaskLandsat:
    STAC_ROOT = "https://landsatlook.usgs.gov/stac-server"
    COLLECTIONS = "collections"
    ITEMS = "items"
    SEPARATOR = "/"

    def _build_url(self, path_segments):
        return self.SEPARATOR.join(path_segments)

    def _get(self, path_segments):
        return requests.get(self._build_url(path_segments)).json()

    def _get_collection_ids(self) -> List[str]:
        """
        Returns an array of collection ids.
        """
        response = self._get([self.STAC_ROOT, self.COLLECTIONS])

        return [collection["id"] for collection in response["collections"]]

    @dask.delayed
    def _get_collection_items(self, id: str) -> Tuple[str, gpd.GeoDataFrame]:
        """
        Returns a Delayed GeoDataFrame of the first 10 items' scene id and area from the collection
        """
        response = self._get(
            [
                self.STAC_ROOT,
                self.COLLECTIONS,
                id,
                self.ITEMS,
            ]
        )

        gdf = gpd.GeoDataFrame.from_features(response)

        return (id, gdf) if "landsat:scene_id" in gdf.columns else None

    @dask.delayed
    def _get_area_geometry_dict(
        self, id_df: Tuple[str, gpd.GeoDataFrame] | None
    ) -> dict:
        """
        Returns a Delayed dict of scene ids -> area for this dataframe
        """
        if not id_df:
            return {}

        id, gdf = id_df

        gdf.set_crs(4326, inplace=True)

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
        collection_items_dicts = [
            self._get_area_geometry_dict(self._get_collection_items(id))
            for id in self._get_collection_ids()
        ]

        return {
            key: value
            for dict in dask.compute(*collection_items_dicts)
            for key, value in dict.items()
        }


def run():
    with Client():
        runner = DaskLandsat()
        print(runner())


if __name__ == "__main__":
    run()

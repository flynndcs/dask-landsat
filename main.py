from dask.distributed import Client


class DaskLandsat:
    def __call__(self):
        return "dask-landsat"


def run():
    with Client():
        runner = DaskLandsat()
        print(runner())


if __name__ == "__main__":
    run()

import logging

from src.batch_processing import aggregate_data, transform_dask_to_es, transform_dask_to_time_stream
from src.collection import collect_data_from_spotify
from src.master_storage import save_data_to_s3
from src.real_time_views import save_data_to_timestream
from src.visualization import save_data_to_es

logger = logging.getLogger(__name__)

COMPLETED_MESSAGE = "completed tier"


def execute_pipeline():
    # Collection Tier
    dfs = collect_data_from_spotify()
    logger.info(COMPLETED_MESSAGE, extra={"tier": "collection"})

    # Master Storage Tier
    save_data_to_s3(dfs)
    logger.info(COMPLETED_MESSAGE, extra={"tier": "master storage"})

    # Batch Processing Tier
    es_data = aggregate_data(dfs, transform_dask_to_es)
    ts_data = aggregate_data(dfs, transform_dask_to_time_stream)
    logger.info(COMPLETED_MESSAGE, extra={"tier": "batch processing"})

    # Real time views Tier
    save_data_to_timestream(ts_data)
    logger.info(COMPLETED_MESSAGE, extra={"tier": "real time views"})

    # Visualization Tier
    save_data_to_es(es_data)
    logger.info(COMPLETED_MESSAGE, extra={"tier": "visualization"})


if __name__ == '__main__':
    execute_pipeline()

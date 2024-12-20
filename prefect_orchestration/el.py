from prefect import flow, task, get_run_logger
import time
import random

@task
def extract():
    seconds = random.randint(1,3)
    time.sleep(seconds)
    return seconds

@task
def load(data):
    seconds = random.randint(1,3)
    time.sleep(seconds)
    return seconds

@flow()
def wrapper(thread: int, source_db: str, target_db: str, **kwargs):
    logger = get_run_logger()
    logger.info(thread)
    logger.info(source_db)
    logger.info(target_db)
    data = extract()
    load.submit(data)

@flow(flow_run_name="extract_load_{target_db}")
def main(thread: int, source_db: str, target_db: str, **kwargs):
    for _ in range(10):
        wrapper(thread, source_db, target_db, **kwargs)


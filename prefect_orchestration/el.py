from prefect import flow, task, get_run_logger
import time
import random

@task
def extract():
    seconds = random.randint(1,10)
    time.sleep(seconds)
    return seconds

@task
def load(data):
    seconds = random.randint(1,10)
    time.sleep(seconds)
    return seconds

@flow(flow_run_name="extract_load_{target_db}")
def main(thread: int, source_db: str, target_db: str, **kwargs):
    logger = get_run_logger()
    logger.info(thread)
    logger.info(source_db)
    logger.info(target_db)
    data = extract()
    load(data)




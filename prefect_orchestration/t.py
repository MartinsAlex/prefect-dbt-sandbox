from prefect import flow, task, get_run_logger
import time
import random

@task
def transform():
    time.sleep(random.randint(1,10))


@flow
def main():
    logger = get_run_logger()
    logger.info("Tranforming data")


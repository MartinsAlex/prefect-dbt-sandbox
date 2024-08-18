from prefect import flow, task, get_run_logger

@flow
def my_flow(message: str):
    logger = get_run_logger()
    logger.info(message)


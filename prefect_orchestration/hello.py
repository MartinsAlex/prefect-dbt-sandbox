from prefect import flow, task



@flow(log_prints=True)
def my_flow(message: str):
    print(f"{message}")
    

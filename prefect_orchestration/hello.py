from prefect import flow, task

@task(log_prints=True)
def dummy():
    print(3)


@flow
def main():
    for _ in range(1, 10):
        dummy.submit()

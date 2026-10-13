"""Runnable local example; replace handlers with your deployment registry."""
import os
import time

from workflow_engine import RetryPolicy, TaskEngine


def multiply(value, factor=2):
    return value * factor


def registry():
    # Also usable via: workflow-management --registry examples.management_server:registry
    return {'abs-v1': abs, 'multiply-v1': multiply}


def main():
    token = os.environ['WORKFLOW_API_TOKEN']
    engine = TaskEngine(database_path='workflow-management.db', task_registry=registry(),
                        retry_policy=RetryPolicy(2))
    service = engine.create_management_service(token,
        readonly_token=os.environ.get('WORKFLOW_API_READ_TOKEN'), port=8080)
    workers = [engine.create_worker('local-example-' + str(index)) for index in range(2)]
    try:
        service.start()
        for worker in workers:
            worker.start()
        print('Management API: ' + service.url, flush=True)
        while True:
            time.sleep(.5)
    except KeyboardInterrupt:
        pass
    finally:
        for worker in workers:
            worker.shutdown()
        service.shutdown()


if __name__ == '__main__':
    main()

from datetime import date

from tasks.fact_customer_task import FactCustomerTask
from tasks.init_source_task import InitSourceTask

if __name__ == "__main__":
    # create initial data used by main task
    task = InitSourceTask()
    task.execute()

    # execute main task
    task = FactCustomerTask(report_date=date(2019, 6, 30))
    task.execute()

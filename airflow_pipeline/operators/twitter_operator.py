import sys
sys.path.append("airflow_pipeline")

from airflow.models import BaseOperator, DAG, TaskInstance
import json
from hook.twitter_hook import TwitterHook
from datetime import datetime, timedelta
from os.path import join
from pathlib import Path

class TwitterOperator(BaseOperator):

    template_fields = ["query", "file_path", "start_time", "end_time"]

    def __init__(self, file_path, end_time, start_time, query, **kwargs):
        self.end_time = end_time
        self.start_time = start_time
        self.query = query
        self.file_path = file_path
        super().__init__(**kwargs)
    
    def create_parent_folders(self):
        (Path(self.file_path).parent).mkdir(parents=True, exist_ok=True)
        

    def execute(self, context):
        end_time = self.end_time
        start_time = self.start_time
        query = self.query
        
        self.log.info({"start": start_time, "end": end_time})

        self.create_parent_folders()

        with open(self.file_path, "w") as output_file:
            for pg in TwitterHook(end_time, start_time, query).run():
                self.log.info(pg)
                json.dump(pg, output_file, ensure_ascii=False)
                output_file.write("\n")


if __name__ == "__main__":
    time_zone = datetime.now().astimezone().tzname()
    TIMESTAMP_FORMAT = f"%Y-%m-%dT%H:%M:%S.00{time_zone}:00"
    end_time = datetime.now().strftime(TIMESTAMP_FORMAT)
    start_time = (datetime.now() + timedelta(days=-7)).strftime(TIMESTAMP_FORMAT)
    query = "datascience"
    file_path = join(
        f'datalake/twitter_{query}', 
        f"extract_date_{datetime.now().date()}",
        f"{query}_{datetime.now().date().strftime('%Y%m%d')}.json"
    )
    
    with DAG(dag_id="Twitter_test", start_date=datetime.now()) as dag:
        t_operator = TwitterOperator(file_path=file_path, query=query, start_time=start_time, end_time=end_time, task_id = "test_run")
        t_task_instance = TaskInstance(task=t_operator)
        t_operator.execute(t_task_instance.task_id)
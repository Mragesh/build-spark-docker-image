
class Audit(object):
    def __init__(self, etl_run_id="", etl_job_name="", etl_start_time="", etl_end_time="", status="Started"):
        self.etl_run_id = etl_run_id
        self.etl_job_name = etl_job_name
        self.etl_start_time = etl_start_time
        self.etl_end_time = etl_end_time
        self.etl_status = status


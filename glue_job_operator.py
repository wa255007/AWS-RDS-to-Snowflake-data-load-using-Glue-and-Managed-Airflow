import time

from airflow.models.baseoperator import BaseOperator
from airflow.exceptions import AirflowException

import json
import base64
import boto3
from botocore.config import Config
from airflow.utils.decorators import apply_defaults
import os.path
from typing import TYPE_CHECKING, Optional, Sequence, Dict

class GlueJobOperator(BaseOperator):
    """
    Runs an AWS Glue Job. AWS Glue is a serverless Spark
    ETL service for running Spark Jobs on the AWS cloud.
    Language support: Python and Scala

    :param job_name: unique job name per AWS Account
    :param job_desc: job description details
    :param script_args: etl script arguments and AWS Glue arguments (templated)
    :param run_job_kwargs: Extra arguments for Glue Job Run
    :param wait_for_completion: Whether or not wait for job run completion. (default: True)
    """
    JOB_POLL_INTERVAL = 10  # polls job status after every JOB_POLL_INTERVAL seconds
    template_fields: Sequence[str] = ('script_args','run_job_kwargs')
    template_ext: Sequence[str] = ()
    template_fields_renderers = {
        "script_args": "json",
        "create_job_kwargs": "json",
    }
    ui_color = '#ededed'


    @apply_defaults
    def __init__(
        self,
        *,
        job_name: str = 'aws_glue_default_job',
        job_desc: str = 'AWS Glue Job with Airflow',
        script_args: Optional[dict] = None,
        run_job_kwargs: Optional[dict] = None,
        table_list: Optional[list] = None,
        wait_for_completion: bool = True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.job_name = job_name
        self.job_desc = job_desc
        self.script_args = script_args or {}
        self.run_job_kwargs = run_job_kwargs or {}
        self.table_list = table_list or []
        self.wait_for_completion = wait_for_completion

    def initialize_job(
            self,
            job_name: str,
            script_arguments: Optional[dict] = None,
            run_kwargs: Optional[dict] = None,
    ) -> Dict[str, str]:
        """
        Initializes connection with AWS Glue
        to run job
        :return:
        """
        glue_client = boto3.client('glue')
        script_arguments = script_arguments or {}
        run_kwargs = run_kwargs or {}
        run_kwargs['NumberOfWorkers'] = int(run_kwargs.get('NumberOfWorkers'))
        self.log.info(" Updated NumberOfWorkers variable to integer ")

        try:
            job_run = glue_client.start_job_run(JobName=job_name, Arguments=script_arguments, **run_kwargs)
            return job_run
        except Exception as general_error:
            self.log.error("Failed to run aws glue job, error: %s", general_error)
            raise

    def get_job_state(self, job_name: str, run_id: str) -> str:
        """
        Get state of the Glue job. The job state can be
        running, finished, failed, stopped or timeout.
        :param job_name: unique job name per AWS account
        :param run_id: The job-run ID of the predecessor job run
        :return: State of the Glue job
        """
        glue_client = boto3.client('glue')
        job_run = glue_client.get_job_run(JobName=job_name, RunId=run_id, PredecessorsIncluded=True)
        job_run_state = job_run['JobRun']['JobRunState']
        return job_run_state

    def job_completion(self, job_name: str, run_id: str) -> Dict[str, str]:
        """
        Waits until Glue job with job_name completes or
        fails and return final state if finished.
        Raises AirflowException when the job failed
        :param job_name: unique job name per AWS account
        :param run_id: The job-run ID of the predecessor job run
        :return: Dict of JobRunState and JobRunId
        """
        failed_states = ['FAILED', 'TIMEOUT']
        finished_states = ['SUCCEEDED', 'STOPPED']

        while True:
            job_run_state = self.get_job_state(job_name, run_id)
            if job_run_state in finished_states:
                self.log.info("Exiting Job %s Run State: %s", run_id, job_run_state)
                return {'JobRunState': job_run_state, 'JobRunId': run_id}
            if job_run_state in failed_states:
                job_error_message = "Exiting Job " + run_id + " Run State: " + job_run_state
                self.log.info(job_error_message)
                raise AirflowException(job_error_message)
            else:
                self.log.info(
                    "Polling for AWS Glue Job %s current run state with status %s", job_name, job_run_state
                )
                time.sleep(self.JOB_POLL_INTERVAL)

    def execute(self, context: 'Context'):
        """
        Executes AWS Glue Job from Airflow

        :return: the id of the current glue job.
        """

        glue_job_run = self.initialize_job(self.job_name,self.script_args, self.run_job_kwargs)
        if self.wait_for_completion:
            glue_job_run = self.job_completion(self.job_name, glue_job_run['JobRunId'])
            self.log.info(
                "AWS Glue Job: %s status: %s. Run Id: %s",
                self.job_name,
                glue_job_run['JobRunState'],
                glue_job_run['JobRunId'],
            )
        else:
            self.log.info("AWS Glue Job: %s. Run Id: %s", self.job_name, glue_job_run['JobRunId'])
        return glue_job_run['JobRunId']

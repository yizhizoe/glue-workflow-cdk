from aws_cdk import core
from aws_cdk.aws_glue import CfnTrigger, CfnWorkflow, CfnJob
from aws_cdk import aws_s3 as s3

WF_NAME = 'workflow2-by-cdk'

azk_flow = {
  "project" : "azkaban-test-project",
  "nodes" : [ {
    "id" : "test-final",
    "type" : "command",
    "in" : [ "test-job-4" ]
  },
      {
    "id" : "test-job-start",
    "type" : "java"
  }, {
    "id" : "test-job-3",
    "type" : "java",
    "in" : [ "test-job-2" ]
  }, {
    "id" : "test-job-4",
    "type" : "java",
    "in" : [ "test-job-3" ]
  }, {
    "id" : "test-job-2",
    "type" : "java",
    "in" : [ "test-job-start" ]
  } ],
  "flow" : "test",
  "projectId" : 193
}

class GlueWorkflowCdkStack(core.Stack):

    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        CfnWorkflow(self, WF_NAME, name=WF_NAME)

        job_command = CfnJob.JobCommandProperty(name='pythonshell',
                                                script_location="s3://aws-glue-scripts-182470276418-cn-north-1/yizhi/dummy.py")
        last_trigger = None

        for node in azk_flow['nodes']:

            triggered_job_name = node['id']


            job = CfnJob(self, triggered_job_name,
                         name=triggered_job_name,
                         command=job_command,
                         role='glue-readmsk')

            action_property = CfnTrigger.ActionProperty(job_name=triggered_job_name)

            if 'in' not in node:
                # Scheduled trigger
                trigger = CfnTrigger(self, 'daily-trigger-2', workflow_name=WF_NAME, actions=[action_property],
                                            type='SCHEDULED',
                                            schedule='cron(00 14 * * ? *)')
            else:
                # Trigger on other job success
                firing_on_job_list = node['in']
                condition_list = [CfnTrigger.ConditionProperty(job_name=firing_job,
                                                               state='SUCCEEDED',
                                                               logical_operator='EQUALS')
                                  for firing_job in firing_on_job_list]

                firing_predicate=CfnTrigger.PredicateProperty(conditions=condition_list, logical='AND')

                trigger = CfnTrigger(self, 'trigger-'+triggered_job_name,
                                         workflow_name=WF_NAME,
                                         actions=[action_property],
                                         type='CONDITIONAL',
                                         predicate=firing_predicate)
            if last_trigger:
                last_trigger.add_depends_on(trigger)
            last_trigger = trigger

        # The code that defines your stack goes here

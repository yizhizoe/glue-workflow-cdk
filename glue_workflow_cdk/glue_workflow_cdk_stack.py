from aws_cdk import core
from aws_cdk.aws_glue import CfnTrigger, CfnWorkflow, CfnJob
from aws_cdk.aws_iam import Role, ServicePrincipal, ManagedPolicy
import json
import os
INPUT_WORKFLOW_FILE = 'sample_workflow.json'

SCRIPT_LOCATION = os.environ.get('GLUE_SCRIPT_S3')

class GlueJobCdkStack(core.Stack):
    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        with open(INPUT_WORKFLOW_FILE) as f:
            workflow_dict = json.load(f)
            glue_role = Role(self, "dummy_glue_job_role",
                             assumed_by=ServicePrincipal("glue.amazonaws.com"),
                             managed_policies=[ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")],
                             )
            # mock job
            job_command = CfnJob.JobCommandProperty(name='pythonshell',
                                                    script_location=SCRIPT_LOCATION)

            for workflow_item in workflow_dict:
                for node in workflow_item['nodes']:
                    triggered_job_name = node['id']
                    job = CfnJob(self, triggered_job_name,
                                 name=triggered_job_name,
                                 command=job_command,
                                 role=glue_role.role_name)


class GlueWorkflowCdkStack(core.Stack):

    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        with open(INPUT_WORKFLOW_FILE) as f:
            workflow_dict = json.load(f)

            for workflow_item in workflow_dict:
                workflow_name = workflow_item['flow']
                CfnWorkflow(self, workflow_name, name=workflow_name)

                last_trigger = None

                for node in workflow_item['nodes']:

                    triggered_job_name = node['id']

                    action_property = CfnTrigger.ActionProperty(job_name=triggered_job_name)

                    if 'in' not in node:
                        # Scheduled trigger
                        trigger = CfnTrigger(self, 'scheduled-daily-trigger', workflow_name=workflow_name,
                                             actions=[action_property],
                                             start_on_creation=True,
                                                    type='SCHEDULED',
                                                    schedule='cron(00 23 * * ? *)')
                    else:
                        # Trigger on other job success
                        firing_on_job_list = node['in']
                        condition_list = [CfnTrigger.ConditionProperty(job_name=firing_job,
                                                                       state='SUCCEEDED',
                                                                       logical_operator='EQUALS')
                                          for firing_job in firing_on_job_list]

                        firing_predicate=CfnTrigger.PredicateProperty(conditions=condition_list, logical='AND')

                        trigger = CfnTrigger(self, 'trigger-'+triggered_job_name,
                                                 workflow_name=workflow_name,
                                                 actions=[action_property],
                                                 start_on_creation=True,
                                                 type='CONDITIONAL',
                                                 predicate=firing_predicate)
                    # if last_trigger:
                    #     trigger.add_depends_on(last_trigger)
                    # last_trigger = trigger

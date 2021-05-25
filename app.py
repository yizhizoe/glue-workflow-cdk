#!/usr/bin/env python3

from aws_cdk import core

from glue_workflow_cdk.glue_workflow_cdk_stack import GlueJobCdkStack, GlueWorkflowCdkStack


app = core.App()
GlueJobCdkStack(app, "glue-job-cdk")
GlueWorkflowCdkStack(app, "glue-workflow-cdk")
app.synth()

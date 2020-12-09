#!/usr/bin/env python3

from aws_cdk import core

from glue_workflow_cdk.glue_workflow_cdk_stack import GlueWorkflowCdkStack


app = core.App()
GlueWorkflowCdkStack(app, "glue-workflow-cdk")

app.synth()

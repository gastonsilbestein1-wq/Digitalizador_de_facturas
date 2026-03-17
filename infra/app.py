#!/usr/bin/env python3
import aws_cdk as cdk
from stack import FiscalDocumentProcessorStack

app = cdk.App()

env = app.node.try_get_context("env") or "dev"

FiscalDocumentProcessorStack(
    app,
    f"fiscal-document-processor-{env}",
    env_name=env,
    env=cdk.Environment(
        account=app.node.try_get_context("account"),
        region=app.node.try_get_context("region") or "us-east-1",
    ),
)

app.synth()

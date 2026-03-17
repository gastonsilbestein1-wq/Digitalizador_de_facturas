import os
import aws_cdk as cdk
from aws_cdk import (
    Stack, Duration, RemovalPolicy,
    aws_s3 as s3,
    aws_dynamodb as dynamodb,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_logs as logs,
    aws_apigatewayv2 as apigwv2,
    aws_apigatewayv2_integrations as integrations,
    aws_cloudfront as cloudfront,
    aws_cloudfront_origins as origins,
    aws_s3_deployment as s3deploy,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_events as events,
    aws_events_targets as targets,
    aws_cloudwatch as cw,
)
from constructs import Construct

BEDROCK_MODEL_ID = "us.anthropic.claude-sonnet-4-5-20250929-v1:0"
LAMBDAS_DIR = os.path.join(os.path.dirname(__file__), "..", "lambdas")
FRONTEND_DIR = os.path.join(os.path.dirname(__file__), "..", "frontend")


class FiscalDocumentProcessorStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, env_name: str = "dev", **kwargs):
        super().__init__(scope, construct_id, **kwargs)
        self.env_name = env_name

        self._create_storage()
        self._create_lambdas()
        self._create_api()
        self._create_webapp()
        self._create_pipeline()
        self._create_trigger()
        self._create_observability()
        self._create_outputs()

    # ─── STORAGE ────────────────────────────────────────────────────────────────

    def _create_storage(self):
        self.fact_input = s3.Bucket(
            self, "FactInput",
            bucket_name=f"fact-input-{self.env_name}-{self.account}",
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            event_bridge_enabled=True,
            lifecycle_rules=[s3.LifecycleRule(expiration=Duration.days(7))],
            removal_policy=RemovalPolicy.DESTROY,
            cors=[s3.CorsRule(
                allowed_methods=[s3.HttpMethods.PUT],
                allowed_origins=["*"],
                allowed_headers=["*"],
                max_age=3000,
            )],
        )

        self.fact_output = s3.Bucket(
            self, "FactOutput",
            bucket_name=f"fact-output-{self.env_name}-{self.account}",
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            versioned=True,
            removal_policy=RemovalPolicy.RETAIN,
        )

        self.pipeline_table = dynamodb.Table(
            self, "PipelineExecutions",
            table_name="pipeline-executions",
            partition_key=dynamodb.Attribute(name="processing_id", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
        )

        self.tx_counter_table = dynamodb.Table(
            self, "TxCounter",
            table_name="tx-counter",
            partition_key=dynamodb.Attribute(name="date", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
        )

    # ─── LAMBDAS ─────────────────────────────────────────────────────────────────

    def _lambda_defaults(self, name: str, timeout: int = 120, memory: int = 512) -> dict:
        return dict(
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.handler",
            timeout=Duration.seconds(timeout),
            memory_size=memory,
            log_retention=logs.RetentionDays.ONE_MONTH,
        )

    def _bedrock_policy(self) -> iam.PolicyStatement:
        return iam.PolicyStatement(
            actions=["bedrock:InvokeModel"],
            resources=["*"],
        )

    def _create_lambdas(self):
        # presign
        self.presign_fn = lambda_.Function(
            self, "PresignFn",
            function_name=f"presign-{self.env_name}",
            code=lambda_.Code.from_asset(os.path.join(LAMBDAS_DIR, "presign")),
            environment={
                "FACT_INPUT_BUCKET": f"fact-input-{self.env_name}-{self.account}",
                "TX_COUNTER_TABLE": "tx-counter",
                "PIPELINE_EXECUTIONS_TABLE": "pipeline-executions",
                "FACT_OUTPUT_BUCKET": f"fact-output-{self.env_name}-{self.account}",
            },
            **self._lambda_defaults("presign", timeout=30, memory=256),
        )
        self.fact_input.grant_put(self.presign_fn)
        self.tx_counter_table.grant_read_write_data(self.presign_fn)
        self.pipeline_table.grant_read_data(self.presign_fn)
        self.fact_output.grant_read(self.presign_fn)

        # doc-clasif
        self.doc_clasif_fn = lambda_.Function(
            self, "DocClasifFn",
            function_name=f"doc-clasif-{self.env_name}",
            code=lambda_.Code.from_asset(os.path.join(LAMBDAS_DIR, "doc_clasif")),
            environment={
                "FACT_INPUT_BUCKET": f"fact-input-{self.env_name}-{self.account}",
                "BEDROCK_MODEL_ID": BEDROCK_MODEL_ID,
            },
            **self._lambda_defaults("doc-clasif"),
        )
        self.fact_input.grant_read(self.doc_clasif_fn)
        self.doc_clasif_fn.add_to_role_policy(self._bedrock_policy())

        # doc-eval
        self.doc_eval_fn = lambda_.Function(
            self, "DocEvalFn",
            function_name=f"doc-eval-{self.env_name}",
            code=lambda_.Code.from_asset(os.path.join(LAMBDAS_DIR, "doc_eval")),
            environment={
                "FACT_INPUT_BUCKET": f"fact-input-{self.env_name}-{self.account}",
                "BEDROCK_MODEL_ID": BEDROCK_MODEL_ID,
            },
            **self._lambda_defaults("doc-eval"),
        )
        self.fact_input.grant_read(self.doc_eval_fn)
        self.doc_eval_fn.add_to_role_policy(self._bedrock_policy())

        # data-extract
        self.data_extract_fn = lambda_.Function(
            self, "DataExtractFn",
            function_name=f"data-extract-{self.env_name}",
            code=lambda_.Code.from_asset(os.path.join(LAMBDAS_DIR, "data_extract")),
            environment={
                "FACT_INPUT_BUCKET": f"fact-input-{self.env_name}-{self.account}",
                "BEDROCK_MODEL_ID": BEDROCK_MODEL_ID,
            },
            **self._lambda_defaults("data-extract"),
        )
        self.fact_input.grant_read(self.data_extract_fn)
        self.data_extract_fn.add_to_role_policy(self._bedrock_policy())

        # json-gen
        self.json_gen_fn = lambda_.Function(
            self, "JsonGenFn",
            function_name=f"json-gen-{self.env_name}",
            code=lambda_.Code.from_asset(os.path.join(LAMBDAS_DIR, "json_gen")),
            environment={
                "FACT_OUTPUT_BUCKET": f"fact-output-{self.env_name}-{self.account}",
                "PIPELINE_EXECUTIONS_TABLE": "pipeline-executions",
            },
            **self._lambda_defaults("json-gen", memory=256),
        )
        self.fact_output.grant_put(self.json_gen_fn)
        self.pipeline_table.grant_write_data(self.json_gen_fn)

    # ─── API GATEWAY ─────────────────────────────────────────────────────────────

    def _create_api(self):
        self.api = apigwv2.HttpApi(
            self, "PresignApi",
            api_name=f"presign-api-{self.env_name}",
            cors_preflight=apigwv2.CorsPreflightOptions(
                allow_origins=["*"],
                allow_methods=[apigwv2.CorsHttpMethod.POST],
                allow_headers=["Content-Type"],
            ),
        )
        self.api.add_routes(
            path="/presign",
            methods=[apigwv2.HttpMethod.POST],
            integration=integrations.HttpLambdaIntegration("PresignIntegration", self.presign_fn),
        )
        self.api.add_routes(
            path="/status/{processingId}",
            methods=[apigwv2.HttpMethod.GET],
            integration=integrations.HttpLambdaIntegration("StatusIntegration", self.presign_fn),
        )

    # ─── WEB APP ──────────────────────────────────────────────────────────────────

    def _create_webapp(self):
        self.webapp_bucket = s3.Bucket(
            self, "WebAppBucket",
            bucket_name=f"fact-webapp-{self.env_name}-{self.account}",
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        oac = cloudfront.S3OriginAccessControl(self, "WebAppOAC")

        self.distribution = cloudfront.Distribution(
            self, "WebAppDistribution",
            comment=f"Web App fiscal ({self.env_name})",
            default_root_object="index.html",
            price_class=cloudfront.PriceClass.PRICE_CLASS_100,
            default_behavior=cloudfront.BehaviorOptions(
                origin=origins.S3BucketOrigin.with_origin_access_control(
                    self.webapp_bucket, origin_access_control=oac
                ),
                viewer_protocol_policy=cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
                cache_policy=cloudfront.CachePolicy.CACHING_OPTIMIZED,
                compress=True,
            ),
            error_responses=[
                cloudfront.ErrorResponse(
                    http_status=404,
                    response_http_status=200,
                    response_page_path="/index.html",
                    ttl=Duration.seconds(0),
                )
            ],
        )

        # Subir frontend al bucket
        s3deploy.BucketDeployment(
            self, "WebAppDeploy",
            sources=[s3deploy.Source.asset(FRONTEND_DIR)],
            destination_bucket=self.webapp_bucket,
            distribution=self.distribution,
            distribution_paths=["/*"],
        )

    # ─── STEP FUNCTIONS PIPELINE ─────────────────────────────────────────────────

    def _create_pipeline(self):
        # Paso 0: Registrar inicio en DynamoDB
        registrar_inicio = tasks.DynamoPutItem(
            self, "RegistrarInicio",
            table=self.pipeline_table,
            item={
                "processing_id": tasks.DynamoAttributeValue.from_string(sfn.JsonPath.string_at("$.processing_id")),
                "s3_key":        tasks.DynamoAttributeValue.from_string(sfn.JsonPath.string_at("$.s3_key")),
                "status":        tasks.DynamoAttributeValue.from_string("RUNNING"),
                "current_step":  tasks.DynamoAttributeValue.from_string("DOC_CLASIF"),
                "created_at":    tasks.DynamoAttributeValue.from_string(sfn.JsonPath.string_at("$$.Execution.StartTime")),
                "updated_at":    tasks.DynamoAttributeValue.from_string(sfn.JsonPath.string_at("$$.Execution.StartTime")),
            },
            result_path=sfn.JsonPath.DISCARD,
        )

        # Helper: actualizar current_step en DynamoDB
        def update_step(step_id: str, step_name: str):
            return tasks.DynamoUpdateItem(
                self, f"ActualizarPaso{step_id}",
                table=self.pipeline_table,
                key={"processing_id": tasks.DynamoAttributeValue.from_string(sfn.JsonPath.string_at("$.processing_id"))},
                update_expression="SET current_step = :step, updated_at = :ts",
                expression_attribute_values={
                    ":step": tasks.DynamoAttributeValue.from_string(step_name),
                    ":ts":   tasks.DynamoAttributeValue.from_string(sfn.JsonPath.string_at("$$.State.EnteredTime")),
                },
                result_path=sfn.JsonPath.DISCARD,
            )

        # Estado de error
        registrar_error = tasks.DynamoUpdateItem(
            self, "RegistrarError",
            table=self.pipeline_table,
            key={"processing_id": tasks.DynamoAttributeValue.from_string(sfn.JsonPath.string_at("$.processing_id"))},
            update_expression="SET #s = :status, error_code = :ec, updated_at = :ts",
            expression_attribute_names={"#s": "status"},
            expression_attribute_values={
                ":status": tasks.DynamoAttributeValue.from_string("FAILED"),
                ":ec":     tasks.DynamoAttributeValue.from_string(sfn.JsonPath.format("{}",sfn.JsonPath.string_at("$.error"))),
                ":ts":     tasks.DynamoAttributeValue.from_string(sfn.JsonPath.string_at("$$.State.EnteredTime")),
            },
            result_path=sfn.JsonPath.DISCARD,
        )
        pipeline_fallido = sfn.Fail(self, "PipelineFallido",
            error="PipelineError",
            cause="El pipeline se detuvo por un error o rechazo del documento",
        )
        registrar_error.next(pipeline_fallido)

        # Helper: invocar Lambda + verificar error
        def lambda_step(step_id: str, fn: lambda_.Function):
            invoke = tasks.LambdaInvoke(
                self, f"Invoke{step_id}",
                lambda_function=fn,
                payload_response_only=True,
            )
            check = sfn.Choice(self, f"VerificarError{step_id}")
            check.when(sfn.Condition.is_not_null("$.error"), registrar_error)
            invoke.add_catch(registrar_error, errors=["States.ALL"], result_path="$.lambda_error")
            invoke.next(check)
            return invoke, check

        # Paso 1: doc-clasif
        doc_clasif, check_clasif = lambda_step("DocClasif", self.doc_clasif_fn)
        upd_eval = update_step("DocEval", "DOC_EVAL")
        check_clasif.otherwise(upd_eval)

        # Paso 2: doc-eval
        doc_eval, check_eval = lambda_step("DocEval", self.doc_eval_fn)
        upd_eval.next(doc_eval)
        upd_extract = update_step("DataExtract", "DATA_EXTRACT")
        check_eval.otherwise(upd_extract)

        # Paso 3: data-extract
        data_extract, check_extract = lambda_step("DataExtract", self.data_extract_fn)
        upd_extract.next(data_extract)
        upd_json = update_step("JsonGen", "JSON_GEN")
        check_extract.otherwise(upd_json)

        # Paso 4: json-gen
        json_gen, check_json = lambda_step("JsonGen", self.json_gen_fn)
        upd_json.next(json_gen)
        pipeline_ok = sfn.Succeed(self, "PipelineCompletado")
        check_json.otherwise(pipeline_ok)

        # Encadenar inicio
        definition = registrar_inicio.next(doc_clasif)

        log_group = logs.LogGroup(
            self, "SfnLogGroup",
            log_group_name="/aws/states/fiscal-pipeline",
            retention=logs.RetentionDays.ONE_MONTH,
            removal_policy=RemovalPolicy.DESTROY,
        )

        self.state_machine = sfn.StateMachine(
            self, "FiscalPipeline",
            state_machine_name=f"fiscal-pipeline-{self.env_name}",
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            logs=sfn.LogOptions(
                destination=log_group,
                level=sfn.LogLevel.ALL,
                include_execution_data=True,
            ),
        )
        # Permisos DynamoDB para Step Functions
        self.pipeline_table.grant_write_data(self.state_machine.role)

    # ─── TRIGGER S3 → EventBridge → Step Functions ───────────────────────────────

    def _create_trigger(self):
        rule = events.Rule(
            self, "S3UploadRule",
            rule_name=f"fiscal-s3-upload-{self.env_name}",
            event_pattern=events.EventPattern(
                source=["aws.s3"],
                detail_type=["Object Created"],
                detail={
                    "bucket": {"name": [self.fact_input.bucket_name]},
                    "object": {"key": [
                        {"suffix": ".pdf"},  {"suffix": ".PDF"},
                        {"suffix": ".jpg"},  {"suffix": ".JPG"},
                        {"suffix": ".jpeg"}, {"suffix": ".JPEG"},
                        {"suffix": ".png"},  {"suffix": ".PNG"},
                    ]},
                },
            ),
        )

        rule.add_target(
            targets.SfnStateMachine(
                self.state_machine,
                input=events.RuleTargetInput.from_object({
                    "processing_id": events.EventField.from_path("$.detail.object.key"),
                    "s3_key":        events.EventField.from_path("$.detail.object.key"),
                    "bucket":        events.EventField.from_path("$.detail.bucket.name"),
                }),
            )
        )

    # ─── OBSERVABILIDAD ──────────────────────────────────────────────────────────

    def _create_observability(self):
        # Alarma: ejecuciones fallidas del pipeline
        cw.Alarm(
            self, "AlarmPipelineFailed",
            alarm_name=f"fiscal-pipeline-failed-{self.env_name}",
            metric=cw.Metric(
                namespace="AWS/States",
                metric_name="ExecutionsFailed",
                dimensions_map={"StateMachineArn": self.state_machine.state_machine_arn},
                period=Duration.minutes(5),
                statistic="Sum",
            ),
            threshold=1,
            evaluation_periods=1,
            comparison_operator=cw.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            treat_missing_data=cw.TreatMissingData.NOT_BREACHING,
        )

        # Alarma: errores en Lambda doc-clasif
        cw.Alarm(
            self, "AlarmDocClasifErrors",
            alarm_name=f"doc-clasif-errors-{self.env_name}",
            metric=self.doc_clasif_fn.metric_errors(period=Duration.minutes(5), statistic="Sum"),
            threshold=3,
            evaluation_periods=1,
            treat_missing_data=cw.TreatMissingData.NOT_BREACHING,
        )

        # Alarma: errores en Lambda data-extract
        cw.Alarm(
            self, "AlarmDataExtractErrors",
            alarm_name=f"data-extract-errors-{self.env_name}",
            metric=self.data_extract_fn.metric_errors(period=Duration.minutes(5), statistic="Sum"),
            threshold=3,
            evaluation_periods=1,
            treat_missing_data=cw.TreatMissingData.NOT_BREACHING,
        )

        # Alarma: duración alta en doc-clasif (> 45s)
        cw.Alarm(
            self, "AlarmDocClasifDuration",
            alarm_name=f"doc-clasif-duration-{self.env_name}",
            metric=self.doc_clasif_fn.metric_duration(period=Duration.minutes(5), statistic="p95"),
            threshold=45000,
            evaluation_periods=3,
            treat_missing_data=cw.TreatMissingData.NOT_BREACHING,
        )

        # Alarma: errores en Lambda json-gen
        cw.Alarm(
            self, "AlarmJsonGenErrors",
            alarm_name=f"json-gen-errors-{self.env_name}",
            metric=self.json_gen_fn.metric_errors(period=Duration.minutes(5), statistic="Sum"),
            threshold=3,
            evaluation_periods=1,
            treat_missing_data=cw.TreatMissingData.NOT_BREACHING,
        )

    # ─── OUTPUTS ─────────────────────────────────────────────────────────────────

    def _create_outputs(self):
        cdk.CfnOutput(self, "ApiEndpoint",
            value=self.api.api_endpoint,
            description="URL del endpoint de pre-firma",
        )
        cdk.CfnOutput(self, "CloudFrontUrl",
            value=f"https://{self.distribution.distribution_domain_name}",
            description="URL de la Web App",
        )
        cdk.CfnOutput(self, "FactInputBucket",
            value=self.fact_input.bucket_name,
            description="Bucket S3 de entrada",
        )
        cdk.CfnOutput(self, "FactOutputBucket",
            value=self.fact_output.bucket_name,
            description="Bucket S3 de salida",
        )
        cdk.CfnOutput(self, "StateMachineArn",
            value=self.state_machine.state_machine_arn,
            description="ARN de la Step Function",
        )

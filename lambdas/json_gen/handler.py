"""
Lambda: json-gen
Ensambla el JSON_Fiscal final (14 campos) y lo persiste en el bucket Fact_output.

Entrada (payload de Step Functions):
  {
    "processing_id": "...",
    "s3_key": "...",
    "document_type": "factura_fiscal_b",
    "quality_score": 0.85,
    "extracted_data": { <10 campos fiscales> },
    "error": null
  }

Salida (éxito):
  { ..., "output_key": "{processing_id}.json", "error": null }

Salida (error):
  { ..., "error": "JSON_WRITE_ERROR" }
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

import boto3
from botocore.config import Config

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")

FACT_OUTPUT_BUCKET = os.environ.get("FACT_OUTPUT_BUCKET", "")
PIPELINE_EXECUTIONS_TABLE = os.environ.get("PIPELINE_EXECUTIONS_TABLE", "pipeline-executions")

# Campos requeridos en el JSON de salida (exactamente 14)
REQUIRED_FIELDS = {
    "document_type",
    "total_amount",
    "iva_amount",
    "net_amount",
    "iva_rate",
    "items",
    "tx_match_confidence",
    "merchant_name",
    "cuit",
    "date",
    "receipt_number",
    "quality_score",
    "processing_id",
    "timestamp",
}

# Reintentos con backoff exponencial: 1s, 2s, 4s
MAX_RETRIES = 3
BACKOFF_BASE_SECONDS = 1


def _build_fiscal_json(event: dict) -> dict:
    """
    Ensambla el JSON_Fiscal con exactamente los 14 campos requeridos.
    Genera el timestamp UTC en formato ISO 8601.
    """
    extracted = event.get("extracted_data") or {}
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    fiscal_json = {
        "document_type": event.get("document_type", ""),
        "total_amount": extracted.get("total_amount", 0.0),
        "iva_amount": extracted.get("iva_amount", 0.0),
        "net_amount": extracted.get("net_amount", 0.0),
        "iva_rate": extracted.get("iva_rate", ""),
        "items": extracted.get("items", ""),
        "tx_match_confidence": extracted.get("tx_match_confidence", 0.0),
        "merchant_name": extracted.get("merchant_name", ""),
        "cuit": extracted.get("cuit", ""),
        "date": extracted.get("date", ""),
        "receipt_number": extracted.get("receipt_number", ""),
        "quality_score": event.get("quality_score", 0.0),
        "processing_id": event.get("processing_id", ""),
        "timestamp": timestamp,
    }

    return fiscal_json


def _validate_fiscal_json(fiscal_json: dict) -> None:
    """
    Valida que el JSON tenga exactamente los 14 campos requeridos.
    Lanza ValueError si falta algún campo o hay campos extra.
    """
    present_fields = set(fiscal_json.keys())
    missing = REQUIRED_FIELDS - present_fields
    extra = present_fields - REQUIRED_FIELDS

    if missing:
        raise ValueError(f"Campos faltantes en el JSON fiscal: {missing}")
    if extra:
        raise ValueError(f"Campos extra no permitidos en el JSON fiscal: {extra}")


def _write_to_s3_with_retry(bucket: str, key: str, body: str) -> None:
    """
    Escribe el JSON en S3 con hasta MAX_RETRIES intentos y backoff exponencial.
    Lanza la última excepción si todos los intentos fallan.
    """
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=body.encode("utf-8"),
                ContentType="application/json",
            )
            return  # Éxito
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            logger.warning(
                json.dumps({
                    "action": "s3_write_retry",
                    "attempt": attempt,
                    "max_retries": MAX_RETRIES,
                    "error": str(exc),
                })
            )
            if attempt < MAX_RETRIES:
                time.sleep(BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)))

    raise last_exc  # type: ignore[misc]


def _update_dynamodb_status(processing_id: str, status: str, error_code: str = None) -> None:
    """Actualiza el estado de la ejecución en DynamoDB."""
    try:
        table = dynamodb.Table(PIPELINE_EXECUTIONS_TABLE)
        update_expr = "SET #s = :status, updated_at = :ts"
        expr_names = {"#s": "status"}
        expr_values = {
            ":status": status,
            ":ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        if error_code:
            update_expr += ", error_code = :ec"
            expr_values[":ec"] = error_code

        table.update_item(
            Key={"processing_id": processing_id},
            UpdateExpression=update_expr,
            ExpressionAttributeNames=expr_names,
            ExpressionAttributeValues=expr_values,
        )
    except Exception as exc:  # noqa: BLE001
        # No propagar errores de DynamoDB para no enmascarar el error principal
        logger.error(
            json.dumps({
                "action": "dynamodb_update_failed",
                "processing_id": processing_id,
                "detail": str(exc),
            })
        )


def handler(event, context):
    """
    Handler principal de la Lambda json-gen.
    Ensambla el JSON_Fiscal y lo escribe en Fact_output con reintentos.
    """
    processing_id = event.get("processing_id", "unknown")
    bucket = FACT_OUTPUT_BUCKET or event.get("output_bucket", "")

    logger.info(
        json.dumps({
            "processing_id": processing_id,
            "step": "json-gen",
            "action": "start",
        })
    )

    try:
        # 1. Ensamblar el JSON_Fiscal
        fiscal_json = _build_fiscal_json(event)

        # 2. Validar que tenga exactamente los 14 campos
        _validate_fiscal_json(fiscal_json)

        # 3. Serializar
        output_key = f"{processing_id}.json"
        body = json.dumps(fiscal_json, ensure_ascii=False, indent=2)

        # 4. Escribir en S3 con reintentos
        _write_to_s3_with_retry(bucket, output_key, body)

        # 5. Actualizar DynamoDB con estado COMPLETED
        _update_dynamodb_status(processing_id, "COMPLETED")

        logger.info(
            json.dumps({
                "processing_id": processing_id,
                "step": "json-gen",
                "action": "completed",
                "output_key": output_key,
            })
        )

        return {
            **event,
            "output_key": output_key,
            "error": None,
        }

    except ValueError as exc:
        # Error de validación del JSON (campos faltantes o extra)
        logger.error(
            json.dumps({
                "processing_id": processing_id,
                "step": "json-gen",
                "action": "error",
                "error_code": "INVALID_PAYLOAD",
                "detail": str(exc),
            })
        )
        _update_dynamodb_status(processing_id, "FAILED", "INVALID_PAYLOAD")
        return {
            **event,
            "error": "INVALID_PAYLOAD",
        }

    except Exception as exc:  # noqa: BLE001
        # Error de escritura en S3 tras agotar reintentos
        logger.error(
            json.dumps({
                "processing_id": processing_id,
                "step": "json-gen",
                "action": "error",
                "error_code": "JSON_WRITE_ERROR",
                "detail": str(exc),
            })
        )
        _update_dynamodb_status(processing_id, "FAILED", "JSON_WRITE_ERROR")
        return {
            **event,
            "error": "JSON_WRITE_ERROR",
        }

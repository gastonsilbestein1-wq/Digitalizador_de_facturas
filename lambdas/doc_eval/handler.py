"""
Lambda: doc-eval
Evalúa la calidad y legibilidad del documento fiscal usando Amazon Bedrock (Claude 3.5 Sonnet).

Entrada (payload de Step Functions):
  { "processing_id": "...", "s3_key": "...", "document_type": "factura_fiscal_b", ... }

Salida (aprobado):
  { ..., "quality_score": 0.85, "error": null }

Salida (rechazado):
  { ..., "quality_score": 0.3, "error": "QUALITY_BELOW_THRESHOLD" }
"""

import json
import base64
import logging
import os

import boto3
from botocore.config import Config

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")
bedrock_client = boto3.client(
    "bedrock-runtime",
    config=Config(retries={"max_attempts": 3, "mode": "standard"}),
)

BEDROCK_MODEL_ID = os.environ.get(
    "BEDROCK_MODEL_ID", "anthropic.claude-3-5-sonnet-20241022-v2:0"
)
FACT_INPUT_BUCKET = os.environ.get("FACT_INPUT_BUCKET", "")
QUALITY_THRESHOLD = 0.5

SYSTEM_PROMPT = """Eres un sistema experto en documentos fiscales argentinos. Conoces los formatos de:
- Factura tipo A: emitida entre responsables inscriptos, discrimina IVA, tiene CUIT del emisor y receptor
- Factura tipo B: emitida a consumidores finales, puede o no discriminar IVA, tiene CUIT del emisor
- Ticket fiscal: emitido por controlador fiscal, tiene número de controlador y CUIT del emisor

Siempre responde ÚNICAMENTE con JSON válido, sin texto adicional, sin markdown."""

EVALUATION_PROMPT = """Evalúa la calidad y legibilidad del documento fiscal argentino adjunto.
Determina si los campos fiscales clave son legibles y responde con el siguiente JSON:

{
  "quality_score": 0.0-1.0,
  "identifiable_fields": ["total_amount", "cuit", "date", ...],
  "issues": ["descripción de problemas de calidad si los hay"]
}

Criterios para quality_score:
- 1.0: todos los campos perfectamente legibles
- 0.7-0.9: campos principales legibles, algunos secundarios con dificultad
- 0.5-0.7: campos principales legibles con esfuerzo
- < 0.5: documento ilegible o muy deteriorado"""


def _download_and_encode(bucket: str, key: str) -> tuple[bytes, str]:
    """Descarga el objeto de S3 y devuelve (bytes_raw, media_type)."""
    response = s3_client.get_object(Bucket=bucket, Key=key)
    raw = response["Body"].read()
    lower_key = key.lower()
    if lower_key.endswith(".pdf"):
        media_type = "application/pdf"
    elif lower_key.endswith(".png"):
        media_type = "image/png"
    else:
        media_type = "image/jpeg"
    return raw, media_type


def _invoke_bedrock(raw_bytes: bytes, media_type: str) -> dict:
    """Invoca Claude en Bedrock para evaluar la calidad del documento."""
    if media_type == "application/pdf":
        content_block = {
            "type": "document",
            "source": {
                "type": "base64",
                "media_type": "application/pdf",
                "data": base64.b64encode(raw_bytes).decode("utf-8"),
            },
        }
    else:
        content_block = {
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": media_type,
                "data": base64.b64encode(raw_bytes).decode("utf-8"),
            },
        }

    request_body = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 512,
        "system": SYSTEM_PROMPT,
        "messages": [
            {
                "role": "user",
                "content": [
                    content_block,
                    {"type": "text", "text": EVALUATION_PROMPT},
                ],
            }
        ],
    }

    response = bedrock_client.invoke_model(
        modelId=BEDROCK_MODEL_ID,
        contentType="application/json",
        accept="application/json",
        body=json.dumps(request_body),
    )

    response_body = json.loads(response["body"].read())
    text_content = response_body["content"][0]["text"]
    start = text_content.find("{")
    end = text_content.rfind("}") + 1
    if start == -1 or end == 0:
        raise json.JSONDecodeError("No JSON found in response", text_content, 0)
    return json.loads(text_content[start:end])


def _normalize_quality_score(value) -> float:
    """Asegura que quality_score sea un float en [0.0, 1.0]."""
    try:
        score = float(value)
    except (TypeError, ValueError):
        score = 0.0
    return max(0.0, min(1.0, score))


def handler(event, context):
    """
    Handler principal de la Lambda doc-eval.
    Evalúa la calidad del documento y devuelve el payload enriquecido con quality_score.
    """
    processing_id = event.get("processing_id", "unknown")
    s3_key = event.get("s3_key", "")
    document_type = event.get("document_type")
    bucket = FACT_INPUT_BUCKET or event.get("bucket", "")

    logger.info(
        json.dumps({
            "processing_id": processing_id,
            "step": "doc-eval",
            "action": "start",
            "s3_key": s3_key,
            "document_type": document_type,
        })
    )

    try:
        # 1. Descargar documento de S3
        raw_bytes, media_type = _download_and_encode(bucket, s3_key)

        # 2. Invocar Bedrock para evaluar calidad
        result = _invoke_bedrock(raw_bytes, media_type)

        quality_score = _normalize_quality_score(result.get("quality_score", 0.0))
        identifiable_fields = result.get("identifiable_fields", [])
        issues = result.get("issues", [])

        logger.info(
            json.dumps({
                "processing_id": processing_id,
                "step": "doc-eval",
                "action": "evaluated",
                "quality_score": quality_score,
                "identifiable_fields": identifiable_fields,
                "issues": issues,
            })
        )

        # 3. Verificar umbral de calidad
        if quality_score < QUALITY_THRESHOLD:
            logger.warning(
                json.dumps({
                    "processing_id": processing_id,
                    "step": "doc-eval",
                    "action": "rejected",
                    "reason": "QUALITY_BELOW_THRESHOLD",
                    "quality_score": quality_score,
                })
            )
            return {
                **event,
                "quality_score": quality_score,
                "extracted_data": None,
                "error": "QUALITY_BELOW_THRESHOLD",
            }

        # 4. Devolver payload enriquecido
        return {
            **event,
            "quality_score": quality_score,
            "extracted_data": None,
            "error": None,
        }

    except json.JSONDecodeError as exc:
        logger.error(
            json.dumps({
                "processing_id": processing_id,
                "step": "doc-eval",
                "action": "error",
                "error_code": "EVALUATION_ERROR",
                "detail": f"JSON parse error: {str(exc)}",
            })
        )
        return {
            **event,
            "quality_score": None,
            "extracted_data": None,
            "error": "EVALUATION_ERROR",
        }

    except Exception as exc:  # noqa: BLE001
        logger.error(
            json.dumps({
                "processing_id": processing_id,
                "step": "doc-eval",
                "action": "error",
                "error_code": "EVALUATION_ERROR",
                "detail": str(exc),
            })
        )
        return {
            **event,
            "quality_score": None,
            "extracted_data": None,
            "error": "EVALUATION_ERROR",
        }

"""
Lambda: data-extract
Extrae los campos fiscales del documento usando Amazon Bedrock (Claude 3.5 Sonnet).

Entrada (payload de Step Functions):
  { "processing_id": "...", "s3_key": "...", "document_type": "...", "quality_score": 0.85, ... }

Salida:
  { ..., "extracted_data": { <10 campos fiscales> }, "error": null }
"""

import json
import base64
import logging
import os
import re

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

# Regex de validación
CUIT_REGEX = re.compile(r"^\d{2}-\d{8}-\d$")
DATE_REGEX = re.compile(r"^\d{2}/\d{2}/\d{4}$")

SYSTEM_PROMPT = """Eres un sistema experto en documentos fiscales argentinos. Conoces los formatos de:
- Factura tipo A: emitida entre responsables inscriptos, discrimina IVA, tiene CUIT del emisor y receptor
- Factura tipo B: emitida a consumidores finales, puede o no discriminar IVA, tiene CUIT del emisor
- Ticket fiscal: emitido por controlador fiscal, tiene número de controlador y CUIT del emisor

Siempre responde ÚNICAMENTE con JSON válido, sin texto adicional, sin markdown."""

EXTRACTION_PROMPT_TEMPLATE = """Extrae los datos fiscales del documento argentino adjunto.
El tipo de documento es: {document_type}

Responde ÚNICAMENTE con el siguiente JSON (sin texto adicional):

{{
  "total_amount": número o 0.0,
  "iva_amount": número o 0.0,
  "net_amount": número o 0.0,
  "iva_rate": "XX.XX" o "",
  "items": "descripción de items" o "",
  "tx_match_confidence": 0.0-1.0,
  "merchant_name": "nombre" o "",
  "cuit": "XX-XXXXXXXX-X" o "",
  "date": "DD/MM/YYYY" o "",
  "receipt_number": "número" o ""
}}

Reglas:
- cuit debe tener formato XX-XXXXXXXX-X
- date debe tener formato DD/MM/YYYY
- Para campos no encontrados: usar "" para texto, 0.0 para números
- tx_match_confidence: confianza global de la extracción (0.0-1.0)"""


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


def _invoke_bedrock(raw_bytes: bytes, media_type: str, document_type: str) -> dict:
    """Invoca Claude en Bedrock para extraer los campos fiscales."""
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

    prompt = EXTRACTION_PROMPT_TEMPLATE.format(document_type=document_type)

    request_body = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 1024,
        "system": SYSTEM_PROMPT,
        "messages": [
            {
                "role": "user",
                "content": [
                    content_block,
                    {"type": "text", "text": prompt},
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


def _normalize_extracted_data(raw: dict) -> dict:
    """
    Normaliza y valida los campos extraídos por el modelo.
    - Campos numéricos ausentes o inválidos → 0.0
    - Campos de texto ausentes o inválidos → ""
    - cuit: debe coincidir con XX-XXXXXXXX-X o ""
    - date: debe coincidir con DD/MM/YYYY o ""
    - tx_match_confidence: float en [0.0, 1.0]
    """
    def to_float(value, default=0.0) -> float:
        try:
            return float(value) if value is not None else default
        except (TypeError, ValueError):
            return default

    def to_str(value, default="") -> str:
        if value is None:
            return default
        s = str(value).strip()
        return s if s else default

    cuit_raw = to_str(raw.get("cuit"))
    cuit = cuit_raw if CUIT_REGEX.match(cuit_raw) else ""

    date_raw = to_str(raw.get("date"))
    date = date_raw if DATE_REGEX.match(date_raw) else ""

    tx_confidence = to_float(raw.get("tx_match_confidence"), 0.0)
    tx_confidence = max(0.0, min(1.0, tx_confidence))

    return {
        "total_amount": to_float(raw.get("total_amount")),
        "iva_amount": to_float(raw.get("iva_amount")),
        "net_amount": to_float(raw.get("net_amount")),
        "iva_rate": to_str(raw.get("iva_rate")),
        "items": to_str(raw.get("items")),
        "tx_match_confidence": tx_confidence,
        "merchant_name": to_str(raw.get("merchant_name")),
        "cuit": cuit,
        "date": date,
        "receipt_number": to_str(raw.get("receipt_number")),
    }


def handler(event, context):
    """
    Handler principal de la Lambda data-extract.
    Extrae los campos fiscales del documento y devuelve el payload enriquecido.
    """
    processing_id = event.get("processing_id", "unknown")
    s3_key = event.get("s3_key", "")
    document_type = event.get("document_type", "")
    quality_score = event.get("quality_score")
    bucket = FACT_INPUT_BUCKET or event.get("bucket", "")

    logger.info(
        json.dumps({
            "processing_id": processing_id,
            "step": "data-extract",
            "action": "start",
            "s3_key": s3_key,
            "document_type": document_type,
            "quality_score": quality_score,
        })
    )

    try:
        # 1. Descargar documento de S3
        raw_bytes, media_type = _download_and_encode(bucket, s3_key)

        # 2. Invocar Bedrock para extraer campos fiscales
        raw_result = _invoke_bedrock(raw_bytes, media_type, document_type)

        # 3. Normalizar y validar campos
        extracted_data = _normalize_extracted_data(raw_result)

        logger.info(
            json.dumps({
                "processing_id": processing_id,
                "step": "data-extract",
                "action": "extracted",
                "tx_match_confidence": extracted_data["tx_match_confidence"],
                "cuit_found": bool(extracted_data["cuit"]),
                "date_found": bool(extracted_data["date"]),
            })
        )

        # 4. Devolver payload enriquecido
        return {
            **event,
            "extracted_data": extracted_data,
            "error": None,
        }

    except json.JSONDecodeError as exc:
        logger.error(
            json.dumps({
                "processing_id": processing_id,
                "step": "data-extract",
                "action": "error",
                "error_code": "EXTRACTION_ERROR",
                "detail": f"JSON parse error: {str(exc)}",
            })
        )
        return {
            **event,
            "extracted_data": None,
            "error": "EXTRACTION_ERROR",
        }

    except Exception as exc:  # noqa: BLE001
        logger.error(
            json.dumps({
                "processing_id": processing_id,
                "step": "data-extract",
                "action": "error",
                "error_code": "EXTRACTION_ERROR",
                "detail": str(exc),
            })
        )
        return {
            **event,
            "extracted_data": None,
            "error": "EXTRACTION_ERROR",
        }

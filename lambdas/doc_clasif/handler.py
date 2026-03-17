"""
Lambda: doc-clasif
Clasifica el tipo de documento fiscal invocando Amazon Bedrock (Claude 3.5 Sonnet).

Entrada (payload de Step Functions):
  { "processing_id": "...", "s3_key": "..." }

Salida (éxito):
  { "processing_id": "...", "s3_key": "...", "document_type": "factura_fiscal_b", ... }

Salida (rechazo):
  { "processing_id": "...", "s3_key": "...", "document_type": null, "error": "DOCUMENT_NOT_FISCAL" }
"""

import json
import base64
import logging
import os

import boto3
from botocore.config import Config

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Clientes AWS (inicializados fuera del handler para reutilización entre invocaciones)
s3_client = boto3.client("s3")
bedrock_client = boto3.client(
    "bedrock-runtime",
    config=Config(retries={"max_attempts": 3, "mode": "standard"}),
)

# Constantes
BEDROCK_MODEL_ID = os.environ.get(
    "BEDROCK_MODEL_ID", "anthropic.claude-3-5-sonnet-20241022-v2:0"
)
FACT_INPUT_BUCKET = os.environ.get("FACT_INPUT_BUCKET", "")

VALID_DOCUMENT_TYPES = {"factura_fiscal_a", "factura_fiscal_b", "ticket_fiscal"}

SYSTEM_PROMPT = """Eres un sistema experto en documentos fiscales argentinos. Conoces los formatos de:
- Factura tipo A: emitida entre responsables inscriptos, discrimina IVA, tiene CUIT del emisor y receptor
- Factura tipo B: emitida a consumidores finales, puede o no discriminar IVA, tiene CUIT del emisor
- Ticket fiscal: emitido por controlador fiscal, tiene número de controlador y CUIT del emisor

Siempre responde ÚNICAMENTE con JSON válido, sin texto adicional, sin markdown."""

CLASSIFICATION_PROMPT = """Analiza la imagen del documento fiscal argentino adjunto.
Determina el tipo de documento y responde con el siguiente JSON:

{
  "document_type": "factura_fiscal_a" | "factura_fiscal_b" | "ticket_fiscal" | "unknown",
  "confidence": 0.0-1.0,
  "reasoning": "breve explicación"
}"""


def _download_and_encode(bucket: str, key: str) -> tuple[bytes, str]:
    """Descarga el objeto de S3 y devuelve (bytes_raw, base64_string)."""
    response = s3_client.get_object(Bucket=bucket, Key=key)
    raw = response["Body"].read()
    # Determinar media type según extensión
    lower_key = key.lower()
    if lower_key.endswith(".pdf"):
        media_type = "application/pdf"
    elif lower_key.endswith(".png"):
        media_type = "image/png"
    else:
        media_type = "image/jpeg"
    return raw, media_type


def _invoke_bedrock(raw_bytes: bytes, media_type: str) -> dict:
    """Invoca Claude en Bedrock con el documento codificado y devuelve el JSON parseado."""
    # Construir el mensaje con el documento como contenido de imagen/documento
    if media_type == "application/pdf":
        # PDFs se envían como tipo "document"
        content_block = {
            "type": "document",
            "source": {
                "type": "base64",
                "media_type": "application/pdf",
                "data": base64.b64encode(raw_bytes).decode("utf-8"),
            },
        }
    else:
        # Imágenes (JPEG, PNG) se envían como tipo "image"
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
                    {"type": "text", "text": CLASSIFICATION_PROMPT},
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
    # Extraer el texto de la respuesta del modelo
    text_content = response_body["content"][0]["text"]
    # Extraer JSON aunque el modelo agregue texto antes/después
    start = text_content.find("{")
    end = text_content.rfind("}") + 1
    if start == -1 or end == 0:
        raise json.JSONDecodeError("No JSON found in response", text_content, 0)
    return json.loads(text_content[start:end])


def handler(event, context):
    """
    Handler principal de la Lambda doc-clasif.
    Recibe el payload del pipeline, clasifica el documento y devuelve el payload enriquecido.
    """
    processing_id = event.get("processing_id", "unknown")
    s3_key = event.get("s3_key", "")
    bucket = FACT_INPUT_BUCKET or event.get("bucket", "")

    # Si processing_id coincide con s3_key (viene del InputTransformer de EventBridge),
    # extraerlo del prefijo de la clave: "{processing_id}/{filename}"
    if processing_id == s3_key and "/" in s3_key:
        processing_id = s3_key.split("/")[0]

    logger.info(
        json.dumps({
            "processing_id": processing_id,
            "step": "doc-clasif",
            "action": "start",
            "s3_key": s3_key,
        })
    )

    try:
        # 1. Descargar documento de S3
        raw_bytes, media_type = _download_and_encode(bucket, s3_key)

        # 2. Invocar Bedrock para clasificar
        result = _invoke_bedrock(raw_bytes, media_type)

        document_type = result.get("document_type", "unknown")
        confidence = result.get("confidence", 0.0)
        reasoning = result.get("reasoning", "")

        logger.info(
            json.dumps({
                "processing_id": processing_id,
                "step": "doc-clasif",
                "action": "classified",
                "document_type": document_type,
                "confidence": confidence,
                "reasoning": reasoning,
            })
        )

        # 3. Validar que el tipo sea uno de los valores permitidos
        if document_type not in VALID_DOCUMENT_TYPES:
            logger.warning(
                json.dumps({
                    "processing_id": processing_id,
                    "step": "doc-clasif",
                    "action": "rejected",
                    "reason": "DOCUMENT_NOT_FISCAL",
                    "document_type_returned": document_type,
                })
            )
            return {
                **event,
                "processing_id": processing_id,
                "document_type": None,
                "quality_score": None,
                "extracted_data": None,
                "error": "DOCUMENT_NOT_FISCAL",
            }

        # 4. Devolver payload enriquecido
        return {
            **event,
            "processing_id": processing_id,
            "document_type": document_type,
            "quality_score": None,
            "extracted_data": None,
            "error": None,
        }

    except json.JSONDecodeError as exc:
        logger.error(
            json.dumps({
                "processing_id": processing_id,
                "step": "doc-clasif",
                "action": "error",
                "error_code": "CLASSIFICATION_ERROR",
                "detail": f"JSON parse error: {str(exc)}",
            })
        )
        return {
            **event,
            "processing_id": processing_id,
            "document_type": None,
            "quality_score": None,
            "extracted_data": None,
            "error": "CLASSIFICATION_ERROR",
        }

    except Exception as exc:  # noqa: BLE001
        logger.error(
            json.dumps({
                "processing_id": processing_id,
                "step": "doc-clasif",
                "action": "error",
                "error_code": "CLASSIFICATION_ERROR",
                "detail": str(exc),
            })
        )
        return {
            **event,
            "processing_id": processing_id,
            "document_type": None,
            "quality_score": None,
            "extracted_data": None,
            "error": "CLASSIFICATION_ERROR",
        }

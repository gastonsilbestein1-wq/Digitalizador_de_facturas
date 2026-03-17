"""
Lambda: presign
Genera URLs pre-firmadas para subida a S3 y asigna el processing_id único.
Requerimientos: 1.2, 1.3, 1.5, 1.6, 2.2
"""

import json
import logging
import os
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

# ── Configuración de logging estructurado ──────────────────────────────────
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── Constantes de validación ───────────────────────────────────────────────
MAX_FILE_SIZE = 10 * 1024 * 1024  # 10 MB (se comprime en el frontend antes de llegar a Bedrock)

CONTENT_TYPES_VALIDOS = {
    "application/pdf",
    "image/jpeg",
    "image/png",
}

EXTENSIONES_VALIDAS = {".pdf", ".jpg", ".jpeg", ".png"}

# ── Headers CORS requeridos en todas las respuestas ────────────────────────
CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Content-Type": "application/json",
}

# ── Clientes AWS (inicializados fuera del handler para reutilización) ──────
s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")


def _respuesta(status_code: int, body: dict) -> dict:
    """Construye una respuesta HTTP con headers CORS."""
    return {
        "statusCode": status_code,
        "headers": CORS_HEADERS,
        "body": json.dumps(body),
    }


def _validar_request(filename: str, content_type: str, file_size: int) -> str | None:
    """
    Valida los parámetros de la solicitud.
    Devuelve el código de error si hay un problema, o None si todo es válido.
    """
    # Validar tamaño del archivo
    if file_size > MAX_FILE_SIZE:
        return "FILE_TOO_LARGE"

    # Validar content_type
    if content_type not in CONTENT_TYPES_VALIDOS:
        return "INVALID_FORMAT"

    # Validar extensión del filename
    nombre_lower = filename.lower()
    if not any(nombre_lower.endswith(ext) for ext in EXTENSIONES_VALIDAS):
        return "INVALID_FORMAT"

    return None


def _generar_processing_id(tabla_nombre: str) -> tuple[str, str]:
    """
    Genera un processing_id único usando un contador atómico en DynamoDB.
    Devuelve (processing_id, timestamp_str).
    """
    # Obtener timestamp UTC actual
    ahora = datetime.now(timezone.utc)
    timestamp_str = ahora.strftime("%Y%m%dT%H%M%SZ")
    fecha_str = ahora.strftime("%Y%m%d")

    # Incremento atómico en DynamoDB tabla tx-counter
    tabla = dynamodb.Table(tabla_nombre)
    respuesta = tabla.update_item(
        Key={"date": fecha_str},
        UpdateExpression="ADD #c :uno",
        ExpressionAttributeNames={"#c": "counter"},
        ExpressionAttributeValues={":uno": 1},
        ReturnValues="UPDATED_NEW",
    )

    contador = int(respuesta["Attributes"]["counter"])

    # Construir processing_id: "YYYYMMDDTHHmmssZ-NNNNN"
    processing_id = f"{timestamp_str}-{contador:05d}"
    return processing_id, timestamp_str


def _generar_presigned_url(bucket: str, s3_key: str, content_type: str) -> str:
    """Genera una URL pre-firmada PUT para S3 con expiración de 300 segundos."""
    url = s3_client.generate_presigned_url(
        ClientMethod="put_object",
        Params={
            "Bucket": bucket,
            "Key": s3_key,
            "ContentType": content_type,
        },
        ExpiresIn=300,  # 5 minutos
    )
    return url


def _handle_status(processing_id: str) -> dict:
    """Consulta el estado de un processing_id en DynamoDB."""
    tabla_nombre = os.environ.get("PIPELINE_EXECUTIONS_TABLE", "pipeline-executions")
    output_bucket = os.environ.get("FACT_OUTPUT_BUCKET", "")
    try:
        tabla = dynamodb.Table(tabla_nombre)
        resp = tabla.get_item(Key={"processing_id": processing_id})
        item = resp.get("Item")
        if not item:
            return _respuesta(404, {"error": "NOT_FOUND"})

        status = item.get("status", "UNKNOWN")
        result = {"processing_id": processing_id, "status": status}

        if status == "COMPLETED":
            result["output_url"] = s3_client.generate_presigned_url(
                ClientMethod="get_object",
                Params={"Bucket": output_bucket, "Key": f"{processing_id}.json"},
                ExpiresIn=3600,  # 1 hora
            )
        elif status == "FAILED":
            result["error_code"] = item.get("error_code", "UNKNOWN_ERROR")

        return _respuesta(200, result)
    except Exception as exc:
        logger.error("Error consultando status: %s", str(exc))
        return _respuesta(500, {"error": "INTERNAL_ERROR"})


def handler(event, context):
    """
    Handler principal de la Lambda presign.
    POST /presign  → genera URL pre-firmada
    GET  /status/{processing_id} → consulta estado del pipeline
    """
    processing_id = None

    # Routing por método y path
    method = event.get("requestContext", {}).get("http", {}).get("method", "POST")
    path = event.get("rawPath", "/presign")

    if method == "GET" and "/status/" in path:
        pid = path.split("/status/")[-1].strip("/")
        return _handle_status(pid)

    try:
        # ── Parsear el body del evento ─────────────────────────────────────
        try:
            body = json.loads(event.get("body") or "{}")
        except (json.JSONDecodeError, TypeError) as exc:
            logger.error("Body JSON inválido: %s", exc)
            return _respuesta(400, {"error": "INVALID_REQUEST"})

        filename = body.get("filename", "")
        content_type = body.get("content_type", "")
        file_size = body.get("file_size", 0)

        # Validar que los campos requeridos estén presentes
        if not filename or not content_type or not isinstance(file_size, int):
            logger.error(
                "Campos requeridos faltantes o inválidos: filename=%s, "
                "content_type=%s, file_size=%s",
                filename, content_type, file_size,
            )
            return _respuesta(400, {"error": "INVALID_REQUEST"})

        logger.info(
            "Solicitud recibida: filename=%s, content_type=%s, file_size=%d",
            filename, content_type, file_size,
        )

        # ── Validar tamaño y formato ───────────────────────────────────────
        error_validacion = _validar_request(filename, content_type, file_size)
        if error_validacion:
            logger.warning(
                "Validación fallida: error=%s, filename=%s, file_size=%d",
                error_validacion, filename, file_size,
            )
            return _respuesta(400, {"error": error_validacion})

        # ── Obtener variables de entorno ───────────────────────────────────
        bucket = os.environ["FACT_INPUT_BUCKET"]
        tabla_contador = os.environ["TX_COUNTER_TABLE"]

        # ── Generar processing_id con contador atómico ─────────────────────
        processing_id, timestamp_str = _generar_processing_id(tabla_contador)
        logger.info(
            "processing_id generado: %s, timestamp=%s",
            processing_id, timestamp_str,
        )

        # ── Construir la clave S3 ──────────────────────────────────────────
        s3_key = f"{processing_id}/{filename}"

        # ── Generar URL pre-firmada PUT ────────────────────────────────────
        upload_url = _generar_presigned_url(bucket, s3_key, content_type)
        logger.info(
            "URL pre-firmada generada: processing_id=%s, s3_key=%s",
            processing_id, s3_key,
        )

        # ── Respuesta exitosa ──────────────────────────────────────────────
        return _respuesta(200, {
            "upload_url": upload_url,
            "processing_id": processing_id,
            "s3_key": s3_key,
        })

    except ClientError as exc:
        error_code = exc.response["Error"]["Code"]
        logger.error(
            "Error de AWS SDK: processing_id=%s, error_code=%s, detalle=%s",
            processing_id, error_code, str(exc),
        )
        return _respuesta(500, {"error": "INTERNAL_ERROR"})

    except Exception as exc:  # pylint: disable=broad-except
        logger.error(
            "Error inesperado: processing_id=%s, detalle=%s",
            processing_id, str(exc),
            exc_info=True,
        )
        return _respuesta(500, {"error": "INTERNAL_ERROR"})

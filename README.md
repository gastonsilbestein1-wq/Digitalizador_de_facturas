# Procesador de Documentos Fiscales Argentinos

Sistema serverless para procesar facturas y tickets fiscales argentinos (tipo A, B y tickets) usando un pipeline en AWS con Amazon Bedrock (Claude Sonnet 4.5).

## Arquitectura

```
Usuario (móvil / escritorio)
  └─► Web App (S3 + CloudFront)
        ├─► POST /presign  → Lambda presign → URL pre-firmada + processing_id
        │     └─► PUT archivo → S3 fact-input  (hasta 10 MB, compresión automática en browser)
        │           └─► EventBridge (s3:ObjectCreated)
        │                 └─► Step Functions (fiscal-pipeline)
        │                       ├─► Lambda doc-clasif   (Bedrock) → clasifica tipo
        │                       ├─► Lambda doc-eval     (Bedrock) → evalúa calidad
        │                       ├─► Lambda data-extract (Bedrock) → extrae 10 campos fiscales
        │                       └─► Lambda json-gen             → JSON en S3 fact-output
        │                                                          └─► DynamoDB pipeline-executions
        └─► GET /status/{processingId} → Lambda presign → estado + presigned URL del JSON
```

## Estructura del proyecto

```
fiscal-document-processor/
  infra/
    app.py            ← Entrypoint CDK
    stack.py          ← Stack CDK (toda la infraestructura)
    cdk.json          ← Configuración CDK
    requirements.txt  ← Dependencias Python del stack
    template.yaml     ← Equivalente CloudFormation (referencia, no usar para deploy)
    .venv/            ← Virtualenv (no commitear)
  lambdas/
    presign/          ← Genera URLs pre-firmadas, asigna processing_id, consulta estado
    doc_clasif/       ← Clasifica el tipo de documento fiscal (Bedrock)
    doc_eval/         ← Evalúa la calidad del documento (Bedrock)
    data_extract/     ← Extrae los campos fiscales (Bedrock)
    json_gen/         ← Genera y persiste el JSON de salida en fact-output
  frontend/
    index.html        ← Interfaz web (cámara + upload + polling de estado + link al JSON)
    config.js         ← URL base del endpoint API (se actualiza post-deploy)
  tests/
    __init__.py
  README.md
```

## Recursos AWS desplegados

| Recurso | Nombre | Descripción |
|---|---|---|
| S3 | `fact-input-{env}-{account}` | Bucket de entrada (expira en 7 días) |
| S3 | `fact-output-{env}-{account}` | Bucket de salida de JSONs (versionado, RETAIN) |
| S3 | `fact-webapp-{env}-{account}` | Bucket de la Web App estática |
| CloudFront | — | Distribución HTTPS para la Web App |
| API Gateway | `presign-api-{env}` | HTTP API: `POST /presign` y `GET /status/{id}` |
| Lambda | `presign-{env}` | Pre-firma + consulta de estado |
| Lambda | `doc-clasif-{env}` | Clasificación con Bedrock |
| Lambda | `doc-eval-{env}` | Evaluación de calidad con Bedrock |
| Lambda | `data-extract-{env}` | Extracción de datos con Bedrock |
| Lambda | `json-gen-{env}` | Generación del JSON fiscal |
| DynamoDB | `pipeline-executions` | Estado de cada ejecución |
| DynamoDB | `tx-counter` | Contador atómico para `processing_id` |
| Step Functions | `fiscal-pipeline-{env}` | Orquestador del pipeline |
| EventBridge | `fiscal-s3-upload-{env}` | Trigger S3 → Step Functions |
| CloudWatch Alarms | 4 alarmas | Errores Lambda y fallas del pipeline |

## Endpoints (entorno dev)

| Recurso | URL |
|---|---|
| Web App | `https://d1zqr78cj4w3ku.cloudfront.net` |
| API presign | `https://v19x1se7td.execute-api.us-east-1.amazonaws.com/presign` |
| API status | `https://v19x1se7td.execute-api.us-east-1.amazonaws.com/status/{processingId}` |
| Bucket entrada | `fact-input-dev-805472282641` |
| Bucket salida | `fact-output-dev-805472282641` |
| Step Function | `arn:aws:states:us-east-1:805472282641:stateMachine:fiscal-pipeline-dev` |
| CloudFront ID | `E7HVA8VATPFN6` |

## Requisitos previos

- AWS CLI configurado con credenciales válidas
- Node.js 18+ (para CDK CLI)
- Python 3.12+
- Acceso a Amazon Bedrock habilitado para el modelo `us.anthropic.claude-sonnet-4-5-20250929-v1:0` en `us-east-1`

> Los modelos Anthropic en Bedrock se activan automáticamente al primer uso. Para modelos de AWS Marketplace, un usuario con permisos de Marketplace debe invocarlo una vez para habilitarlo a nivel de cuenta.

## Despliegue con CDK (recomendado)

### 1. Instalar CDK CLI (una sola vez)

```bash
npm install -g aws-cdk
```

### 2. Crear el virtualenv e instalar dependencias

```bash
cd fiscal-document-processor/infra
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Bootstrap (una sola vez por cuenta/región)

```bash
cdk bootstrap aws://<account-id>/us-east-1
```

### 4. Desplegar

```bash
cdk deploy \
  --context env=dev \
  --context account=<account-id> \
  --context region=us-east-1 \
  --require-approval never
```

CDK empaqueta y sube automáticamente el código de las Lambdas y el frontend.

### 5. Actualizar config.js con la URL de la API

Después del deploy, copiar el valor del output `ApiEndpoint` en `frontend/config.js`:

```js
window.APP_CONFIG = {
  apiUrl: "https://<api-id>.execute-api.us-east-1.amazonaws.com"
};
```

Luego subir el archivo actualizado:

```bash
aws s3 cp frontend/config.js s3://fact-webapp-<env>-<account-id>/config.js
aws cloudfront create-invalidation --distribution-id <dist-id> --paths "/*"
```

## Eliminar el stack

```bash
cdk destroy \
  --context env=dev \
  --context account=<account-id> \
  --context region=us-east-1
```

> El bucket `fact-output` tiene `RemovalPolicy.RETAIN` para preservar los JSONs generados. Hay que vaciarlo y eliminarlo manualmente si se desea.

## Flujo del frontend

1. El usuario selecciona o fotografía un documento (JPG, PNG o PDF, hasta 10 MB)
2. Si la imagen supera 3.5 MB, el browser la comprime automáticamente antes de subir (siempre queda bajo el límite de 5 MB de Bedrock)
3. Se llama a `POST /presign` para obtener una URL pre-firmada y un `processing_id`
4. El archivo se sube directamente a S3 con `PUT`
5. El frontend hace polling a `GET /status/{processingId}` cada 3 segundos (máx. 2 minutos)
6. Al completar: muestra un link al JSON (presigned URL con 1 hora de expiración)
7. Si falla: muestra el código de error con descripción y sugerencias

## Formato del JSON de salida (fact-output)

```json
{
  "document_type":       "factura_fiscal_b",
  "total_amount":        324209.0,
  "iva_amount":          0.0,
  "net_amount":          0.0,
  "iva_rate":            "21.00",
  "items":               "",
  "tx_match_confidence": 0.8,
  "merchant_name":       "Empresa S.A.",
  "cuit":                "30-12345678-9",
  "date":                "17/03/2026",
  "receipt_number":      "0001-00012345",
  "quality_score":       1.0,
  "processing_id":       "20260317T153000Z-00001",
  "timestamp":           "2026-03-17T15:30:00Z"
}
```

## Códigos de error del pipeline

| Código | Paso | Descripción |
|---|---|---|
| `DOCUMENT_NOT_FISCAL` | doc-clasif | El documento no es un fiscal argentino válido |
| `CLASSIFICATION_ERROR` | doc-clasif | Error interno al invocar Bedrock |
| `QUALITY_BELOW_THRESHOLD` | doc-eval | `quality_score < 0.5` |
| `EVALUATION_ERROR` | doc-eval | Error interno al invocar Bedrock |
| `EXTRACTION_ERROR` | data-extract | Error interno al invocar Bedrock |
| `JSON_WRITE_ERROR` | json-gen | Fallo de escritura en S3 |
| `INVALID_PAYLOAD` | json-gen | JSON fiscal con campos faltantes o extra |

## Modelo Bedrock

Modelo activo: `us.anthropic.claude-sonnet-4-5-20250929-v1:0`

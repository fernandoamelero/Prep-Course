# Pipeline de Elasticsearch para logs SIACU (líneas resaltadas)

Este pipeline está pensado para líneas de log como las del pantallazo y **clasifica** cada evento como `request` o `response` usando las condiciones resaltadas (método/mensaje), además de preparar una `correlation_id` para luego calcular la latencia por transacción.

> Nota importante: un ingest pipeline procesa **un documento por vez**. Para diferencias de tiempo entre líneas distintas (request en una línea y response en otra), el cálculo final se hace en consulta (ES|QL) o en un proceso batch (Transform).

## 1) Ingest pipeline

```json
PUT _ingest/pipeline/siacu_postpago_latency
{
  "description": "Parseo SIACU + clasificación request/response con reglas de negocio resaltadas",
  "processors": [
    {
      "grok": {
        "field": "message",
        "ignore_failure": true,
        "patterns": [
          "^%{WORD:log.level} %{TIMESTAMP_ISO8601:event.original_ts} Hilo: \[%{INT:thread.id}\] Session: %{NOTSPACE:session.id} Transacci[oó]n: %{NOTSPACE:transaction.id} Usuario: %{NOTSPACE:user.name} Clase: %{DATA:java.class} M[eé]todo: %{NOTSPACE:method.name} Mensaje: %{GREEDYDATA:log.message}$"
        ]
      }
    },
    {
      "date": {
        "field": "event.original_ts",
        "target_field": "@timestamp",
        "formats": ["yyyy-MM-dd HH:mm:ss,SSS"],
        "timezone": "America/Bogota",
        "ignore_failure": true
      }
    },
    {
      "set": {
        "field": "event.dataset",
        "value": "siacu.postpago"
      }
    },
    {
      "script": {
        "lang": "painless",
        "source": """
          String metodo = ctx.containsKey('method') && ctx.method instanceof Map && ctx.method.containsKey('name') && ctx.method.name != null
            ? ctx.method.name.toString()
            : '';

          String msg = ctx.containsKey('log') && ctx.log instanceof Map && ctx.log.containsKey('message') && ctx.log.message != null
            ? ctx.log.message.toString()
            : '';

          String msgLower = msg.toLowerCase();

          boolean isRequest = false;
          boolean isResponse = false;

          // Condiciones request (resaltadas)
          if (msgLower.contains("requeststring") ||
              msgLower.contains("parametros de entrada") ||
              msgLower.contains("parámetros de entrada")) {
            isRequest = true;
          }

          // Condiciones response (resaltadas)
          if (msgLower.contains("responsestring") ||
              msgLower.contains("responsevalue") ||
              msgLower.contains("parametros de salida") ||
              msgLower.contains("parámetros de salida") ||
              msgLower.contains("datos de salida") ||
              msgLower.contains("objcustomerresponse") ||
              msgLower.contains("obtenerdatosclienteresponse")) {
            isResponse = true;
          }

          // Reglas extra por método resaltado
          if (metodo.equalsIgnoreCase("GetTypeProductPivotTobe") && msgLower.contains("responsevalue")) {
            isResponse = true;
          }
          if (metodo.equalsIgnoreCase("GetSegmentCustomerQuery") &&
              (msgLower.contains("parametros de salida") || msgLower.contains("parámetros de salida"))) {
            isResponse = true;
          }

          if (isRequest && !isResponse) {
            ctx.event = ctx.containsKey('event') && ctx.event instanceof Map ? ctx.event : new HashMap();
            ctx.event.type = "request";
          } else if (isResponse && !isRequest) {
            ctx.event = ctx.containsKey('event') && ctx.event instanceof Map ? ctx.event : new HashMap();
            ctx.event.type = "response";
          } else if (isRequest && isResponse) {
            ctx.event = ctx.containsKey('event') && ctx.event instanceof Map ? ctx.event : new HashMap();
            ctx.event.type = "request_response";
          }

          // Clave de correlación para unir request/response luego
          String sessionId = (ctx.containsKey('session') && ctx.session instanceof Map && ctx.session.containsKey('id') && ctx.session.id != null)
            ? ctx.session.id.toString()
            : "no-session";
          String txId = (ctx.containsKey('transaction') && ctx.transaction instanceof Map && ctx.transaction.containsKey('id') && ctx.transaction.id != null)
            ? ctx.transaction.id.toString()
            : "no-tx";

          ctx.correlation_id = sessionId + "|" + txId + "|" + metodo;
        """
      }
    }
  ]
}
```

## 2) Cálculo de diferencia de tiempo (consulta)

Una vez indexados los logs con el pipeline anterior, calcula la latencia por correlación:

```txt
FROM siacu-logs-*
| WHERE event.type IN ("request", "response")
| STATS
    request_ts = MIN(CASE(event.type == "request", @timestamp, NULL)),
    response_ts = MAX(CASE(event.type == "response", @timestamp, NULL))
  BY correlation_id, transaction.id, session.id, method.name
| EVAL diferencia_ms = DATE_DIFF("millisecond", request_ts, response_ts)
| WHERE request_ts IS NOT NULL AND response_ts IS NOT NULL
| SORT response_ts DESC
```

## 3) Simulación rápida

```json
POST _ingest/pipeline/siacu_postpago_latency/_simulate
{
  "docs": [
    {
      "_source": {
        "message": "SIACU INFO 2026-03-03 10:12:13,386 Hilo: [9260 ] Session: strGetDataCustomerTobeRest Transacción: 2026030310033258820 Usuario: siacu-backend Clase: Claro.Data.RestService Método: PostInvoque Mensaje: { requestString ... }"
      }
    },
    {
      "_source": {
        "message": "SIACU INFO 2026-03-03 10:12:14,511 Hilo: [9260 ] Session: strGetDataCustomerTobeRest Transacción: 2026030310033258820 Usuario: siacu-backend Clase: Claro.Data.RestService Método: PostInvoque Mensaje: { responseString ... }"
      }
    }
  ]
}
```

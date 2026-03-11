# Определение JSON-схемы сообщения от бэкенда
json_schema_str = """
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Message",
  "type": "object",
  "properties": {
    "id": {
      "type": "string"
    },
    "timestamp": {
      "type": "string"
    },
    "key": {
      "type": "string"
    },
    "msg": {
      "type": "string"
    }
  },
  "required": ["id", "timestamp", "key", "msg"]
}
"""
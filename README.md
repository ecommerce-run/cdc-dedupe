# CDC Deduplication

Intended to deduplicate ids from Redis stream filled up from Debezium.

Input: Redis stream with Debezium redis Compact or Excended format

Output: Redis stream with Lists of IDs, 1000 at once by default.



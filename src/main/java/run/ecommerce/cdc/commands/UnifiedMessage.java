package run.ecommerce.cdc.commands;

import org.springframework.data.redis.connection.stream.RecordId;

public record UnifiedMessage(RecordId id, String source, Integer entityId) {
}

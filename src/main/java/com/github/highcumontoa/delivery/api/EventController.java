package com.github.highcumontoa.delivery.api;

import com.github.highcumontoa.delivery.auth.TenantAuthService;
import com.github.highcumontoa.delivery.domain.DeliveryEvent;
import com.github.highcumontoa.delivery.domain.EventStatus;
import com.github.highcumontoa.delivery.service.SubmissionService;
import com.github.highcumontoa.delivery.store.EventStore;
import java.util.List;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/**
 * 事件提交/查询/重放 API。
 *
 * <p>所有端点要求 X-Tenant-Token 头，且令牌所属租户必须与路径租户一致；
 * 响应与日志均不包含令牌。
 */
@RestController
@RequestMapping("/tenants/{tenantId}/events")
public class EventController {

    private final SubmissionService submissionService;
    private final EventStore store;
    private final TenantAuthService authService;

    public EventController(SubmissionService submissionService, EventStore store, TenantAuthService authService) {
        this.submissionService = submissionService;
        this.store = store;
        this.authService = authService;
    }

    public record SubmitRequest(String idempotencyKey, String aggregateKey, String payload) {
    }

    public record EventView(String eventId, String tenantId, String idempotencyKey,
                            String aggregateKey, long sequence, EventStatus status,
                            int attemptCount, String lastFailureKind, String lastError) {

        static EventView of(DeliveryEvent e) {
            return new EventView(e.getEventId(), e.getTenantId(), e.getIdempotencyKey(),
                    e.getAggregateKey(), e.getSequence(), e.getStatus(), e.getAttemptCount(),
                    e.getLastFailureKind() == null ? null : e.getLastFailureKind().name(),
                    e.getLastError());
        }
    }

    @PostMapping
    public ResponseEntity<EventView> submit(@PathVariable String tenantId,
                                            @RequestHeader(value = "X-Tenant-Token", required = false) String token,
                                            @RequestBody SubmitRequest request) {
        String tenant = authService.authorize(token, tenantId);
        DeliveryEvent event = submissionService.submit(
                tenant, request.idempotencyKey(), request.aggregateKey(), request.payload());
        return ResponseEntity.status(HttpStatus.CREATED).body(EventView.of(event));
    }

    @GetMapping("/{eventId}")
    public EventView get(@PathVariable String tenantId,
                         @PathVariable String eventId,
                         @RequestHeader(value = "X-Tenant-Token", required = false) String token) {
        String tenant = authService.authorize(token, tenantId);
        return store.findById(tenant, eventId)
                .map(EventView::of)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "event not found"));
    }

    /** 按状态查询，如 ?status=FAILED 查询可重放的失败事件。 */
    @GetMapping
    public List<EventView> listByStatus(@PathVariable String tenantId,
                                        @RequestParam EventStatus status,
                                        @RequestHeader(value = "X-Tenant-Token", required = false) String token) {
        String tenant = authService.authorize(token, tenantId);
        return store.findByStatus(tenant, status).stream().map(EventView::of).toList();
    }

    /** 显式重放 FAILED 事件：回到 PENDING 重新进入投递流程。 */
    @PostMapping("/{eventId}/replay")
    public Map<String, Object> replay(@PathVariable String tenantId,
                                      @PathVariable String eventId,
                                      @RequestHeader(value = "X-Tenant-Token", required = false) String token) {
        String tenant = authService.authorize(token, tenantId);
        boolean replayed = store.replay(tenant, eventId);
        if (!replayed) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "event not in FAILED status or not found");
        }
        return Map.of("eventId", eventId, "status", EventStatus.PENDING.name());
    }
}

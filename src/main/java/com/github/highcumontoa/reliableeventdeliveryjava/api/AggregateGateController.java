package com.github.highcumontoa.reliableeventdeliveryjava.api;

import com.github.highcumontoa.reliableeventdeliveryjava.api.dto.GateStateRequest;
import com.github.highcumontoa.reliableeventdeliveryjava.api.dto.GateViewResponse;
import com.github.highcumontoa.reliableeventdeliveryjava.security.TenantContext;
import com.github.highcumontoa.reliableeventdeliveryjava.service.AggregateGateService;
import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** 聚合闸门 API：暂停/恢复/查询。租户只来自认证上下文，跨租户操作被拒绝。 */
@RestController
@RequestMapping("/api/gates")
public class AggregateGateController {

    private final AggregateGateService service;

    public AggregateGateController(AggregateGateService service) {
        this.service = service;
    }

    @PostMapping("/{aggregateKey}/pause")
    public GateViewResponse pause(@PathVariable String aggregateKey,
                                  @RequestBody(required = false) GateStateRequest request) {
        return service.pause(TenantContext.get(), aggregateKey, request);
    }

    @PostMapping("/{aggregateKey}/resume")
    public GateViewResponse resume(@PathVariable String aggregateKey,
                                   @RequestBody(required = false) GateStateRequest request) {
        return service.resume(TenantContext.get(), aggregateKey, request);
    }

    @GetMapping("/{aggregateKey}")
    public GateViewResponse get(@PathVariable String aggregateKey) {
        return service.get(TenantContext.get(), aggregateKey);
    }

    @GetMapping
    public List<GateViewResponse> list(@RequestParam(required = false) String state) {
        return service.list(TenantContext.get(), state);
    }
}

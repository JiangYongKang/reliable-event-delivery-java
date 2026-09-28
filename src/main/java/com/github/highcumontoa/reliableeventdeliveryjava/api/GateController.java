package com.github.highcumontoa.reliableeventdeliveryjava.api;

import com.github.highcumontoa.reliableeventdeliveryjava.api.dto.GateView;
import com.github.highcumontoa.reliableeventdeliveryjava.domain.ErrorCode;
import com.github.highcumontoa.reliableeventdeliveryjava.security.TenantContext;
import com.github.highcumontoa.reliableeventdeliveryjava.service.GateService;
import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 聚合闸门 API。路径中的 tenantId 必须与认证租户一致，
 * 跨租户操作返回 403 TENANT_FORBIDDEN（与其它拒绝原因可区分）。
 */
@RestController
@RequestMapping("/api/gates")
public class GateController {

    private final GateService service;

    public GateController(GateService service) {
        this.service = service;
    }

    @PutMapping("/{tenantId}/{aggregateKey}/pause")
    public GateView pause(@PathVariable String tenantId, @PathVariable String aggregateKey) {
        return service.pause(requireOwnTenant(tenantId), aggregateKey);
    }

    @PutMapping("/{tenantId}/{aggregateKey}/resume")
    public GateView resume(@PathVariable String tenantId, @PathVariable String aggregateKey) {
        return service.resume(requireOwnTenant(tenantId), aggregateKey);
    }

    @GetMapping("/{tenantId}/{aggregateKey}")
    public GateView get(@PathVariable String tenantId, @PathVariable String aggregateKey) {
        return service.get(requireOwnTenant(tenantId), aggregateKey);
    }

    @GetMapping("/{tenantId}")
    public List<GateView> listNonOpen(@PathVariable String tenantId) {
        return service.listNonOpen(requireOwnTenant(tenantId));
    }

    private static String requireOwnTenant(String tenantId) {
        String current = TenantContext.get();
        if (!current.equals(tenantId)) {
            throw new ApiException(ErrorCode.TENANT_FORBIDDEN, 403,
                    "cross-tenant gate operation is not allowed");
        }
        return current;
    }
}

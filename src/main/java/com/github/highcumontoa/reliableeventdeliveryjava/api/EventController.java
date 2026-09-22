package com.github.highcumontoa.reliableeventdeliveryjava.api;

import com.github.highcumontoa.reliableeventdeliveryjava.api.dto.EventView;
import com.github.highcumontoa.reliableeventdeliveryjava.api.dto.SubmitEventRequest;
import com.github.highcumontoa.reliableeventdeliveryjava.api.dto.SubmitEventResponse;
import com.github.highcumontoa.reliableeventdeliveryjava.security.TenantContext;
import com.github.highcumontoa.reliableeventdeliveryjava.service.EventService;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/** 事件提交/查询/重放 API。租户只来自认证上下文，客户端无法指定他人租户。 */
@RestController
@RequestMapping("/api/events")
public class EventController {

    private final EventService service;

    public EventController(EventService service) {
        this.service = service;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public SubmitEventResponse submit(@RequestBody SubmitEventRequest request) {
        return service.submit(TenantContext.get(), request);
    }

    @GetMapping
    public List<EventView> list() {
        return service.list(TenantContext.get());
    }

    @GetMapping("/{id}")
    public EventView get(@PathVariable String id) {
        return service.get(TenantContext.get(), id);
    }

    @PostMapping("/{id}/replay")
    public EventView replay(@PathVariable String id) {
        return service.replay(TenantContext.get(), id);
    }
}

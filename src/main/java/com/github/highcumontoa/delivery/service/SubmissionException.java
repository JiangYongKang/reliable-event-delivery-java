package com.github.highcumontoa.delivery.service;

/** 提交被拒绝。reason 可区分：幂等键内容冲突 / 积压超限。 */
public class SubmissionException extends RuntimeException {

    public enum Reason {
        IDEMPOTENCY_CONFLICT,
        BACKPRESSURE_LIMIT
    }

    private final Reason reason;

    public SubmissionException(Reason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public Reason getReason() {
        return reason;
    }
}

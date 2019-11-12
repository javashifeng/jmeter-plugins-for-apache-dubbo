package io.github.ningyu.jmeter.plugin.dubbo.exception;

import io.github.ningyu.jmeter.plugin.util.ErrorCode;

/**
 * @author wangshifeng
 * @date 2019-11-12 11:06
 */
public class BusinessException extends Exception {
    /**
     * 错误码
     */
    private ErrorCode errorCode;

    private static final long serialVersionUID = -8213943026163641747L;

    public BusinessException(ErrorCode errorCode) {
        super();
        this.errorCode = errorCode;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(ErrorCode errorCode) {
        this.errorCode = errorCode;
    }
}

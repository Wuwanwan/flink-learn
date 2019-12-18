package com.learn.entity;

import lombok.Data;

/**
 * 身份信息事件
 *
 * @author wuww
 * @version 1.0
 */
@Data
public class IdentityEvent {

    /**
     * 身份唯一标识（身份证号）
     */
    private String identity;
    /**
     * 时间
     */
    private Long time;

}

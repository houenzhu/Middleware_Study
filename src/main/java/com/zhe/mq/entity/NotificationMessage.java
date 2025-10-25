package com.zhe.mq.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serial;
import java.io.Serializable;
import java.util.Date;

/**
 * @version 1.0
 * @Author 朱厚恩
 */

@Data
@NoArgsConstructor
public class NotificationMessage implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private String id;
    private Long userId;
    private String title;
    private String content;
    private Integer type;      // 1=系统通知, 2=私信, 3=公告
    private Integer status;    // 0=未读, 1=已读
    private Date createTime;
    private String sender;
    private String extraData;

    public NotificationMessage(Long userId, String title, String content, String sender) {
        this.userId = userId;
        this.title = title;
        this.content = content;
        this.sender = sender;
        this.createTime = new Date();
        this.type = 1;
        this.status = 0;
    }
}

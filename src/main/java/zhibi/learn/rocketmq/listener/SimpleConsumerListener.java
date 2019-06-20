package zhibi.learn.rocketmq.listener;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * @author 执笔
 * @date 2019/6/20 17:16
 */
@Slf4j
@Component
@RocketMQMessageListener(consumerGroup = "group_1", topic = "topic_1")
public class SimpleConsumerListener implements RocketMQListener<MessageExt> {
    @Override
    public void onMessage(MessageExt message) {
        log.info("tag_1  接受到消息：  {}  -- {}  -- {}", message.getTopic(), message.getTags(), new String(message.getBody(), StandardCharsets.UTF_8));
    }
}

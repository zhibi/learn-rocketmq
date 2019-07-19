package zhibi.learn.rocketmq.listener;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * 事务消息消费者
 *
 * @author 执笔
 * @date 2019/6/20 17:16
 */
@Component
@Slf4j
@RocketMQMessageListener(consumerGroup = "group_tra", topic = "topic_tra")
public class TransactionConsumerListener implements RocketMQListener<MessageExt> {
    @Override
    public void onMessage(MessageExt message) {
        log.info("group_tra  接受到消息：  {}  -- {}  -- {}", message.getTopic(), message.getTags(), new String(message.getBody(), StandardCharsets.UTF_8));
    }
}

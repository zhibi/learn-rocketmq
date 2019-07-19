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
@RocketMQMessageListener(consumerGroup = "group_tra2", topic = "topic_tra")
public class TransactionConsumerListener2 implements RocketMQListener<MessageExt> {
    @Override
    public void onMessage(MessageExt message) {
        log.info("group_tra2  接受到消息：  {}  -- {}  -- {}", message.getTopic(), message.getTags(), new String(message.getBody(), StandardCharsets.UTF_8));
        // 出现异常以后会重复接受
        throw new RuntimeException("123");
    }
}

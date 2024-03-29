package zhibi.learn.rocketmq.listener;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.MessageModel;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * consumerGroup 用来做负载均衡 同一个 consumerGroup 下面的只会有其中一个收到消息
 * BROADCASTING 模式下同一个组的汇全部接收到
 * @author 执笔
 * @date 2019/6/20 17:16
 */
@Slf4j
@Component
@RocketMQMessageListener(consumerGroup = "group_1", topic = "topic_2", selectorExpression = "tag_1", messageModel = MessageModel.BROADCASTING)
public class SimpleConsumerListener implements RocketMQListener<MessageExt> {
    @Override
    public void onMessage(MessageExt message) {
        log.info("  接受到消息：  {}  -- {}  -- {}", message.getTopic(), message.getTags(), new String(message.getBody(), StandardCharsets.UTF_8));
    }
}

package zhibi.learn.rocketmq.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * 消费者 简单的
 *
 * @author 执笔
 * @date 2019/6/20 15:48
 */
@Slf4j
public class ConsumerSimple2 {

    public static void main(String[] args) throws MQClientException {
        /**
         * 创 建一个 DefaultMQPushConsumer ，设 置好 GroupName
         * NameServer 地址后启动，然后 把待发送的消息拼装成 Message 对象，使
         * Producer 发送
         */
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("group_1");
        consumer.setNamesrvAddr("127.0.0.1:9876");
        // 从头消费
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe("topic_5", "*");
        /**
         * 在 Broadcasting 模式下，同 ConsumerGroup 里的每个 Consumer
         * 能消费到所订阅 Topic 的全部消息，也就是一个消息会被多次分发
         *
         * Clustering 模式下，同 ConsumerGroup ( GroupN ame 相同 里的
         * Consumer 只消费所订阅消 一部分 容， 同一个 ConsumerGroup
         * 所有的 Consumer 的内 容合 来才是所订阅 Topic
         * 从而达到负载均衡的目的
         */
        consumer.setMessageModel(MessageModel.CLUSTERING);
        consumer.setInstanceName("1");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                log.info("{} -- {}", msgs, context);
                for (MessageExt ext : msgs) {
                    log.info("   " + ext.getMsgId() + "---" + new String(ext.getBody(), StandardCharsets.UTF_8));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        // 启动
        consumer.start();
    }
}

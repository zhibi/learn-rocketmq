package zhibi.learn.rocketmq.runner;

import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

/**
 * 启动发送mq消息
 *
 * @author 执笔
 * @date 2019/7/19 10:53
 */
@Component
public class ProducerSendRunner implements ApplicationRunner {

    @Autowired
    private RocketMQTemplate rocketMQTemplate;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        for (int i = 0; i < 10; i++) {
            Message msg = MessageBuilder.withPayload("Hello RocketMQ 消息" + i).build();
            rocketMQTemplate.sendMessageInTransaction("txProducerGroup", "topic_tra:*", msg, i);
        }

    }
}

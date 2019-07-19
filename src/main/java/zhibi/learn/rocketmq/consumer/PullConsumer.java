package zhibi.learn.rocketmq.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.Set;

/**
 * pull 模式
 *
 * @author 执笔
 * @date 2019/7/19 15:13
 */
@Slf4j
public class PullConsumer {

    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("group_3");
        consumer.setNamesrvAddr("127.0.0.1:9876");
        consumer.start();
        Set<MessageQueue> messageQueues = consumer.fetchSubscribeMessageQueues("topic_1");
        for (MessageQueue messageQueue : messageQueues) {
            log.info(messageQueue.getQueueId() + " -- " + messageQueue);
            PullResult pullResult = consumer.pull(messageQueue, "", 0L, 1024);
            log.info(" 消息 {} ", pullResult.getMsgFoundList());
        }
        consumer.shutdown();
    }
}

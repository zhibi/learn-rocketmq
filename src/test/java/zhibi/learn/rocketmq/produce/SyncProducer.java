package zhibi.learn.rocketmq.produce;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.Test;

import java.io.UnsupportedEncodingException;

/**
 * sync 方式发送
 *
 * @author 执笔
 * @date 2019/6/20 14:59
 */
@Slf4j
public class SyncProducer {

    public static void send(String tag, String body) throws MQClientException, UnsupportedEncodingException, RemotingException, InterruptedException, MQBrokerException {
        /**
         * 主要 流程 ：创 建一个 DefaultMQProducer ，设 置好 GroupName
         * NameServer 地址后启动，然后 把待发送的消息拼装成 Message 对象，使
         * Producer 发送
         */
        DefaultMQProducer producer = new DefaultMQProducer("group_1");
        producer.setNamesrvAddr("172.20.111.110:9876");
        // 启动生产者
        producer.start();
        // 发送消息
        Message    message    = new Message("topic_1", tag, body.getBytes(RemotingHelper.DEFAULT_CHARSET));
        SendResult sendResult = producer.send(message);
        log.info("发送消息  {}", sendResult);
        // 关闭生产者
        producer.shutdown();
    }

    @Test
    public void test_1() throws InterruptedException, RemotingException, MQClientException, MQBrokerException, UnsupportedEncodingException {
        send("tag_1","123");
    }

    @Test
    public void test_2() throws InterruptedException, RemotingException, MQClientException, MQBrokerException, UnsupportedEncodingException {
        send("tag_2","123789");
    }
}

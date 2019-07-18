package zhibi.learn.rocketmq.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;

/**
 * 延迟发送
 * @author 执笔
 * @date 2019/7/18 15:48
 */
@Slf4j
public class DelayTimeProducer {

    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException, UnsupportedEncodingException {
        DefaultMQProducer producer = new DefaultMQProducer("group_999");
        producer.setNamesrvAddr("127.0.0.1:9876");
        // 设置 lnstanceName ，当一个 Jv 需要启动多个 Producer 的时候，通过
        // 设置不同的 InstanceName 来区分，不设置的话系统使用默认名称“DEFAULT
        producer.setInstanceName("second");
        // 失败以后重试次数
        producer.setRetryTimesWhenSendFailed(5);
        // 启动生产者
        producer.start();
        // 发送消息
        for (int i = 0; i < 1; i++) {
            Message    message    = new Message("topic_6", "*", ("消息" + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            // 延时消息 延时 1分钟
            // 1 s/5s/1Os/30s/1m/2m/3m/4m/5m/6m/7m/8m/9m/1Om/20m/30m/1h 2h
            message.setDelayTimeLevel(5);
            SendResult sendResult = producer.send(message);
            log.info("发送消息  {}", sendResult);
        }
        // 关闭生产者
        producer.shutdown();
    }
}

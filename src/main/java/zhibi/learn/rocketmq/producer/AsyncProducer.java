package zhibi.learn.rocketmq.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;

/**
 * 异步 方式发送
 *
 * @author 执笔
 * @date 2019/6/20 14:59
 */
@Slf4j
public class AsyncProducer {

    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, RemotingException, InterruptedException {
        /**
         * 主要 流程 ：创 建一个 DefaultMQProducer ，设 置好 GroupName
         * NameServer 地址后启动，然后 把待发送的消息拼装成 Message 对象，使
         * Producer 发送
         */
        DefaultMQProducer producer = new DefaultMQProducer("group_999");
        producer.setNamesrvAddr("127.0.0.1:9876");
        // 设置 lnstanceName ，当一个 Jv 需要启动多个 Producer 的时候，通过
        // 设置不同的 InstanceName 来区分，不设置的话系统使用默认名称“DEFAULT
        producer.setInstanceName("first2");
        // 失败以后重试次数
        producer.setRetryTimesWhenSendFailed(5);
        // 启动生产者
        producer.start();
        // 发送消息
        for (int i = 0; i < 1; i++) {
            Message message = new Message("topic_6", "*", ("消息" + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            // 异步处理结果
            producer.send(message, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    log.info("发送消息  {}", sendResult);
                }

                @Override
                public void onException(Throwable e) {
                    log.info("发送消息  {}", e);
                }
            });
        }
        // 关闭生产者
        producer.shutdown();
    }
}

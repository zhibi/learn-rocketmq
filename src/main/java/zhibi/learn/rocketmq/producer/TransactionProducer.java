package zhibi.learn.rocketmq.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import zhibi.learn.rocketmq.transactional.SimpleTransactionListener;

import java.nio.charset.StandardCharsets;

/**
 * 事务消息生产者
 *
 * @author 执笔
 * @date 2019/7/19 10:00
 */
@Slf4j
public class TransactionProducer {

    public static void main(String[] args) throws MQClientException {
        TransactionMQProducer transactionMQProducer = new TransactionMQProducer("TransactionGroup");
        transactionMQProducer.setNamesrvAddr("127.0.0.1:9876");
        transactionMQProducer.setInstanceName("TransactionProducer");
        transactionMQProducer.setRetryTimesWhenSendFailed(5);
        transactionMQProducer.start();

        // 事务接听器
        TransactionListener transactionListener = new SimpleTransactionListener();
        transactionMQProducer.setTransactionListener(transactionListener);
        // 发送消息
        for (int i = 0; i < 10; i++) {
            Message message = new Message("topic_tran", ("message-" + i).getBytes(StandardCharsets.UTF_8));
            transactionMQProducer.sendMessageInTransaction(message, i);
        }
        transactionMQProducer.shutdown();
    }
}

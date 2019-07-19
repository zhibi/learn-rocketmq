package zhibi.learn.rocketmq.listener;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionState;
import org.springframework.messaging.Message;

/**
 * @author 执笔
 * @date 2019/7/19 10:48
 */
@RocketMQTransactionListener(txProducerGroup = "txProducerGroup")
@Slf4j
public class TransactionalProducerListener implements RocketMQLocalTransactionListener {
    /**
     * 实现执行本地事务的逻辑,并返回本地事务执行状态
     *
     * @param msg
     * @param arg
     * @return
     */
    @Override
    public RocketMQLocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        log.info("executeLocalTransaction  {}", arg);
        int index = (int) arg;
        if (index % 3 == 0) {
            return RocketMQLocalTransactionState.UNKNOWN;
        } else if (index % 3 == 1) {
            return RocketMQLocalTransactionState.COMMIT;
        }
        return RocketMQLocalTransactionState.ROLLBACK;
    }

    /**
     * 本地消息状态回差
     * @param msg
     * @return
     */
    @Override
    public RocketMQLocalTransactionState checkLocalTransaction(Message msg) {
        return RocketMQLocalTransactionState.COMMIT;
    }
}

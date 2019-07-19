package zhibi.learn.rocketmq.transactional;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * 事务接听器
 *
 * @author 执笔
 * @date 2019/7/19 10:13
 */
@Slf4j
public class SimpleTransactionListener implements TransactionListener {
    /**
     * 执行本地事务
     *
     * @param msg
     * @param arg
     * @return
     */
    @Override
    public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        log.info("executeLocalTransaction  {}", arg);
        int index = (int) arg;
        if (index % 3 == 0) {
            return LocalTransactionState.UNKNOW;
        } else if (index % 3 == 1) {
            return LocalTransactionState.COMMIT_MESSAGE;
        }
        return LocalTransactionState.ROLLBACK_MESSAGE;
    }

    /**
     * 校验本地事务是否执行成功
     *
     * @param msg
     * @return
     */
    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt msg) {
        log.info("checkLocalTransaction  {}", msg);
        return LocalTransactionState.COMMIT_MESSAGE;
    }
}

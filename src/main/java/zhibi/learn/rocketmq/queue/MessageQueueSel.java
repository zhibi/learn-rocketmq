package zhibi.learn.rocketmq.queue;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.List;

/**
 * 选择消息发送的队列
 *
 * @author 执笔
 * @date 2019/7/18 16:00
 */
@Slf4j
public class MessageQueueSel implements MessageQueueSelector {
    @Override
    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
        log.info(JSONObject.toJSONString(msg));
        log.info(JSONObject.toJSONString(arg));
        // 选择哪个 队列
        int id          = Integer.parseInt(arg.toString());
        int size        = mqs.size();
        int index       = id % size;
        System.out.println(index);
        return mqs.get(index);
    }
}

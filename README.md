# PubSub

本项目的灵感来源于: [lileio/pubsub](github.com/lileio/pubsub), 吸取了其动态方法以及中间件的思路并提供配置的支持以应用不同的消息队列所需的配置.

支持的消息队列:
- RocketMq生态: AliYun RocketMQ V4; Apache RocketMQ V4

TODO:

- 事务消息处理.

## 生态比较

| 队列类型               | 描述                | 消息重试 | 死信队列 |
|--------------------|-------------------|------|------|
| AliYun RocketMq V4 | 由阿里云开发的SDK | N    | N    |
| Apache RocketMq V4 | 由Apache提供的SDK     | Y    | Y    |

### AliYun RocketMq V4

其生命周期已经结束

由于不支持死信队列, 在消费失败处理上,需要由调用方自行处理消费失败事件.
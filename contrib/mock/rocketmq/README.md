# RocketMQ Mock

为方便消息的测试, 我们提供了 RocketMQ Mock. 结合Docker环境, 在集成测试中启动了MQ以方便单元测试

## 环境配置

基础都需要Docker环境

### windows

wsl2: 环境为 2.3 
docker engine: 27.3.1

调试`.wslconfig`如下:
  
```
[wsl2]
networkingMode=mirrored
dnsTunneling=true
firewall=true
autoProxy=true

[experimental]
autoMemoryReclaim=gradual  
hostAddressLoopback=true
```

由于RocketMQ 对Docker化的Broker容器有配置要求,需要采用本机的IP能成完成正常启动, 
在配置文件目录中添加`.env.local`环境变量文件.

```
rocketmq_host={你的本机IP}
```

你也可以在代码中自行设置`rocketmq_host`环境变量`os.Setenv("rocketmq_host", {本机IP})`


## 命令

```shell
./mqadmin sendMessage -t Trading_Notify -n namesrv:9876 -k key -c tag -p 
```

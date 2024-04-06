--参数列表
--优惠券ID
local voucherId=ARGV[1]
--用户ID
local userId=ARGV[2]
--订单ID
local orderId=ARGV[3]

--KEY
--库存key
local stockKey='seckill:stock:'..voucherId
--订单key
local orderKey='seckill:order:'..voucherId

--脚本业务
--判断库存
if(tonumber(redis.call('get',stockKey))<=0) then
    --库存不足返回
    return 1
end
--判断用户下单
if(redis.call('sismember',orderKey,userId)==1) then
    --重复下单返回2
    return 2
end

--扣库存
redis.call('incrby',stockKey,-1)
--下单
redis.call('sadd',orderKey,userId)
--发送消息到队列中, XADD STREAM.ORDERS * K1 V1 K2 V2
-- redis.call('xadd','stream.orders','*','userId',userId,'voucherId',voucherId,'id',orderId)

return 0


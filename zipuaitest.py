import zai
print(zai.__version__)
from zai import ZhipuAiClient

# 初始化客户端
client = ZhipuAiClient(api_key="b5ab4ae4b94e4c0387ff8ef07c329795.SKNjdpbq6o9K1HOA")

# 创建聊天完成请求
response = client.chat.completions.create(
    model="glm-5",
    messages=[
        {
            "role": "system",
            "content": ""
        },
        {
            "role": "user",
            "content": "你好，请介绍一下自己。"
        }
    ],
    temperature=0.6
)

# 获取回复
print(response.choices[0].message.content)
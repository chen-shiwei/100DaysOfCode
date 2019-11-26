# MapReduce中文版
```
一个编程模型，处理和生成超大数据集的算法模型的相关实现。
map函数 创建 key/value 键值对的数据集合，输出 key/value 键值对的数据集合；
reduce函数 合并具有相同中间key值的中间value值。
```

## 应用场景
- web页面请求记录
    Map 处理日志中的请求记录 输出(URL,1)
    Reduce 把相同的URL的 value 值都累加起来 输出(URL,记录总数)
    
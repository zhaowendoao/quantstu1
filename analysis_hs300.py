import akshare as ak
import pandas as pd
import numpy as np
import random
import time
import os
import json
import requests

# ===================== 1. 基础配置（缩减样本到10只） =====================
# 创建所需目录
os.makedirs("raw_data", exist_ok=True)
os.makedirs("clean_data", exist_ok=True)
os.makedirs("final_data", exist_ok=True)
"""
#  获取沪深300最新成分股
#hs300_stocks = ak.index_stock_cons(symbol="000300")
#hs300_stocks.to_csv("raw_data/raw_沪深300成分股.csv", index=False, encoding="utf-8-sig")
hs300_stocks=pd.read_csv("raw_data/raw_沪深300成分股.csv", encoding="utf-8-sig")
#print(hs300_stocks)

# 补全6位代码并去重，只取前10只
all_code_list = hs300_stocks["品种代码"].astype(str).str.zfill(6).unique().tolist()
stock_code_list=random.sample(all_code_list, 10)



# 保存到文件
with open("raw_data/stock_code_list.json", "w", encoding="utf-8") as f:
    json.dump(stock_code_list, f, ensure_ascii=False, indent=4)

print("已保存到 stock_code_list.json")

"""
# ============ 读取 ============
with open("raw_data/stock_code_list.json", "r", encoding="utf-8") as f:
    loaded_list = json.load(f)

print(f"选取的沪深300成分股（10只）：{loaded_list}")
print(f"HTTP_PROXY: {os.environ.get('HTTP_PROXY')}")
print(f"HTTPS_PROXY: {os.environ.get('HTTPS_PROXY')}")




# ===================== 2. 批量获取日线行情数据 =====================

# ... existing code ...


def get_stock_daily(stock_code="000001", start="2025-01-01", end="2025-12-31"):
    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    secid = f"1.{stock_code}" if stock_code.startswith("6") else f"0.{stock_code}"
    
    params = {
        "secid": secid,
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": "101",  # 日线
        "fqt": "1",    # 前复权
        "beg": start,
        "end": end,
    }
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://quote.eastmoney.com/"
    }

    resp = requests.get(url, params=params, headers=headers)
    data_json = resp.json()

    if not (data_json["data"] and data_json["data"]["klines"]):
        return pd.DataFrame()
    temp_df = pd.DataFrame([item.split(",") for item in data_json["data"]["klines"]])
    temp_df["股票代码"] = stock_code
    temp_df.columns = [
        "日期",
        "开盘",
        "收盘",
        "最高",
        "最低",
        "成交量",
        "成交额",
        "振幅",
        "涨跌幅",
        "涨跌额",
        "换手率",
        "股票代码",
    ]
    temp_df["日期"] = pd.to_datetime(temp_df["日期"], errors="coerce").dt.date
    temp_df["开盘"] = pd.to_numeric(temp_df["开盘"], errors="coerce")
    temp_df["收盘"] = pd.to_numeric(temp_df["收盘"], errors="coerce")
    temp_df["最高"] = pd.to_numeric(temp_df["最高"], errors="coerce")
    temp_df["最低"] = pd.to_numeric(temp_df["最低"], errors="coerce")
    temp_df["成交量"] = pd.to_numeric(temp_df["成交量"], errors="coerce")
    temp_df["成交额"] = pd.to_numeric(temp_df["成交额"], errors="coerce")
    temp_df["振幅"] = pd.to_numeric(temp_df["振幅"], errors="coerce")
    temp_df["涨跌幅"] = pd.to_numeric(temp_df["涨跌幅"], errors="coerce")
    temp_df["涨跌额"] = pd.to_numeric(temp_df["涨跌额"], errors="coerce")
    temp_df["换手率"] = pd.to_numeric(temp_df["换手率"], errors="coerce")
    temp_df = temp_df[
        [
            "日期",
            "股票代码",
            "开盘",
            "收盘",
            "最高",
            "最低",
            "成交量",
            "成交额",
            "振幅",
            "涨跌幅",
            "涨跌额",
            "换手率",
        ]
    ]
    return temp_df




# ... existing code ...
with open("raw_data/stock_code_list.json", "r", encoding="utf-8") as f:
    loaded_list = json.load(f)
raw_price_list = []
for code in loaded_list:
    for attempt in range(3):  # 最多重试3次
        try:
            df = get_stock_daily(code, "20240101", "20250101")
            
            if df is None or df.empty:
                raise ValueError("返回数据为空")
            
            df["股票代码"] = code
            raw_price_list.append(df)
            print(f"已获取 {code} 行情数据")
            break  # 成功则跳出重试循环
        except Exception as e:
            if attempt < 2:  # 不是最后一次重试
                wait_time = (attempt + 1) * 5  # 5秒、10秒递增
                print(f"获取 {code} 失败，{wait_time}秒后重试...（第{attempt + 1}次）")
                time.sleep(wait_time)
            else:
                print(f"获取 {code} 行情数据最终失败：{e}")
    
    time.sleep(random.uniform(2, 4))  # 手动请求延时可以短一些

# ... existing code ...

# 合并并保存原始行情数据
raw_price_df = pd.concat(raw_price_list, ignore_index=True) if raw_price_list else pd.DataFrame()
raw_price_df.to_csv("raw_data/raw_行情数据.csv", index=False, encoding="utf-8-sig")
print(f"原始行情数据量：{raw_price_df.shape}")

# ... rest of code ...

"""
# ===================== 3. 获取财务数据（核心修复：字段名统一） =====================
raw_finance_list = []
with open("raw_data/stock_code_list.json", "r", encoding="utf-8") as f:
    loaded_list = json.load(f)
for code in loaded_list:
    
    try:
        # 逐个获取成分股的财务核心指标（按报告期）
        df = ak.stock_financial_abstract_ths(symbol=code, indicator="按报告期")
        if not df.empty:
        # 补充股票代码列（核心）
            df["股票代码"] = code
            df["股票代码"] = df["股票代码"].astype(str).str.zfill(6)
            # 调整到第一列
            cols = ["股票代码"] + [col for col in df.columns if col != "股票代码"]
            df = df[cols]
            raw_finance_list.append(df)
        else:
            print(f"警告：{code} 无财务数据，跳过")
       # print(df.columns.tolist())
        # 核心修复：财务数据返回的是「股票代码」，统一重命名为「股票代码」
        if "股票代码" in df.columns:
            print("测试成功")
            df.rename(columns={"股票代码": "股票代码"}, inplace=True)
        raw_finance_list.append(df)
        time.sleep(0.3)
        print(f"已获取 {code} 财务数据")
    except Exception as e:
        print(f"获取 {code} 财务数据失败：{e}")


# 合并财务数据
raw_finance_df = pd.concat(raw_finance_list, ignore_index=True) if raw_finance_list else pd.DataFrame()

# 筛选有效数据（现在字段名统一为「股票代码」，不会再KeyError）
if not raw_finance_df.empty and "股票代码" in raw_finance_df.columns:
    raw_finance_df = raw_finance_df[raw_finance_df["股票代码"].isin(loaded_list)].copy()
else:
    print("财务数据为空或无「股票代码」字段，跳过筛选")

raw_finance_df.to_csv("raw_data/raw_财务数据.csv", index=False, encoding="utf-8-sig")
print(f"原始财务数据量：{raw_finance_df.shape}")

# ===================== 4. 清洗行情数据 =====================
price_df = pd.read_csv("raw_data/raw_行情数据.csv", encoding="utf-8-sig")

if not price_df.empty:
    # 4.1 基础格式规整
    price_df["日期"] = pd.to_datetime(price_df["日期"])
    # 行情数据字段名：品种代码
    price_df = price_df.set_index(["股票代码", "日期"]).sort_index()
    # 剔除重复记录
    price_df = price_df[~price_df.index.duplicated(keep="first")]

    # 4.2 无效数据剔除
    price_df = price_df.dropna(subset=["涨跌幅", "成交量"])
    price_df = price_df[price_df["成交量"] > 0]

    # 4.3 缺失值处理
    core_price_cols = ["开盘", "收盘", "最高", "最低", "涨跌幅", "成交量"]
    price_df[core_price_cols] = price_df[core_price_cols].ffill()
    price_df = price_df.dropna(subset=core_price_cols)

    # 4.4 极值缩尾处理
    def winsorize_series(series, min_q=0.01, max_q=0.99):
        #自定义缩尾函数，处理极端值
        lower = series.quantile(min_q)
        upper = series.quantile(max_q)
        return series.clip(lower=lower, upper=upper)

    # 对易出现极值的字段做缩尾处理
    for col in ["涨跌幅", "成交量", "换手率"]:
        if col in price_df.columns:
            price_df[col] = winsorize_series(price_df[col])

    # 保存清洗后的行情数据
    price_df.to_csv("clean_data/clean_行情数据.csv", encoding="utf-8-sig")
    print(f"清洗后行情数据量：{price_df.shape}，原始行情数据清洗完成")
else:
    print("无有效行情数据，跳过行情数据清洗")

# ===================== 5. 清洗财务数据 =====================
finance_df = pd.read_csv("raw_data/raw_财务数据.csv", encoding="utf-8-sig")

if not finance_df.empty:
    # 5.1 基础格式规整
    # 确保品种代码是6位字符串
    if "股票代码" in finance_df.columns:
        finance_df["股票代码"] = finance_df["股票代码"].astype(str).str.zfill(6)
    else:
        print("财务数据无「股票代码」字段，跳过财务数据清洗")
        finance_df = pd.DataFrame()

    if not finance_df.empty:
        finance_df["报告期"] = pd.to_datetime(finance_df["报告期"], errors="coerce")
        
        # 锁定核心财务字段（只保留存在的字段）
        core_finance_cols = ["股票代码", "报告期", "净资产收益率ROE", "归属母公司股东的净利润", 
                            "营业总收入同比增长率", "净利润同比增长率"]
        core_finance_cols = [col for col in core_finance_cols if col in finance_df.columns]
        finance_df = finance_df[core_finance_cols].copy()
        
        # 剔除重复记录
        finance_df = finance_df.drop_duplicates(subset=["股票代码", "报告期"], keep="first")

        # 5.2 缺失值处理
        na_cols = ["净资产收益率ROE", "归属母公司股东的净利润"]
        na_cols = [col for col in na_cols if col in finance_df.columns]
        if na_cols:
            finance_df = finance_df.dropna(subset=na_cols)

        # 5.3 异常值处理
        if "净资产收益率ROE" in finance_df.columns:
            finance_df = finance_df[(finance_df["净资产收益率ROE"]> -100) & (finance_df["净资产收益率ROE"] < 100)]
        if "营业总收入同比增长率" in finance_df.columns:
            #print(finance_df["营业总收入同比增长率"].describe())
            finance_df = finance_df[(finance_df["营业总收入同比增长率"]> -10) & (finance_df["营业总收入同比增长率"] < 10)]#
        # 保存清洗后的财务数据
        finance_df.to_csv("clean_data/clean_财务数据.csv", encoding="utf-8-sig")
        print(f"清洗后财务数据量：{finance_df.shape}，财务数据清洗完成")
else:
    print("无有效财务数据，跳过财务数据清洗")

# ===================== 6. 多源数据合并对齐 =====================
# 读取清洗后的数据
clean_price_df = pd.read_csv("clean_data/clean_行情数据.csv", encoding="utf-8-sig", parse_dates=["日期"]) if not price_df.empty else pd.DataFrame()
clean_finance_df = pd.read_csv("clean_data/clean_财务数据.csv", encoding="utf-8-sig", parse_dates=["报告期"]) if not finance_df.empty else pd.DataFrame()

final_df = pd.DataFrame()
if not clean_price_df.empty and not clean_finance_df.empty:
    # 6.1 财务数据的日频扩展（规避未来函数）
    finance_index_df = clean_finance_df.set_index(["股票代码", "报告期"]).sort_index()#财务数据
    full_index = clean_price_df.set_index(["股票代码", "日期"]).index#日间行情数据
    # finance_daily_df = finance_index_df.reindex(full_index, level="股票代码").groupby(level="股票代码").ffill()
    # 步骤1：重置财务数据索引，方便后续匹配
    finance_reset = finance_index_df.reset_index()
# 将报告期列名改为日期（与行情数据统一），便于索引对齐
    finance_reset.rename(columns={"报告期": "日期"}, inplace=True)

# 步骤2：将财务数据重新设置为（股票代码+日期）复合索引（与行情数据索引结构一致）
    finance_aligned = finance_reset.set_index(["股票代码", "日期"]).sort_index()

# 步骤3：方式1 - 仅保留两者共有的索引（交集）
# 财务数据对齐到行情数据的索引（只保留行情数据中存在的股票+日期组合）
    finance_daily_df = finance_aligned.reindex(full_index)
    # 6.2 多源数据合并对齐
    price_df_indexed = clean_price_df.set_index(["股票代码", "日期"])
    final_df = pd.merge(
        left=price_df_indexed,
        right=finance_daily_df,
        left_index=True,
        right_index=True,
        how="left"
    )

    # 剔除财务数据完全缺失的记录
    if "净资产收益率ROE" in final_df.columns:
        final_df = final_df.dropna(subset=["净资产收益率ROE"])
elif not clean_price_df.empty:
    # 若无财务数据，仅保留行情数据
    final_df = clean_price_df.set_index(["股票代码", "日期"])

print(f"对齐后最终数据集维度：{final_df.shape}")

# ===================== 7. 标准化输出 =====================
if not final_df.empty:
    # 字段重命名
    rename_mapping = {
        "开盘": "开盘价",
        "收盘": "收盘价",
        "最高": "最高价",
        "最低": "最低价",
        "净资产收益率ROE": "ROE",
        "归属母公司股东的净利润": "归母净利润",
        "营业总收入同比增长率": "营收同比增速",
        "净利润同比增长率": "净利润同比增速"
    }
    # 只重命名存在的字段
    rename_mapping = {k: v for k, v in rename_mapping.items() if k in final_df.columns}
    final_standard_df = final_df.rename(columns=rename_mapping)

    # 保留核心字段（只保留存在的字段）
    final_cols = ["开盘价", "收盘价", "最高价", "最低价", "涨跌幅", "成交量", "换手率", 
                "ROE", "归母净利润", "营收同比增速", "净利润同比增速"]
    final_cols = [col for col in final_cols if col in final_standard_df.columns]
    final_standard_df = final_standard_df[final_cols].copy()

    # 保存最终数据
    final_standard_df.to_csv("final_data/沪深300_投研标准化数据集.csv", encoding="utf-8-sig")
    try:
        final_standard_df.to_parquet("final_data/沪深300_投研标准化数据集.parquet")
    except Exception as e:
        print(f"保存parquet格式失败（需安装pyarrow）：{e}")

    # 输出数据说明文档
    with open("final_data/数据集说明文档.txt", "w", encoding="utf-8") as f:
        f.write("数据集说明：\n")
        f.write("1. 标的范围：沪深300成分股（随机选取10只）\n")
        f.write("2. 时间周期：2024-01-01 至 2025-01-01 日频数据\n")
        f.write("3. 数据内容：A股日线行情数据+上市公司季度核心财务指标\n")
        f.write("4. 处理流程：原始数据获取→无效值剔除→缺失值填充→极值缩尾→跨频数据对齐→标准化输出\n")
        f.write("5. 适用场景：量化因子测试、策略回测、投研数据分析\n")

print("所有数据处理完成，最终数据已保存至 final_data 目录！")
"""
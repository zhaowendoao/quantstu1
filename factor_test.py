import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings("ignore")

# 设置中文字体（解决可视化中文乱码）
plt.rcParams["font.family"] = ["SimHei", "WenQuanYi Micro Hei", "Heiti TC"]
plt.rcParams["axes.unicode_minus"] = False

# ===================== 1. 数据读取与基础准备 =====================
def load_data():
    """读取标准化后的最终数据集"""
    try:
        # 读取你生成的标准化数据集
        df = pd.read_csv(
            "final_data/沪深300_投研标准化数据集.csv",
            encoding="utf-8-sig",
            parse_dates=["日期"]  # 注意：索引重置后会有日期列
        )
        # 重置索引（如果是复合索引）
        if "股票代码" not in df.columns or "日期" not in df.columns:
            df = df.reset_index()
        
        # 基础格式检查
        df["股票代码"] = df["股票代码"].astype(str).str.zfill(6)
        df = df.sort_values(["股票代码", "日期"]).reset_index(drop=True)
        
        print(f"成功读取数据，数据维度：{df.shape}")
        print(f"覆盖标的数量：{df['股票代码'].nunique()}")
        print(f"时间范围：{df['日期'].min()} 至 {df['日期'].max()}")
        return df
    except FileNotFoundError:
        print("未找到标准化数据集，请先运行数据清洗代码！")
        return pd.DataFrame()

# ===================== 2. 因子构建（核心） =====================
def build_factors(df):
    """构建基础因子：量价类+财务类"""
    factor_df = df.copy()
    
    # ---------------- 2.1 量价类因子 ----------------
    # 2.1.1 均线乖离率（BIAS）：(收盘价 - N日均线) / N日均线 * 100
    # 选择60日作为基准（可调整为20/120日）
    factor_df["MA60"] = factor_df.groupby("股票代码")["收盘价"].rolling(window=60, min_periods=30).mean().reset_index(0, drop=True)
    factor_df["BIAS60"] = (factor_df["收盘价"] - factor_df["MA60"]) / factor_df["MA60"] * 100
    
    # 2.1.2 动量反转因子（MOM）：过去20日收益率（动量）/ 过去5日收益率（反转）
    # 动量因子：过去20日累计收益率（不含当日）
    factor_df["MOM20"] = factor_df.groupby("股票代码")["收盘价"].pct_change(20).reset_index(0, drop=True) * 100
    # 反转因子：过去5日累计收益率的相反数（反转）
    factor_df["REV5"] = -1 * factor_df.groupby("股票代码")["收盘价"].pct_change(5).reset_index(0, drop=True) * 100
    
    # ---------------- 2.2 财务类因子 ----------------
    # 2.2.1 ROE（直接使用清洗后的ROE，已处理异常值）
    factor_df.rename(columns={"ROE": "ROE"}, inplace=True)
    
    # 2.2.2 净利润增速（同比）
    if "净利润同比增速" in factor_df.columns:
        factor_df["PROFIT_GROWTH"] = factor_df["净利润同比增速"]
    else:
        # 若没有直接的增速，用归母净利润计算环比增速（备选方案）
        factor_df["PROFIT_GROWTH"] = factor_df.groupby("股票代码")["归母净利润"].pct_change().reset_index(0, drop=True) * 100
    
    # ---------------- 2.3 因子数据清理 ----------------
    # 只保留核心列
    core_cols = ["股票代码", "日期", "收盘价", "涨跌幅", "BIAS60", "MOM20", "REV5", "ROE", "PROFIT_GROWTH"]
    core_cols = [col for col in core_cols if col in factor_df.columns]
    factor_df = factor_df[core_cols].copy()
    
    # 剔除上市不足60天的标的（新股影响）
    factor_df["上市天数"] = factor_df.groupby("股票代码")["日期"].rank(method="first")
    factor_df = factor_df[factor_df["上市天数"] > 60].reset_index(drop=True)
    
    print("因子构建完成，包含因子：", [col for col in factor_df.columns if col in ["BIAS60", "MOM20", "REV5", "ROE", "PROFIT_GROWTH"]])
    return factor_df

# ===================== 3. 因子预处理（标准化） =====================
def preprocess_factors(factor_df):
    """因子预处理：缺失值填充、极值缩尾、标准化"""
    # 定义需要处理的因子列表
    factor_list = ["BIAS60", "MOM20", "REV5", "ROE", "PROFIT_GROWTH"]
    factor_list = [f for f in factor_list if f in factor_df.columns]
    
    processed_df = factor_df.copy()
    
    # 3.1 缺失值处理（按日期+股票填充）
    for factor in factor_list:
        
        # 先按股票向前填充（保留时序逻辑），再按日期截面填充（横向填充）
        processed_df[factor] = processed_df.groupby("股票代码")[factor].ffill()
        # 第一步：定义清洗百分数字符串的函数
        def clean_percent_str(x):
            if pd.isna(x):
                return np.nan
    # 如果是字符串且包含%，清洗后转数值
            if isinstance(x, str) and '%' in x:
                return float(x.replace('%', '')) / 100
    # 其他情况尝试转浮点数（兼容已有数值）
            try:
                return float(x)
            except:
                return np.nan

# 第二步：先清洗目标列的百分数字符串
        processed_df[factor] = processed_df[factor].apply(clean_percent_str)

# 第三步：执行分组填充中位数的操作（此时列已是数值类型）
        processed_df[factor] = processed_df.groupby("日期")[factor].transform(lambda x: x.fillna(x.median()))
    
    # 3.2 极值缩尾（1%/99%分位数）
    def winsorize(series, lower=0.01, upper=0.99):
        q_low = series.quantile(lower)
        q_high = series.quantile(upper)
        return series.clip(lower=q_low, upper=q_high)
    
    for factor in factor_list:
        processed_df[factor] = processed_df.groupby("日期")[factor].transform(winsorize)
    
    # 3.3 因子标准化（Z-score）：(因子值 - 截面均值) / 截面标准差
    for factor in factor_list:
        processed_df[factor] = processed_df.groupby("日期")[factor].transform(
            lambda x: (x - x.mean()) / (x.std() + 1e-8)  # 加小值避免除零
        )
    
    # 剔除仍有缺失值的记录
    processed_df = processed_df.dropna(subset=factor_list + ["涨跌幅"])
    
    print(f"因子预处理完成，剩余数据量：{processed_df.shape}")
    return processed_df, factor_list

# ===================== 4. 因子有效性分析 =====================
def factor_analysis(processed_df, factor_list):
    """因子有效性分析：相关性分析 + IC值计算"""
    analysis_results = {}
    
    # 4.1 因子相关性分析
    factor_corr = processed_df[factor_list].corr()
    print("\n===== 因子相关性矩阵 =====")
    print(factor_corr.round(3))
    
    # 4.2 IC值计算（信息系数：因子与下期收益的秩相关系数）
    # IC值是衡量因子预测能力的核心指标，绝对值>0.02即有一定预测性
    ic_results = {}
    for factor in factor_list:
        # 计算下期收益（未来1日涨跌幅）
        processed_df["next_return"] = processed_df.groupby("股票代码")["涨跌幅"].shift(-1)
        
        # 按日期计算秩相关系数（Spearman）
        daily_ic = processed_df.groupby("日期").apply(
            lambda x: x[[factor, "next_return"]].corr(method="spearman").loc[factor, "next_return"]
        ).dropna()
        
        # IC统计
        ic_mean = daily_ic.mean()
        ic_std = daily_ic.std()
        ic_ir = ic_mean / (ic_std + 1e-8)  # 信息比率
        ic_pos_ratio = (daily_ic > 0).sum() / len(daily_ic)  # 正IC占比
        
        ic_results[factor] = {
            "IC均值": round(ic_mean,4),
            "IC标准差": round(ic_std,4),
            "信息比率IR": round(ic_ir,4),
            "正IC占比": f"{ic_pos_ratio:.2%}"
        }
    
    # 输出IC分析结果
    print("\n===== 因子IC分析结果 =====")
    ic_df = pd.DataFrame(ic_results).T
    print(ic_df)
    
    # 保存分析结果
    analysis_results["correlation"] = factor_corr
    analysis_results["ic"] = ic_df
    return analysis_results

# ===================== 5. 分层回测（核心验证） =====================
def stratified_backtest(processed_df, factor_list):
    """分层回测：5分层，计算各层收益、胜率"""
    backtest_results = {}
    
    for factor in factor_list:
        print(f"\n===== 开始{factor}因子分层回测 =====")
        # 5.1 按日期+因子值分层（1层=因子值最小，5层=因子值最大）
        processed_df["factor_rank"] = processed_df.groupby("日期")[factor].rank(method="min", pct=True)
        processed_df["layer"] = pd.cut(
            processed_df["factor_rank"],
            bins=[0, 0.2, 0.4, 0.6, 0.8, 1.0],
            labels=["Layer1", "Layer2", "Layer3", "Layer4", "Layer5"]
        )
        
        # 5.2 计算各层下期收益（未来1日涨跌幅）
        layer_returns = processed_df.groupby(["日期", "layer"])["next_return"].mean().reset_index()
        
        # 5.3 计算累计收益
        layer_cum_return = layer_returns.pivot_table(
            index="日期",
            columns="layer",
            values="next_return"
        ).fillna(0).cumsum()
        
        # 5.4 回测指标计算
        layer_stats = {}
        for layer in ["Layer1", "Layer2", "Layer3", "Layer4", "Layer5"]:
            if layer in layer_cum_return.columns:
                # 总收益
                total_return = layer_cum_return[layer].iloc[-1]
                # 日均收益
                daily_return = layer_returns[layer_returns["layer"] == layer]["next_return"].mean()
                # 胜率（正收益天数占比）
                win_rate = (layer_returns[layer_returns["layer"] == layer]["next_return"] > 0).sum() / len(layer_returns[layer_returns["layer"] == layer])
                # 最大回撤（简化计算）
                roll_max = layer_cum_return[layer].cummax()
                drawdown = (layer_cum_return[layer] - roll_max) / roll_max
                max_dd = drawdown.min()
                
                layer_stats[layer] = {
                    "累计收益(%)": total_return.round(2),
                    "日均收益(%)": daily_return.round(4),
                    "胜率": f"{win_rate:.2%}",
                    "最大回撤(%)": round(max_dd * 100,2)
                }
        
        # 5.5 多空收益（Layer5 - Layer1）
        if "Layer5" in layer_cum_return.columns and "Layer1" in layer_cum_return.columns:
            layer_cum_return["LongShort"] = layer_cum_return["Layer5"] - layer_cum_return["Layer1"]
            long_short_total = layer_cum_return["LongShort"].iloc[-1]
            layer_stats["多空组合"] = {"累计收益(%)": long_short_total.round(2)}
        
        # 保存结果
        backtest_results[factor] = {
            "layer_stats": pd.DataFrame(layer_stats).T,
            "cum_return": layer_cum_return
        }
        
        # 打印分层统计
        print(f"{factor}因子分层回测结果：")
        print(pd.DataFrame(layer_stats).T.round(2))
        
        # 绘制累计收益曲线
        plt.figure(figsize=(12, 6))
        layer_cum_return[["Layer1", "Layer2", "Layer3", "Layer4", "Layer5"]].plot(ax=plt.gca())
        if "LongShort" in layer_cum_return.columns:
            plt.plot(layer_cum_return.index, layer_cum_return["LongShort"], label="多空组合", linestyle="--", linewidth=2)
        plt.title(f"{factor}因子分层累计收益")
        plt.xlabel("日期")
        plt.ylabel("累计收益(%)")
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.savefig(f"final_data/{factor}_分层收益曲线.png", dpi=300, bbox_inches="tight")
        plt.close()
    
    return backtest_results

# ===================== 6. 输出标准化分析报告 =====================
def generate_report(analysis_results, backtest_results, factor_list):
    """生成标准化因子分析报告"""
    report_content = []
    report_content.append("# 单因子有效性测试分析报告")
    report_content.append("## 1. 测试概况")
    report_content.append("- 测试标的：沪深300成分股（样本量：10只）")
    report_content.append("- 测试时间：2024-01-01 至 2025-01-01")
    report_content.append("- 测试因子：量价类（BIAS60、MOM20、REV5）、财务类（ROE、PROFIT_GROWTH）")
    report_content.append("- 测试方法：IC分析、5分层回测")
    
    # 添加因子相关性
    report_content.append("\n## 2. 因子相关性分析")
    report_content.append("### 因子相关性矩阵")
    report_content.append(analysis_results["correlation"].round(3).to_string())
    
    # 添加IC分析结果
    report_content.append("\n## 3. 因子IC分析（核心预测能力）")
    report_content.append("### IC值统计（越大越好，绝对值>0.02为有效）")
    report_content.append(analysis_results["ic"].to_string())
    
    # 添加回测结果
    report_content.append("\n## 4. 分层回测结果")
    for factor in factor_list:
        if factor in backtest_results:
            report_content.append(f"\n### {factor}因子分层统计")
            report_content.append(backtest_results[factor]["layer_stats"].to_string())
    
    # 添加结论
    report_content.append("\n## 5. 测试结论")
    ic_df = analysis_results["ic"]
    effective_factors = ic_df[abs(ic_df["IC均值"]) > 0.02].index.tolist()
    if effective_factors:
        report_content.append(f"- 有效因子：{', '.join(effective_factors)}（IC绝对值>0.02）")
    else:
        report_content.append("- 暂无显著有效因子（IC绝对值均<0.02），建议调整因子参数或扩大样本")
    
    # 保存报告
    with open("final_data/单因子测试分析报告.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(report_content))
    
    print("\n===== 分析报告已生成 =====")
    print("报告路径：final_data/单因子测试分析报告.txt")
    print("收益曲线路径：final_data/[因子名]_分层收益曲线.png")

# ===================== 主函数（一键运行） =====================
def main():
    # 步骤1：读取数据
    df = load_data()
    if df.empty:
        return
    
    # 步骤2：构建因子
    factor_df = build_factors(df)
    if factor_df.empty:
        return
    
    # 步骤3：因子预处理
    processed_df, factor_list = preprocess_factors(factor_df)
    if processed_df.empty:
        print ("因子预处理失败，请检查数据质量！")
        return
    
    # 步骤4：因子有效性分析
    analysis_results = factor_analysis(processed_df, factor_list)
    
    # 步骤5：分层回测
    backtest_results = stratified_backtest(processed_df, factor_list)
    
    # 步骤6：生成报告
    generate_report(analysis_results, backtest_results, factor_list)
    
    print("\n===== 单因子测试MVP完成 =====")
    print("所有结果已保存至 final_data 目录！")

if __name__ == "__main__":
    main()
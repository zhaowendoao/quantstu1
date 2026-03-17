import pandas as pd
import re
import os

# 读取已有因子数据和回测结果
def export_factor_data_to_excel():
    # ===================== 核心改造：正则提取报告中的关键指标 =====================
    def extract_core_indicators_from_report():
        """从分析报告中正则提取核心监控指标"""
        # 1. 读取报告文件
        report_path = "final_data/单因子测试分析报告.txt"
        if not os.path.exists(report_path):
            print("未找到分析报告，返回默认数据")
            return None
        
        with open(report_path, "r", encoding="utf-8") as f:
            report_content = f.read()
        
        # 2. 定义正则表达式（匹配报告中的关键数据）
        # 2.1 提取IC分析结果（IC均值、信息比率IR）
        ic_pattern = re.compile(
            r'(\w+)\s+([-+]?\d+\.\d+)\s+([-+]?\d+\.\d+)\s+([-+]?\d+\.\d+)\s+(\d+\.\d+%)'
        )
        ic_matches = ic_pattern.findall(report_content)
        
        # 2.2 提取各因子多空累计收益
        long_short_pattern = re.compile(
            r'(\w+)因子分层统计[\s\S]*?多空组合\s+([-+]?\d+\.\d+)\s+NaN\s+NaN\s+NaN'
        )
        long_short_matches = long_short_pattern.findall(report_content)
        
        # 2.3 提取各因子胜率（取Layer5的胜率作为代表，也可调整为均值）
        win_rate_pattern = re.compile(
            r'(\w+)因子分层统计[\s\S]*?Layer5\s+[\d\.\-]+\s+[\d\.\-]+\s+(\d+\.\d+%)\s+[\d\.\-]+'
        )
        win_rate_matches = win_rate_pattern.findall(report_content)
        
        # 3. 整理数据为字典
        factor_indicators = {}
        # 先初始化所有因子的IC数据
        for match in ic_matches:
            factor_name, ic_mean, ic_std, ir, positive_ic_ratio = match
            factor_indicators[factor_name] = {
                "IC均值": float(ic_mean),
                "信息比率IR": float(ir),
                "多空累计收益(%)": None,
                "胜率": None
            }
        
        # 补充多空收益
        for factor_name, long_short_return in long_short_matches:
            if factor_name in factor_indicators:
                factor_indicators[factor_name]["多空累计收益(%)"] = float(long_short_return)
        
        # 补充胜率
        for factor_name, win_rate in win_rate_matches:
            if factor_name in factor_indicators:
                factor_indicators[factor_name]["胜率"] = win_rate
        
        # 4. 转换为DataFrame
        factor_list = list(factor_indicators.keys())
        monitor_data = {
            "因子名称": factor_list,
            "IC均值": [factor_indicators[f]["IC均值"] for f in factor_list],
            "信息比率IR": [factor_indicators[f]["信息比率IR"] for f in factor_list],
            "多空累计收益(%)": [factor_indicators[f]["多空累计收益(%)"] for f in factor_list],
            "胜率": [factor_indicators[f]["胜率"] for f in factor_list]
        }
        
        return pd.DataFrame(monitor_data)
    
    # ===================== 原有逻辑：读取因子数据 + 写入Excel =====================
    # 1. 读取因子数据
    try:
        factor_df = pd.read_csv("final_data/沪深300_投研标准化数据集.csv", encoding="utf-8-sig", parse_dates=["日期"])
    except FileNotFoundError:
        print("未找到因子数据集，跳过原始数据写入")
        factor_df = pd.DataFrame()
    
    # 2. 正则提取核心监控指标
    monitor_df = extract_core_indicators_from_report()
    if monitor_df is None:
        # 备用：当报告读取失败时使用默认值（兼容原有逻辑）
        monitor_data = {
            "因子名称": ["BIAS60", "MOM20", "REV5", "PROFIT_GROWTH"],
            "IC均值": [-0.0652, -0.0251, 0.061, 0.0198],
            "信息比率IR": [-0.1677, -0.0697, 0.1595, 0.0553],
            "多空累计收益(%)": [6.62, 12.37, -3.89, 12.71],
            "胜率": ["46.77%", "54.84%", "46.77%", "43.55%"]
        }
        monitor_df = pd.DataFrame(monitor_data)
    
    # 3. 写入Excel模板（适配Sheet结构）
    output_path = "final_data/量化因子跟踪模板.xlsx"
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        # 写入因子原始数据（仅保留核心列）
        if not factor_df.empty:
            core_data_cols = ["股票代码", "日期", "BIAS60", "MOM20", "REV5", "ROE", "PROFIT_GROWTH", "next_return"]
            core_data_cols = [col for col in core_data_cols if col in factor_df.columns]
            factor_df[core_data_cols].to_excel(
                writer, sheet_name="因子原始数据", index=False
            )
        # 写入核心指标监控
        monitor_df.to_excel(writer, sheet_name="核心指标监控", index=False)
    
    print(f"Excel数据导出完成，路径：{output_path}")
    print("提取的核心指标：")
    print(monitor_df.round(4))
    return output_path

# 执行导出（可直接加到原有代码末尾）
if __name__ == "__main__":
    excel_path = export_factor_data_to_excel()
    #os.startfile("final_data/量化因子跟踪模板.xlsx")
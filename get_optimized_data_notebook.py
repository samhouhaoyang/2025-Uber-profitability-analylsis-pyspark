# 获取优化后数据的Notebook代码
# 复制以下代码到notebook中运行

# 方案1: 小样本测试

# ============================================================================
# 获取最新的特征优化版本数据
# ============================================================================

print("🚀 GENERATING OPTIMIZED DATASET WITH FEATURE OPTIMIZATION")
print("="*70)

# 1. 导入优化后的pipeline函数
from preprocessing.final_corrected_profitability_analysis.data_pipeline import (
    load_and_integrate_all_features_final,
    save_comprehensive_profitability_dataset_final,
    process_full_profitability_dataset
)

# 方案1: 生成小样本优化数据 (快速测试)
print("\n📊 方案1: 生成小样本优化数据")
print("-" * 40)

year, month = 2023, 1
sample_fraction = 0.01  # 1%样本

print(f"🔄 Loading optimized data for {year}-{month:02d} (sample: {sample_fraction*100}%)...")

# 这会自动包含特征优化
optimized_sample_data = load_and_integrate_all_features_final(year, month, sample_fraction)

print(f"✅ 优化样本数据生成完成!")
print(f"  📊 Records: {optimized_sample_data.count():,}")
print(f"  📋 Features: {len(optimized_sample_data.columns)}")

# 保存优化后的样本数据
sample_save_path = save_comprehensive_profitability_dataset_final(
    optimized_sample_data, 
    year=year, 
    month=month, 
    dataset_name="optimized_profitability_sample"
)

print(f"💾 优化样本数据已保存: {sample_save_path}")

# 显示特征类别
print("\n📋 优化后的特征预览:")
cols = optimized_sample_data.columns

# 核心业务特征
business_cols = [col for col in cols if any(keyword in col.lower() for keyword in 
                ['uber_commission', 'uber_take_rate', 'uber_revenue', 'driver_pay', 'base_passenger_fare'])]
print(f"  💰 Revenue特征 ({len(business_cols)}): {business_cols}")

# 天气特征
weather_cols = [col for col in cols if any(keyword in col.lower() for keyword in 
               ['temp', 'weather', 'precipitation', 'wind', 'snow', 'fog', 'season'])]
print(f"  🌤️ Weather特征 ({len(weather_cols)}): {weather_cols}")

# 空间特征
spatial_cols = [col for col in cols if any(keyword in col.lower() for keyword in 
               ['location', 'cbd', 'borough', 'PU_', 'DO_'])]
print(f"  📍 Spatial特征 ({len(spatial_cols)}): {spatial_cols}")

# 时间特征
temporal_cols = [col for col in cols if any(keyword in col.lower() for keyword in 
                ['hour', 'rush', 'weekend', 'day', 'time'])]
print(f"  ⏰ Temporal特征 ({len(temporal_cols)}): {temporal_cols}")

print("\n" + "="*70)
print("🎉 小样本优化数据生成完成!")
print("💡 如需完整数据，请运行下面的方案2")


# 方案2: 完整数据

# ============================================================================
# 方案2: 生成完整优化数据 (生产环境)
# ============================================================================

print("\n📊 方案2: 生成完整优化数据")
print("-" * 40)
print("⚠️ 注意: 这会处理完整数据集，需要较长时间")

# 处理完整数据集
result = process_full_profitability_dataset(
    year=2023, 
    month=1, 
    save_name="optimized_profitability_full"
)

if result:
    print(f"✅ 完整优化数据生成完成!")
    print(f"📊 处理结果: {result}")
else:
    print("❌ 完整数据生成失败")


# 方案3: 批量数据

# ============================================================================
# 方案3: 批量生成多个月优化数据
# ============================================================================

print("\n📊 方案3: 批量生成优化数据")
print("-" * 40)

from preprocessing.final_corrected_profitability_analysis.data_pipeline import (
    process_all_months_profitability_batch
)

# 批量处理多个月份
years = [2023]
months = [1, 2, 3]  # 根据需要调整月份

print(f"🔄 批量处理 {len(years)} 年 x {len(months)} 月 的数据...")

process_all_months_profitability_batch(years=years, months=months)

print("✅ 批量优化数据生成完成!")

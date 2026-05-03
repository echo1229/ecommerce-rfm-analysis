import numpy as np
import pandas as pd
from scipy import stats

# ---------------------------------------------------------
# 第一部分：实验数据模拟生成
# ---------------------------------------------------------
np.random.seed(42)

# 1. 基础设定
TOTAL_USERS = 1390000
group_labels = np.random.choice(['A', 'B'], size=TOTAL_USERS, p=[0.5, 0.5])

# 初始化 DataFrame
df = pd.DataFrame({
    'user_id': range(1, TOTAL_USERS + 1),
    'group': group_labels
})

# 2. 模拟转化率 (CVR)
# 假设对照组(A)转化率为 11%，实验组(B)转化率略高为 13.5%
cvr_A = 0.11
cvr_B = 0.135

df['is_converted'] = 0
mask_A = df['group'] == 'A'
mask_B = df['group'] == 'B'

df.loc[mask_A, 'is_converted'] = np.random.binomial(1, cvr_A, size=mask_A.sum())
df.loc[mask_B, 'is_converted'] = np.random.binomial(1, cvr_B, size=mask_B.sum())

# 3. 模拟订单金额 (Order Amount)
# A组发放5元无门槛，客单价呈正态分布(均值60，标准差20)
# B组发放满100减20，存在明显的“凑单效应”，客单价分布右移(均值125，标准差25)
df['order_amount'] = 0.0

converted_A = mask_A & (df['is_converted'] == 1)
converted_B = mask_B & (df['is_converted'] == 1)

# 为防止出现负数订单，使用 np.clip 限制最低金额
df.loc[converted_A, 'order_amount'] = np.clip(np.random.normal(60, 20, size=converted_A.sum()), 5, None)
df.loc[converted_B, 'order_amount'] = np.clip(np.random.normal(125, 25, size=converted_B.sum()), 100, None)

# 4. 计算用券成本 (Coupon Cost)
df['coupon_cost'] = 0
df.loc[converted_A, 'coupon_cost'] = 5
df.loc[converted_B, 'coupon_cost'] = 20

# ---------------------------------------------------------
# 第二部分：实验结果分析与汇总
# ---------------------------------------------------------

# 核心业务指标汇总表
metrics = df.groupby('group').agg(
    users=('user_id', 'count'),
    conversions=('is_converted', 'sum'),
    total_revenue=('order_amount', 'sum'),
    total_cost=('coupon_cost', 'sum')
).reset_index()

metrics['CVR'] = metrics['conversions'] / metrics['users']
metrics['AOV'] = metrics['total_revenue'] / metrics['conversions']
metrics['ARPU'] = metrics['total_revenue'] / metrics['users']
# 简化的 ROI 计算：(总营收 - 总发券成本) / 总发券成本
metrics['ROI'] = (metrics['total_revenue'] - metrics['total_cost']) / metrics['total_cost']

print("="*60)
print("📊 实验组与对照组核心业务指标汇总")
print("="*60)
print(metrics.to_string(index=False))
print("\n")

# ---------------------------------------------------------
# 第三部分：显著性检验 (A/B Test)
# ---------------------------------------------------------

# 1. 转化率 (CVR) - 卡方检验
contingency_table = pd.crosstab(df['group'], df['is_converted'])
chi2, p_val_cvr, dof, expected = stats.chi2_contingency(contingency_table)

print("="*60)
print("🧪 显著性检验结果")
print("="*60)
print(f"1. 转化率 (CVR) 卡方检验 p-value: {p_val_cvr:.4e}")
if p_val_cvr < 0.05:
    print("   ✅ 结论：两组转化率差异在统计学上显著。")
else:
    print("   ❌ 结论：两组转化率差异不显著。")

# 2. 客单价 (AOV) - 两独立样本 T 检验
# 仅提取成功转化的用户进行客单价对比
aov_array_A = df.loc[converted_A, 'order_amount'].values
aov_array_B = df.loc[converted_B, 'order_amount'].values

t_stat, p_val_aov = stats.ttest_ind(aov_array_A, aov_array_B, equal_var=False)

print(f"2. 客单价 (AOV) T-test p-value: {p_val_aov:.4e}")
if p_val_aov < 0.05:
    print("   ✅ 结论：两组客单价差异在统计学上显著。")
else:
    print("   ❌ 结论：两组客单价差异不显著。")
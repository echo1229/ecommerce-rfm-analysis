import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

con = duckdb.connect(r"D:\实战项目\Dataanalysis\Duckdb\project.duckdb")
#project.duckdb更换成你的数据库路径
df = con.execute("SELECT * FROM user_features").df()



#确定参考日期(Recency里最大的一天)
reference_date = df['Recency'].max()
#计算差值
diff = reference_date - df['Recency']
#将差值转换为天数
df['R_days'] = diff.dt.days
#处理空值(这里空值填大的数方便后续分析，因为rfm需要按R_days从小到大排序，天数越小越好)
df['R_days']=df['R_days'].fillna(99999)



#avg_session（时长）转为“总秒数”
df['session_seconds'] = df['avg_session'].dt.total_seconds()
#处理空值(这里空值填0，表示没有会话时长)
df['session_seconds'] = df['session_seconds'].fillna(0)



####用户标签
#筛选出“成交客”（有购买记录的人，即频次大于0）
df_active = df[df['Frequency'] > 0].copy()

#筛选出“潜客”（只逛不买的人，即频次等于0）
df_potential = df[df['Frequency'] == 0].copy()

distribution_stats = df_active[['R_days','Frequency','monetary']].describe(
    percentiles=[0.25, 0.5, 0.75,0.90,0.95,0.99]
)

#R_score:按照传统模型5分制
df_active['R_score'] = pd.qcut(df_active['R_days'],q=5,labels=[5,4,3,2,1])

#F_score：按照传统模型5分制
F_bins = [0,1,2,3,4,np.inf]
df_active["F_score"] = pd.cut(df_active['Frequency'], bins=F_bins, labels=[1,2,3,4,5])

#M_score：增加6分档，因为有些客户的消费金额非常大，远超其他客户，导致分档不均衡
#np.inf的作用，因为我们现在分析的数据是抽样出来，防止有个别客户的消费金额过大，导致分档不均衡，所以设置一个很大的数作为最后一个分档的上限，确保所有客户都能被合理分档。
M_bins  = [0,100,200,400,900,2000,np.inf]
df_active['M_score'] = pd.cut(df_active['monetary'] , bins=M_bins, labels=[1,2,3,4,5,6])

#R_score，为成交用户增加行为维度
df_active['I_score'] = pd.qcut(df_active['Intent'].rank(method = 'first'),q=5,labels=[1, 2, 3, 4, 5]
)



# 定义分层规则函数
def get_active_segment(row):
    r,f,m,i = row['R_score'],row['F_score'],row['M_score'],row['I_score']
    if m >= 5 and r>= 4:
        return'核心高价值客户'
    elif m >= 5 and r<= 2:
        return'重要挽留客户'
    elif f == 1 and m>=4 and i>=4:
        return'高潜单次大客'
    elif m < 4 and i>= 4:
        return'高互动普通客'
    elif f == 1 and m <= 2 and i<=2:
        return'低价值流失客'
    else:
        return'一般客户'

df_active['用户标签'] = df_active.apply(get_active_segment,axis=1)
# print(df_active['用户标签'].value_counts())

#核心聚合计算
profile_active = df_active.groupby('用户标签').agg(
    用户数=('user_id','nunique'),           # 统计绝对去重人数
    总营收GMV=('monetary','sum'),           # 该群体的总贡献金额
    客单价ARPU=('monetary','mean'),         # 该群体平均每人花多少钱
    平均频次F=('Frequency','mean'),         # 平均购买次数
    平均间隔天数R=('R_days','mean')         # 平均多久没来
).reset_index()
#计算大盘总基数
total_revenue = profile_active['总营收GMV'].sum()
total_users = profile_active['用户数'].sum()

#计算人数占比 vs 营收占比
profile_active['人数占比'] = (profile_active['用户数'] / total_users).round(2)
profile_active['营收占比'] = (profile_active['总营收GMV'] / total_revenue).round(2)


# 格式化输出
display_df = profile_active.copy()
display_df['人数占比'] = display_df['人数占比'].apply(lambda x: f"{x:.2%}")
display_df['营收占比'] = display_df['营收占比'].apply(lambda x: f"{x:.2%}")
display_df['总营收GMV'] = display_df['总营收GMV'].round(2)
display_df['客单价ARPU'] = display_df['客单价ARPU'].round(2)
display_df['平均间隔天数R'] = display_df['平均间隔天数R'].round(1)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)






####潜客分析


# 查看潜客 I(互动次数) 和 S(停留秒数) 的真实分布

print(df_potential[['Intent', 'session_seconds']].describe(
    percentiles=[0.5, 0.75, 0.80, 0.90, 0.95, 0.99]
).round(2))

i_bins = [0, 1, 2, 3, 5, np.inf]
df_potential['I_score'] = pd.cut(df_potential['Intent'], bins=i_bins, labels=[1, 2, 3, 4, 5]).astype(float)
s_bins = [-1, 0, 60, 180, 600, np.inf]
df_potential['S_score'] = pd.cut(df_potential['session_seconds'], bins=s_bins, labels=[1, 2, 3, 4, 5]).astype(float)

# 定义潜客分层逻辑函数
def get_potential_segment(row):
    i, s = row['I_score'], row['S_score']
    
    # 逻辑 A：高互动 + 高停留
    if i >= 4 and s >= 4:
        return '核心高意向潜客'
    
    # 逻辑 B：低互动 + 高停留
    elif s >= 4 and i < 4:
        return '高时长静默潜客'
    
    # 逻辑 C：高互动 + 低停留
    elif i >= 4 and s <= 2:
        return '浅层高频交互客'
    
    # 逻辑 D：低互动 + 低停留
    elif i <= 2 and s <= 2:
        return '尾部低价值流量'
    
    else:
        return '常规培育客群'
df_potential['用户标签'] = df_potential.apply(get_potential_segment, axis=1)
print(df_potential['用户标签'].value_counts())


# 聚合计算：按标签统计人数和行为指标均值
profile_potential = df_potential.groupby('用户标签').agg(
    潜客人数=('user_id', 'nunique'),
    平均互动次数=('Intent', 'mean'),
    平均停留秒数=('session_seconds', 'mean')
).reset_index()

# 计算人群占比
total_potential = profile_potential['潜客人数'].sum()
profile_potential['人群占比'] = (profile_potential['潜客人数'] / total_potential)

# 排序并格式化展示
profile_potential = profile_potential.sort_values('平均互动次数', ascending=False)
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)
pd.set_option('display.width', 1000)

display_pot = profile_potential.copy()
display_pot['人群占比'] = display_pot['人群占比'].apply(lambda x: f"{x:.2%}")
display_pot['平均互动次数'] = display_pot['平均互动次数'].round(2)
display_pot['平均停留秒数'] = display_pot['平均停留秒数'].round(2)

print(display_pot)






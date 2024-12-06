import pandas as pd

# 读取 CSV 文件
df = pd.read_csv('C:/Users/liarliar/Downloads/youtube-download/avqa_download_urls_1.csv')  # 请替换为您的文件路径
print(df.columns)
# 筛选出 'train/test split' 列为 'test' 的行
filtered_df = df[df[' train/test split'] == 'Test']

# 将筛选后的数据保存到新的 CSV 文件
filtered_df.to_csv('eval_file.csv', index=False)

print("操作完成，已保存筛选后的数据到 'filtered_file.csv'")

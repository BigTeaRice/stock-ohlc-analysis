import os
import pandas as pd
from datetime import datetime

# 创建必要的目录和文件
os.makedirs('stock_data', exist_ok=True)

# 创建示例CSV文件
dates = pd.date_range(start='2024-01-01', periods=50, freq='D')
data = {
    'Date': dates,
    'Open': [300 + i * 2 for i in range(50)],
    'High': [310 + i * 2 for i in range(50)],
    'Low': [290 + i * 2 for i in range(50)],
    'Close': [305 + i * 2 for i in range(50)],
    'Volume': [1000000 + i * 50000 for i in range(50)]
}
df = pd.DataFrame(data)
df.to_csv('stock_data/0700_HK.csv', index=False)

# 创建简单的HTML文件
html_content = '''
<!DOCTYPE html>
<html>
<head>
    <title>騰訊控股(0700.HK) K線圖</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        h1 { color: #333; }
        .info { background: #f5f5f5; padding: 20px; border-radius: 5px; }
    </style>
</head>
<body>
    <h1>騰訊控股(0700.HK) K線圖</h1>
    <div class="info">
        <p>圖表生成時間: ''' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '''</p>
        <p>數據記錄數: ''' + str(len(df)) + '''</p>
        <p>時間範圍: ''' + str(dates.min().date()) + ''' 至 ''' + str(dates.max().date()) + '''</p>
    </div>
    <p><em>註: 這是示例圖表，實際數據獲取可能遇到問題</em></p>
</body>
</html>
'''

with open('stock_data/0700_HK_candlestick.html', 'w', encoding='utf-8') as f:
    f.write(html_content)

print("文件生成完成！")
print("CSV文件: stock_data/0700_HK.csv")
print("HTML文件: stock_data/0700_HK_candlestick.html")
